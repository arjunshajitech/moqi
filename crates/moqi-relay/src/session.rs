//! One MoQT session (= one WebTransport connection) at the relay.
//!
//! Stream model (§6.3, §6.4):
//! - one unidirectional control stream per direction, starting with SETUP;
//! - one bidirectional stream per request (SUBSCRIBE, PUBLISH_NAMESPACE, ...),
//!   whose lifetime *is* the request's lifetime;
//! - unidirectional data streams (SUBGROUP_HEADER) and OBJECT_DATAGRAMs.
//!
//! Every request runs in its own task; all of them are aborted when the
//! session ends, which in turn withdraws namespaces and drops subscriptions.

use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use bytes::Bytes;
use moqi_proto::coding;
use moqi_proto::data::{
    stream_type, ObjectDatagram, SubgroupHeader, SubgroupObjectCodec,
};
use moqi_proto::message::*;
use moqi_proto::types::*;
use moqi_proto::{Decode, Encode};
use tokio::sync::{oneshot, watch, Notify};
use tokio::task::JoinSet;
use tokio::time::timeout;
use tracing::Instrument;
use wtransport::{Connection, SendStream, VarInt};

use crate::error::SessionError;
use crate::io::{write_message, Reader};
use crate::relay::{NamespaceEvent, Relay, TrackHandle};
use crate::track::{Subgroup, SubgroupEnd, Track, TrackEvent};

const IMPLEMENTATION: &str = concat!("moqi-relay/", env!("CARGO_PKG_VERSION"));
const SETUP_TIMEOUT: Duration = Duration::from_secs(10);
/// How long a data stream / datagram may wait for its Track Alias (§3.1.3.1).
const UNKNOWN_ALIAS_WAIT: Duration = Duration::from_secs(2);
/// Grace period for in-flight subgroup streams after the track ends.
const DRAIN_TIMEOUT: Duration = Duration::from_secs(2);

pub struct Session {
    pub id: u64,
    conn: Connection,
    relay: Arc<Relay>,
    /// Server-initiated Request IDs are odd (§6.4.2.1).
    next_request_id: AtomicU64,
    /// Track Aliases this session assigns in SUBSCRIBE_OK sent downstream.
    next_alias: AtomicU64,
    /// Track Aliases the peer assigned to tracks we subscribed to upstream.
    upstream: Mutex<HashMap<u64, Arc<Track>>>,
    upstream_changed: Notify,
    peer_request_ids: Mutex<HashSet<u64>>,
    setup_done: watch::Sender<bool>,
}

impl Session {
    pub async fn run(relay: Arc<Relay>, conn: Connection, id: u64) {
        let session = Arc::new(Self {
            id,
            conn: conn.clone(),
            relay,
            next_request_id: AtomicU64::new(1),
            next_alias: AtomicU64::new(0),
            upstream: Mutex::default(),
            upstream_changed: Notify::new(),
            peer_request_ids: Mutex::default(),
            setup_done: watch::Sender::new(false),
        });
        match session.clone().serve().await {
            Ok(()) => {}
            Err(SessionError::Connection(e)) => tracing::info!("session closed: {e}"),
            Err(e) => {
                tracing::warn!("session error: {e}");
                if let Some(code) = e.close_code() {
                    conn.close(VarInt::from_u32(code), e.to_string().as_bytes());
                }
            }
        }
    }

    async fn serve(self: Arc<Self>) -> Result<(), SessionError> {
        // Our control stream. It must stay open for the whole session, so it
        // is held by this frame until `serve` returns.
        let mut control = self.conn.open_uni().await?.await?;
        control.set_priority(i32::MAX);
        let mut options = Properties::default();
        options
            .0
            .push((setup_option::MOQT_IMPLEMENTATION, KvValue::Bytes(Bytes::from_static(IMPLEMENTATION.as_bytes()))));
        write_message(&mut control, &Message::Setup(Setup { options })).await?;

        let mut tasks: JoinSet<Result<(), SessionError>> = JoinSet::new();
        loop {
            tokio::select! {
                uni = self.conn.accept_uni() => {
                    let this = self.clone();
                    let recv = uni?;
                    tasks.spawn(async move { this.handle_uni(Reader::new(recv)).await }.in_current_span());
                }
                bi = self.conn.accept_bi() => {
                    let this = self.clone();
                    let (send, recv) = bi?;
                    tasks.spawn(async move { this.handle_request(send, Reader::new(recv)).await }.in_current_span());
                }
                dg = self.conn.receive_datagram() => {
                    self.handle_datagram(dg?.payload())?;
                }
                Some(done) = tasks.join_next() => match done {
                    Ok(Err(e)) if e.close_code().is_some() => return Err(e),
                    Ok(Err(e)) => tracing::debug!("stream ended: {e}"),
                    Ok(Ok(())) => {}
                    Err(join) if join.is_panic() => {
                        return Err(SessionError::Protocol { code: session_error::INTERNAL_ERROR, reason: "task panicked".into() })
                    }
                    Err(_) => {}
                },
            }
        }
    }

    // ------------------------------------------------------------------
    // Unidirectional streams: control, subgroup data, padding
    // ------------------------------------------------------------------

    async fn handle_uni(self: Arc<Self>, mut reader: Reader) -> Result<(), SessionError> {
        // Peek the stream type without consuming: for SETUP the stream type
        // and the message type are the same varint.
        let Some(ty) = reader.decode(|b| coding::get_varint(&mut &b[..])).await? else {
            return Ok(());
        };
        match ty {
            stream_type::SETUP => self.handle_control(reader).await,
            t if stream_type::is_subgroup_header(t) => self.handle_subgroup(reader).await,
            stream_type::PADDING => {
                while reader.decode(|b| coding::get_bytes(b, b.len().max(1))).await?.is_some() {}
                Ok(())
            }
            stream_type::FETCH_HEADER => Err(SessionError::protocol("unexpected FETCH stream (no FETCH sent)")),
            other => Err(SessionError::protocol(format!("unknown stream type {other:#x}"))),
        }
    }

    async fn handle_control(&self, mut reader: Reader) -> Result<(), SessionError> {
        if *self.setup_done.borrow() {
            return Err(SessionError::protocol("second control stream"));
        }
        match reader.message().await? {
            Some(Message::Setup(setup)) => {
                let implementation = setup
                    .options
                    .get_bytes(setup_option::MOQT_IMPLEMENTATION)
                    .map(|b| String::from_utf8_lossy(b).into_owned());
                if setup.options.get_bytes(setup_option::PATH).is_some()
                    || setup.options.get_bytes(setup_option::AUTHORITY).is_some()
                {
                    // PATH / AUTHORITY are for native QUIC only (§9.1.1, §9.1.2).
                    return Err(SessionError::Protocol {
                        code: 0x8,
                        reason: "PATH/AUTHORITY not allowed over WebTransport".into(),
                    });
                }
                tracing::info!(peer = ?implementation, "SETUP received");
                self.setup_done.send_replace(true);
            }
            _ => return Err(SessionError::protocol("control stream must start with SETUP")),
        }
        loop {
            match reader.message().await? {
                Some(Message::Goaway(g)) => tracing::info!(timeout_ms = g.timeout_ms, "peer sent GOAWAY"),
                Some(other) => {
                    return Err(SessionError::protocol(format!("message {:#x} not allowed on control stream", other.type_id())))
                }
                None => return Err(SessionError::protocol("control stream closed")),
            }
        }
    }

    async fn wait_alias(&self, alias: u64) -> Option<Arc<Track>> {
        let deadline = tokio::time::Instant::now() + UNKNOWN_ALIAS_WAIT;
        loop {
            let notified = self.upstream_changed.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            if let Some(track) = self.upstream.lock().unwrap().get(&alias) {
                return Some(track.clone());
            }
            if tokio::time::timeout_at(deadline, notified).await.is_err() {
                return None;
            }
        }
    }

    async fn handle_subgroup(&self, mut reader: Reader) -> Result<(), SessionError> {
        let header = reader
            .decode(|b| SubgroupHeader::decode(b))
            .await?
            .ok_or_else(|| SessionError::protocol("stream ended in SUBGROUP_HEADER"))?;
        let Some(track) = self.wait_alias(header.track_alias).await else {
            tracing::debug!(alias = header.track_alias, "dropping subgroup for unknown track alias");
            reader.into_inner().stop(VarInt::from_u32(reset_code::CANCELLED));
            return Ok(());
        };
        let subgroup = track.create_subgroup(header);
        let mut codec = SubgroupObjectCodec::new(&header);
        let result = loop {
            match reader.decode(|b| codec.decode(b)).await {
                Ok(Some(object)) => track.push_object(&subgroup, object),
                Ok(None) => break Ok(SubgroupEnd::Fin),
                Err(SessionError::Read(e)) => {
                    tracing::debug!("subgroup stream reset: {e}");
                    break Ok(SubgroupEnd::Reset);
                }
                Err(e) => break Err(e),
            }
        };
        subgroup.finish(*result.as_ref().unwrap_or(&SubgroupEnd::Reset));
        result.map(|_| ())
    }

    fn handle_datagram(&self, payload: Bytes) -> Result<(), SessionError> {
        let datagram = ObjectDatagram::decode(&mut payload.as_ref()).map_err(|e| match e {
            coding::DecodeError::Short => SessionError::protocol("truncated datagram"),
            e => e.into(),
        })?;
        // Datagrams that race ahead of SUBSCRIBE_OK are simply dropped.
        if let Some(track) = self.upstream.lock().unwrap().get(&datagram.track_alias) {
            track.push_datagram(datagram);
        }
        Ok(())
    }

    // ------------------------------------------------------------------
    // Bidirectional request streams
    // ------------------------------------------------------------------

    async fn handle_request(self: Arc<Self>, mut send: SendStream, mut reader: Reader) -> Result<(), SessionError> {
        let mut setup = self.setup_done.subscribe();
        timeout(SETUP_TIMEOUT, setup.wait_for(|done| *done))
            .await
            .map_err(|_| SessionError::Protocol {
                code: session_error::CONTROL_MESSAGE_TIMEOUT,
                reason: "no SETUP before request".into(),
            })?
            .map_err(|_| SessionError::protocol("session closing"))?;

        let Some(first) = reader.message().await? else { return Ok(()) };
        let request_id = match &first {
            Message::Subscribe(m) => Some(m.request_id),
            Message::PublishNamespace(m) => Some(m.request_id),
            Message::SubscribeNamespace(m) => Some(m.request_id),
            _ => None,
        };
        if let Some(id) = request_id {
            self.check_peer_request_id(id)?;
        }

        match first {
            Message::Subscribe(m) => self.serve_subscribe(send, reader, m).await,
            Message::PublishNamespace(m) => self.serve_publish_namespace(send, reader, m).await,
            Message::SubscribeNamespace(m) => self.serve_subscribe_namespace(send, reader, m).await,
            Message::Unsupported { ty, .. } if Message::is_request_first(ty) => {
                tracing::debug!(ty, "rejecting unsupported request type");
                let err = RequestError::new(request_error::NOT_SUPPORTED, "request type not supported by relay");
                write_message(&mut send, &Message::RequestError(err)).await?;
                send.finish().await?;
                Ok(())
            }
            other => Err(SessionError::protocol(format!("{:#x} cannot open a request stream", other.type_id()))),
        }
    }

    fn check_peer_request_id(&self, id: u64) -> Result<(), SessionError> {
        // Clients use even IDs (§6.4.2.1); duplicates are fatal too.
        if !id.is_multiple_of(2) || !self.peer_request_ids.lock().unwrap().insert(id) {
            return Err(SessionError::Protocol {
                code: session_error::INVALID_REQUEST_ID,
                reason: format!("invalid request id {id}"),
            });
        }
        Ok(())
    }

    async fn reject(send: &mut SendStream, err: RequestError) -> Result<(), SessionError> {
        tracing::info!(code = err.code, reason = %err.reason, "request rejected");
        write_message(send, &Message::RequestError(err)).await?;
        send.finish().await?;
        Ok(())
    }

    async fn serve_publish_namespace(
        self: Arc<Self>,
        mut send: SendStream,
        mut reader: Reader,
        msg: PublishNamespace,
    ) -> Result<(), SessionError> {
        let registration = match self.relay.publish_namespace(msg.prefix, &self) {
            Ok(r) => r,
            Err(e) => return Self::reject(&mut send, e).await,
        };
        write_message(&mut send, &Message::RequestOk(RequestOk::empty())).await?;

        // A FIN from the publisher does not withdraw the namespace; only
        // cancelling the request (reset / STOP_SENDING) or closing the session does.
        let mut recv_open = true;
        loop {
            tokio::select! {
                msg = next_request_message(&mut reader, &mut recv_open) => match msg {
                    Ok(Some(Message::RequestUpdate(_))) => {
                        write_message(&mut send, &Message::RequestOk(RequestOk::empty())).await?;
                    }
                    Ok(Some(other)) => return Err(SessionError::protocol(format!("unexpected {:#x} on PUBLISH_NAMESPACE stream", other.type_id()))),
                    Ok(None) => {}
                    Err(_) => break,
                },
                _ = send.stopped() => break,
            }
        }
        drop(registration);
        Ok(())
    }

    async fn serve_subscribe_namespace(
        self: Arc<Self>,
        mut send: SendStream,
        mut reader: Reader,
        msg: SubscribeNamespace,
    ) -> Result<(), SessionError> {
        let prefix = msg.prefix;
        let (current, mut events) = self.relay.watch_namespaces(&prefix);
        write_message(&mut send, &Message::RequestOk(RequestOk::empty())).await?;
        // Peers don't need to discover their own namespaces.
        for (ns, owner) in current {
            if owner != self.id {
                write_message(&mut send, &Message::Namespace(Namespace { suffix: ns.suffix_after(&prefix) })).await?;
            }
        }

        let mut recv_open = true;
        loop {
            tokio::select! {
                ev = events.recv() => match ev {
                    Ok(NamespaceEvent::Added { namespace, session_id }) if session_id != self.id && namespace.has_prefix(&prefix) => {
                        write_message(&mut send, &Message::Namespace(Namespace { suffix: namespace.suffix_after(&prefix) })).await?;
                    }
                    Ok(NamespaceEvent::Removed { namespace, session_id }) if session_id != self.id && namespace.has_prefix(&prefix) => {
                        write_message(&mut send, &Message::NamespaceDone(NamespaceDone { suffix: namespace.suffix_after(&prefix) })).await?;
                    }
                    Ok(_) => {}
                    Err(tokio::sync::broadcast::error::RecvError::Lagged(n)) => {
                        tracing::warn!(n, "namespace subscriber lagged; cancelling so it can resubscribe");
                        send.reset(VarInt::from_u32(0)).ok();
                        return Ok(());
                    }
                    Err(tokio::sync::broadcast::error::RecvError::Closed) => return Ok(()),
                },
                msg = next_request_message(&mut reader, &mut recv_open) => match msg {
                    Ok(Some(Message::RequestUpdate(_))) => {
                        let err = RequestError::new(request_error::NOT_SUPPORTED, "namespace prefix updates not supported");
                        write_message(&mut send, &Message::RequestError(err)).await?;
                    }
                    Ok(Some(other)) => return Err(SessionError::protocol(format!("unexpected {:#x} on SUBSCRIBE_NAMESPACE stream", other.type_id()))),
                    Ok(None) => {}
                    Err(_) => return Ok(()),
                },
                _ = send.stopped() => return Ok(()),
            }
        }
    }

    async fn serve_subscribe(self: Arc<Self>, mut send: SendStream, mut reader: Reader, sub: Subscribe) -> Result<(), SessionError> {
        tracing::info!(track = %sub.track, request_id = sub.request_id, "SUBSCRIBE");
        let handle = match self.relay.subscribe(sub.track.clone()).await {
            Ok(h) => h,
            Err(e) => return Self::reject(&mut send, e).await,
        };

        let alias = self.next_alias.fetch_add(1, Ordering::Relaxed);
        let mut params = Parameters::new();
        if let Some(largest) = handle.track.largest() {
            params.set(param::LARGEST_OBJECT, ParamValue::Location(largest));
        }
        let ok = SubscribeOk { track_alias: alias, params, track_properties: Properties::default() };
        write_message(&mut send, &Message::SubscribeOk(ok)).await?;

        let forward = self.clone().forward_track(handle, alias);
        tokio::pin!(forward);
        let mut recv_open = true;
        loop {
            tokio::select! {
                (status, stream_count) = &mut forward => {
                    let done = PublishDone { status, stream_count, reason: "track ended".into() };
                    write_message(&mut send, &Message::PublishDone(done)).await?;
                    send.finish().await?;
                    return Ok(());
                }
                msg = next_request_message(&mut reader, &mut recv_open) => match msg {
                    Ok(Some(Message::RequestUpdate(_))) => {
                        // Parameters are accepted but this relay doesn't act on them.
                        write_message(&mut send, &Message::RequestOk(RequestOk::empty())).await?;
                    }
                    Ok(Some(other)) => return Err(SessionError::protocol(format!("unexpected {:#x} on SUBSCRIBE stream", other.type_id()))),
                    Ok(None) => {}
                    Err(_) => {
                        tracing::info!(track = %sub.track, "unsubscribed");
                        return Ok(());
                    }
                },
                _ = send.stopped() => return Ok(()),
            }
        }
    }

    /// Pumps a relayed track to this session. Returns (PUBLISH_DONE status, stream count).
    async fn forward_track(self: Arc<Self>, handle: Arc<TrackHandle>, alias: u64) -> (u64, u64) {
        let mut reader = handle.track.reader();
        let mut streams = JoinSet::new();
        let mut in_flight: Vec<(u64, tokio::task::AbortHandle)> = Vec::new();
        let mut stream_count = 0;
        loop {
            tokio::select! {
                ev = reader.next() => match ev {
                    TrackEvent::Subgroup(subgroup) => {
                        let group = subgroup.header.group_id;
                        // Live media: drop subgroups more than one group behind.
                        in_flight.retain(|(g, abort)| {
                            let stale = g + 1 < group;
                            if stale { abort.abort(); }
                            !stale && !abort.is_finished()
                        });
                        stream_count += 1;
                        let conn = self.conn.clone();
                        let abort = streams.spawn(forward_subgroup(conn, subgroup, alias).in_current_span());
                        in_flight.push((group, abort));
                    }
                    TrackEvent::Datagram(datagram) => {
                        let mut d = (*datagram).clone();
                        d.track_alias = alias;
                        if let Err(e) = self.conn.send_datagram(d.encode_to_vec()) {
                            tracing::debug!("datagram dropped: {e}");
                        }
                    }
                    TrackEvent::Ended(status) => {
                        let _ = timeout(DRAIN_TIMEOUT, async { while streams.join_next().await.is_some() {} }).await;
                        return (status, stream_count);
                    }
                },
                Some(res) = streams.join_next() => {
                    if let Ok(Err(e)) = res {
                        tracing::debug!("subgroup forward ended: {e}");
                    }
                }
            }
        }
    }

    // ------------------------------------------------------------------
    // Upstream: this session is the publisher of a track a peer asked for
    // ------------------------------------------------------------------

    /// Subscribes to `track` on this (publisher) session and feeds it until the
    /// publisher ends it or the last downstream subscriber goes away.
    /// Returns the PUBLISH_DONE status to propagate downstream.
    pub async fn subscribe_upstream(
        self: &Arc<Self>,
        track: Arc<Track>,
        ready: watch::Sender<Option<Result<(), RequestError>>>,
        cancel: oneshot::Receiver<()>,
    ) -> Option<u64> {
        let result = self.subscribe_upstream_inner(&track, &ready, cancel).await;
        ready.send_if_modified(|r| {
            if r.is_none() {
                *r = Some(Err(RequestError::new(request_error::INTERNAL_ERROR, "upstream subscribe failed")));
                true
            } else {
                false
            }
        });
        match result {
            Ok(status) => status,
            Err(e) => {
                tracing::warn!(track = %track.name, "upstream subscription failed: {e}");
                if let Some(code) = e.close_code() {
                    self.conn.close(VarInt::from_u32(code), e.to_string().as_bytes());
                }
                None
            }
        }
    }

    async fn subscribe_upstream_inner(
        self: &Arc<Self>,
        track: &Arc<Track>,
        ready: &watch::Sender<Option<Result<(), RequestError>>>,
        mut cancel: oneshot::Receiver<()>,
    ) -> Result<Option<u64>, SessionError> {
        let (mut send, recv) = self.conn.open_bi().await?.await?;
        let mut reader = Reader::new(recv);
        let request_id = self.next_request_id.fetch_add(2, Ordering::Relaxed);
        let subscribe = Subscribe {
            request_id,
            track: track.name.clone(),
            // Start at the beginning of the current group so video starts on a keyframe.
            params: Parameters::new().with(
                param::LOCATION_FILTER,
                ParamValue::LocationFilter(LocationFilter::RelativeStart { start_group: 1 }),
            ),
        };
        tracing::info!(track = %track.name, request_id, "SUBSCRIBE upstream");
        write_message(&mut send, &Message::Subscribe(subscribe)).await?;

        let alias = match reader.message().await? {
            Some(Message::SubscribeOk(ok)) => ok.track_alias,
            Some(Message::RequestError(err)) => {
                tracing::info!(track = %track.name, code = err.code, reason = %err.reason, "upstream rejected SUBSCRIBE");
                ready.send_replace(Some(Err(err)));
                return Ok(Some(publish_done::INTERNAL_ERROR));
            }
            Some(other) => return Err(SessionError::protocol(format!("unexpected {:#x} in reply to SUBSCRIBE", other.type_id()))),
            None => return Err(SessionError::protocol("SUBSCRIBE stream closed without response")),
        };

        {
            let mut upstream = self.upstream.lock().unwrap();
            if upstream.contains_key(&alias) {
                return Err(SessionError::Protocol {
                    code: session_error::DUPLICATE_TRACK_ALIAS,
                    reason: format!("track alias {alias} already in use"),
                });
            }
            upstream.insert(alias, track.clone());
        }
        self.upstream_changed.notify_waiters();
        ready.send_replace(Some(Ok(())));

        let status = loop {
            tokio::select! {
                _ = &mut cancel => {
                    // Last downstream subscriber left: cancel the request (§6.4.2.3).
                    tracing::info!(track = %track.name, "no subscribers left, unsubscribing upstream");
                    send.reset(VarInt::from_u32(reset_code::CANCELLED)).ok();
                    reader.into_inner().stop(VarInt::from_u32(reset_code::CANCELLED));
                    break None;
                }
                msg = reader.message() => match msg {
                    Ok(Some(Message::PublishDone(done))) => {
                        tracing::info!(track = %track.name, status = done.status, "PUBLISH_DONE from publisher");
                        send.finish().await.ok();
                        break Some(done.status);
                    }
                    Ok(Some(Message::RequestOk(_) | Message::RequestError(_))) => {}
                    Ok(Some(other)) => return Err(SessionError::protocol(format!("unexpected {:#x} on SUBSCRIBE stream", other.type_id()))),
                    Ok(None) => break Some(publish_done::TRACK_ENDED),
                    Err(_) => break Some(publish_done::INTERNAL_ERROR),
                },
            }
        };
        self.upstream.lock().unwrap().remove(&alias);
        Ok(status)
    }
}

/// Next message on a request stream; pends forever once the peer sent FIN so
/// callers can keep waiting on their other branches.
async fn next_request_message(reader: &mut Reader, open: &mut bool) -> Result<Option<Message>, SessionError> {
    if !*open {
        return std::future::pending().await;
    }
    let msg = reader.message().await?;
    if msg.is_none() {
        *open = false;
    }
    Ok(msg)
}

/// Resets the stream unless it finished cleanly: dropping a quinn stream would
/// otherwise imply FIN, which would wrongly tell the peer the subgroup is complete.
struct ResetOnDrop(Option<SendStream>);

impl Drop for ResetOnDrop {
    fn drop(&mut self) {
        if let Some(mut s) = self.0.take() {
            let _ = s.reset(VarInt::from_u32(reset_code::CANCELLED));
        }
    }
}

async fn forward_subgroup(conn: Connection, subgroup: Arc<Subgroup>, alias: u64) -> Result<(), SessionError> {
    let mut header = subgroup.header;
    header.track_alias = alias;
    let mut objects = subgroup.reader();

    let stream = conn.open_uni().await?.await?;
    let mut guard = ResetOnDrop(Some(stream));
    let stream = guard.0.as_mut().expect("present");
    // quinn: larger = more urgent. Lower publisher priority value wins, then newer groups.
    let publisher_priority = header.publisher_priority.unwrap_or(128) as i32;
    stream.set_priority(((255 - publisher_priority) << 20) | (header.group_id & 0xF_FFFF) as i32);

    let mut buf = header.encode_to_vec();
    let mut codec = SubgroupObjectCodec::new(&header);
    loop {
        match objects.next().await {
            Ok(object) => {
                codec.encode(&object, &mut buf);
                stream.write_all(&buf).await?;
                buf.clear();
            }
            Err(SubgroupEnd::Fin) => {
                if !buf.is_empty() {
                    stream.write_all(&buf).await?;
                }
                let mut stream = guard.0.take().expect("present");
                stream.finish().await?;
                return Ok(());
            }
            Err(SubgroupEnd::Reset) => return Ok(()), // guard resets
        }
    }
}
