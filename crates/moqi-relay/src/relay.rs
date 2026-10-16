//! Relay-wide routing state (§7): which session publishes which namespace,
//! namespace-discovery fan-out, and de-duplicated upstream subscriptions.
//!
//! One upstream SUBSCRIBE serves every downstream subscriber of the same Full
//! Track Name. The upstream subscription lives exactly as long as at least one
//! downstream subscriber holds its [`TrackHandle`].

use std::collections::HashMap;
use std::sync::{Arc, Mutex, Weak};

use moqi_proto::message::{publish_done, request_error, RequestError};
use moqi_proto::types::{FullTrackName, TrackNamespace};
use tokio::sync::{broadcast, oneshot, watch};

use crate::session::Session;
use crate::track::Track;

#[derive(Debug, Clone)]
pub struct RelayConfig {
    /// Namespace depth that identifies a room, e.g. `["moqi", "<room>"]` = 2.
    pub room_depth: usize,
    /// Maximum number of distinct publishers per room.
    pub max_peers_per_room: usize,
}

#[derive(Debug, Clone)]
pub enum NamespaceEvent {
    Added { namespace: TrackNamespace, session_id: u64 },
    Removed { namespace: TrackNamespace, session_id: u64 },
}

struct Publisher {
    session: Weak<Session>,
}

#[derive(Default)]
struct Inner {
    published: HashMap<TrackNamespace, Publisher>,
    tracks: HashMap<FullTrackName, Weak<TrackHandle>>,
}

pub struct Relay {
    pub config: RelayConfig,
    inner: Mutex<Inner>,
    namespace_events: broadcast::Sender<NamespaceEvent>,
}

/// Shared handle to a relayed track; dropping the last one cancels upstream.
pub struct TrackHandle {
    pub track: Arc<Track>,
    ready: watch::Receiver<Option<Result<(), RequestError>>>,
    _cancel: oneshot::Sender<()>,
}

impl TrackHandle {
    async fn wait_ready(&self) -> Result<(), RequestError> {
        let mut rx = self.ready.clone();
        let res = rx.wait_for(Option::is_some).await;
        match res {
            Ok(v) => v.clone().expect("checked is_some"),
            Err(_) => Err(RequestError::new(request_error::INTERNAL_ERROR, "upstream vanished")),
        }
    }
}

/// Keeps a namespace published until dropped.
pub struct NamespaceRegistration {
    relay: Arc<Relay>,
    namespace: TrackNamespace,
    session_id: u64,
}

impl Drop for NamespaceRegistration {
    fn drop(&mut self) {
        self.relay.inner.lock().unwrap().published.remove(&self.namespace);
        tracing::info!(namespace = %self.namespace, session = self.session_id, "namespace withdrawn");
        let _ = self.relay.namespace_events.send(NamespaceEvent::Removed {
            namespace: self.namespace.clone(),
            session_id: self.session_id,
        });
    }
}

impl Relay {
    pub fn new(config: RelayConfig) -> Arc<Self> {
        Arc::new(Self { config, inner: Mutex::default(), namespace_events: broadcast::Sender::new(1024) })
    }

    pub fn publish_namespace(
        self: &Arc<Self>,
        namespace: TrackNamespace,
        session: &Arc<Session>,
    ) -> Result<NamespaceRegistration, RequestError> {
        if namespace.is_empty() {
            return Err(RequestError::new(request_error::DOES_NOT_EXIST, "empty namespace"));
        }
        let mut inner = self.inner.lock().unwrap();
        // Overlapping prefixes would make routing ambiguous.
        if inner.published.keys().any(|ns| ns.has_prefix(&namespace) || namespace.has_prefix(ns)) {
            return Err(RequestError::new(request_error::PREFIX_OVERLAP, "namespace already published"));
        }
        let depth = self.config.room_depth;
        if namespace.len() > depth {
            let room = namespace.truncated(depth);
            let peers = inner.published.keys().filter(|ns| ns.len() > depth && ns.has_prefix(&room)).count();
            if peers >= self.config.max_peers_per_room {
                tracing::warn!(%room, peers, "room full, rejecting publisher");
                return Err(RequestError {
                    code: request_error::EXCESSIVE_LOAD,
                    retry_interval: 0,
                    reason: format!("{} participants max", self.config.max_peers_per_room),
                    redirect: None,
                });
            }
        }
        inner.published.insert(namespace.clone(), Publisher { session: Arc::downgrade(session) });
        drop(inner);

        tracing::info!(%namespace, session = session.id, "namespace published");
        let _ = self
            .namespace_events
            .send(NamespaceEvent::Added { namespace: namespace.clone(), session_id: session.id });
        Ok(NamespaceRegistration { relay: self.clone(), namespace, session_id: session.id })
    }

    /// Snapshot of published namespaces under `prefix` plus a live event feed.
    /// Subscribing to events before taking the snapshot avoids missing updates.
    pub fn watch_namespaces(
        &self,
        prefix: &TrackNamespace,
    ) -> (Vec<(TrackNamespace, u64)>, broadcast::Receiver<NamespaceEvent>) {
        let events = self.namespace_events.subscribe();
        let inner = self.inner.lock().unwrap();
        let current = inner
            .published
            .iter()
            .filter(|(ns, _)| ns.has_prefix(prefix))
            .filter_map(|(ns, p)| p.session.upgrade().map(|s| (ns.clone(), s.id)))
            .collect();
        (current, events)
    }

    fn find_publisher(inner: &Inner, namespace: &TrackNamespace) -> Option<Arc<Session>> {
        inner
            .published
            .iter()
            .filter(|(ns, _)| namespace.has_prefix(ns))
            .max_by_key(|(ns, _)| ns.len())
            .and_then(|(_, p)| p.session.upgrade())
    }

    /// Get or create the relayed track, subscribing upstream if needed.
    pub async fn subscribe(self: &Arc<Self>, name: FullTrackName) -> Result<Arc<TrackHandle>, RequestError> {
        let handle = {
            let mut inner = self.inner.lock().unwrap();
            if let Some(existing) = inner.tracks.get(&name).and_then(Weak::upgrade) {
                existing
            } else {
                let publisher = Self::find_publisher(&inner, &name.namespace)
                    .ok_or_else(|| RequestError::new(request_error::DOES_NOT_EXIST, "no publisher for namespace"))?;
                let track = Track::new(name.clone());
                let (ready_tx, ready_rx) = watch::channel(None);
                let (cancel_tx, cancel_rx) = oneshot::channel();
                let handle = Arc::new(TrackHandle { track: track.clone(), ready: ready_rx, _cancel: cancel_tx });
                inner.tracks.insert(name.clone(), Arc::downgrade(&handle));

                let relay = self.clone();
                tokio::spawn(async move {
                    let status = publisher.subscribe_upstream(track.clone(), ready_tx, cancel_rx).await;
                    track.end(status.unwrap_or(publish_done::INTERNAL_ERROR));
                    relay.forget_track(&track);
                });
                handle
            }
        };
        handle.wait_ready().await?;
        Ok(handle)
    }

    fn forget_track(&self, track: &Arc<Track>) {
        let mut inner = self.inner.lock().unwrap();
        let stale = match inner.tracks.get(&track.name).and_then(Weak::upgrade) {
            Some(h) => Arc::ptr_eq(&h.track, track),
            None => true,
        };
        if stale {
            inner.tracks.remove(&track.name);
        }
    }
}
