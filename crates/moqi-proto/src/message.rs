//! Control and request-stream messages (§9).
//!
//! Framing: `Type (vi64) | Length (16) | Body`.

use bytes::{Buf, BufMut, Bytes};

use crate::coding::*;
use crate::types::*;

pub mod msg_type {
    pub const SETUP: u64 = 0x2F00;
    pub const GOAWAY: u64 = 0x10;
    pub const REQUEST_UPDATE: u64 = 0x2;
    pub const SUBSCRIBE: u64 = 0x3;
    pub const SUBSCRIBE_OK: u64 = 0x4;
    pub const REQUEST_ERROR: u64 = 0x5;
    pub const PUBLISH_NAMESPACE: u64 = 0x6;
    pub const REQUEST_OK: u64 = 0x7;
    pub const NAMESPACE: u64 = 0x8;
    pub const PUBLISH_DONE: u64 = 0xB;
    pub const TRACK_STATUS: u64 = 0xD;
    pub const NAMESPACE_DONE: u64 = 0xE;
    pub const PUBLISH_SKIPPED: u64 = 0xF;
    pub const FETCH: u64 = 0x16;
    pub const FETCH_OK: u64 = 0x18;
    pub const PUBLISH: u64 = 0x1D;
    pub const PUBLISH_STATE_NOTIFY: u64 = 0x22;
    pub const SUBSCRIBE_NAMESPACE: u64 = 0x50;
    pub const SUBSCRIBE_TRACKS: u64 = 0x51;
}

/// Setup Option types (§9.1, §16.4).
pub mod setup_option {
    pub const PATH: u64 = 0x01;
    pub const AUTHORIZATION_TOKEN: u64 = 0x03;
    pub const MAX_AUTH_TOKEN_CACHE_SIZE: u64 = 0x04;
    pub const AUTHORITY: u64 = 0x05;
    pub const MAX_FILTER_RANGES: u64 = 0x06;
    pub const MOQT_IMPLEMENTATION: u64 = 0x07;
    pub const MAX_REQUEST_UPDATES: u64 = 0x08;
}

/// Session termination codes (§12.2).
pub mod session_error {
    pub const NO_ERROR: u32 = 0x0;
    pub const INTERNAL_ERROR: u32 = 0x1;
    pub const UNAUTHORIZED: u32 = 0x2;
    pub const PROTOCOL_VIOLATION: u32 = 0x3;
    pub const INVALID_REQUEST_ID: u32 = 0x4;
    pub const DUPLICATE_TRACK_ALIAS: u32 = 0x5;
    pub const KEY_VALUE_FORMATTING_ERROR: u32 = 0x6;
    pub const CONTROL_MESSAGE_TIMEOUT: u32 = 0x11;
}

/// REQUEST_ERROR codes (§12.3).
pub mod request_error {
    pub const INTERNAL_ERROR: u64 = 0x0;
    pub const UNAUTHORIZED: u64 = 0x1;
    pub const TIMEOUT: u64 = 0x2;
    pub const NOT_SUPPORTED: u64 = 0x3;
    pub const GOING_AWAY: u64 = 0x6;
    pub const EXCESSIVE_LOAD: u64 = 0x9;
    pub const DOES_NOT_EXIST: u64 = 0x10;
    pub const UNINTERESTED: u64 = 0x20;
    pub const PREFIX_OVERLAP: u64 = 0x30;
    pub const REDIRECT: u64 = 0x34;
}

/// PUBLISH_DONE status codes (§12.4).
pub mod publish_done {
    pub const INTERNAL_ERROR: u64 = 0x0;
    pub const UNAUTHORIZED: u64 = 0x1;
    pub const TRACK_ENDED: u64 = 0x2;
    pub const GOING_AWAY: u64 = 0x4;
    pub const TOO_FAR_BEHIND: u64 = 0x5;
    pub const EXPIRED: u64 = 0x6;
}

/// Data stream reset codes (§12.5).
pub mod reset_code {
    pub const INTERNAL_ERROR: u32 = 0x0;
    pub const CANCELLED: u32 = 0x1;
    pub const DELIVERY_TIMEOUT: u32 = 0x2;
    pub const SESSION_CLOSED: u32 = 0x3;
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Setup {
    /// Setup Options; unknown options are preserved and ignored.
    pub options: Properties,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Goaway {
    pub new_session_uri: Bytes,
    pub timeout_ms: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Subscribe {
    pub request_id: u64,
    pub track: FullTrackName,
    pub params: Parameters,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SubscribeOk {
    pub track_alias: u64,
    pub params: Parameters,
    pub track_properties: Properties,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct RequestOk {
    pub params: Parameters,
    pub track_properties: Properties,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RequestError {
    pub code: u64,
    /// Milliseconds + 1; 0 means do not retry.
    pub retry_interval: u64,
    pub reason: String,
    /// Raw Redirect structure, only present when `code == REDIRECT`.
    pub redirect: Option<Bytes>,
}

impl RequestOk {
    pub fn empty() -> Self {
        Self::default()
    }
}

impl RequestError {
    pub fn new(code: u64, reason: impl Into<String>) -> Self {
        Self { code, retry_interval: 0, reason: reason.into(), redirect: None }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RequestUpdate {
    pub request_id: u64,
    pub params: Parameters,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PublishDone {
    pub status: u64,
    pub stream_count: u64,
    pub reason: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PublishNamespace {
    pub request_id: u64,
    pub prefix: TrackNamespace,
    pub params: Parameters,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SubscribeNamespace {
    pub request_id: u64,
    pub prefix: TrackNamespace,
    pub params: Parameters,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Namespace {
    pub suffix: TrackNamespace,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NamespaceDone {
    pub suffix: TrackNamespace,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Message {
    Setup(Setup),
    Goaway(Goaway),
    Subscribe(Subscribe),
    SubscribeOk(SubscribeOk),
    RequestOk(RequestOk),
    RequestError(RequestError),
    RequestUpdate(RequestUpdate),
    PublishDone(PublishDone),
    PublishNamespace(PublishNamespace),
    SubscribeNamespace(SubscribeNamespace),
    Namespace(Namespace),
    NamespaceDone(NamespaceDone),
    /// A message type defined by the spec that this implementation does not
    /// interpret (PUBLISH, FETCH, TRACK_STATUS, ...). Kept as raw body so the
    /// peer can be answered with NOT_SUPPORTED instead of killing the session.
    Unsupported { ty: u64, body: Bytes },
}

impl Message {
    pub fn type_id(&self) -> u64 {
        use msg_type::*;
        match self {
            Self::Setup(_) => SETUP,
            Self::Goaway(_) => GOAWAY,
            Self::Subscribe(_) => SUBSCRIBE,
            Self::SubscribeOk(_) => SUBSCRIBE_OK,
            Self::RequestOk(_) => REQUEST_OK,
            Self::RequestError(_) => REQUEST_ERROR,
            Self::RequestUpdate(_) => REQUEST_UPDATE,
            Self::PublishDone(_) => PUBLISH_DONE,
            Self::PublishNamespace(_) => PUBLISH_NAMESPACE,
            Self::SubscribeNamespace(_) => SUBSCRIBE_NAMESPACE,
            Self::Namespace(_) => NAMESPACE,
            Self::NamespaceDone(_) => NAMESPACE_DONE,
            Self::Unsupported { ty, .. } => *ty,
        }
    }

    /// True for types that may open a bidirectional request stream (§6.3).
    pub fn is_request_first(ty: u64) -> bool {
        use msg_type::*;
        matches!(
            ty,
            TRACK_STATUS | SUBSCRIBE | PUBLISH | FETCH | PUBLISH_NAMESPACE | SUBSCRIBE_NAMESPACE | SUBSCRIBE_TRACKS
        )
    }

    fn encode_body<B: BufMut>(&self, buf: &mut B) {
        match self {
            Self::Setup(m) => m.options.encode(buf),
            Self::Goaway(m) => {
                put_lp_bytes(buf, &m.new_session_uri);
                put_varint(buf, m.timeout_ms);
            }
            Self::Subscribe(m) => {
                put_varint(buf, m.request_id);
                m.track.encode_parts(buf);
                m.params.encode(buf);
            }
            Self::SubscribeOk(m) => {
                put_varint(buf, m.track_alias);
                m.params.encode(buf);
                m.track_properties.encode(buf);
            }
            Self::RequestOk(m) => {
                m.params.encode(buf);
                m.track_properties.encode(buf);
            }
            Self::RequestError(m) => {
                put_varint(buf, m.code);
                put_varint(buf, m.retry_interval);
                put_reason(buf, &m.reason);
                if let Some(r) = &m.redirect {
                    buf.put_slice(r);
                }
            }
            Self::RequestUpdate(m) => {
                put_varint(buf, m.request_id);
                m.params.encode(buf);
            }
            Self::PublishDone(m) => {
                put_varint(buf, m.status);
                put_varint(buf, m.stream_count);
                put_reason(buf, &m.reason);
            }
            Self::PublishNamespace(m) => {
                put_varint(buf, m.request_id);
                m.prefix.encode(buf);
                m.params.encode(buf);
            }
            Self::SubscribeNamespace(m) => {
                put_varint(buf, m.request_id);
                m.prefix.encode(buf);
                m.params.encode(buf);
            }
            Self::Namespace(m) => m.suffix.encode(buf),
            Self::NamespaceDone(m) => m.suffix.encode(buf),
            Self::Unsupported { body, .. } => buf.put_slice(body),
        }
    }

    fn decode_body(ty: u64, body: Bytes) -> Result<Self> {
        use msg_type::*;
        let b = &mut body.clone();
        let msg = match ty {
            SETUP => Self::Setup(Setup { options: Properties::decode_all(b)? }),
            GOAWAY => Self::Goaway(Goaway { new_session_uri: get_lp_bytes(b)?, timeout_ms: get_varint(b)? }),
            SUBSCRIBE => Self::Subscribe(Subscribe {
                request_id: get_varint(b)?,
                track: FullTrackName::decode_parts(b)?,
                params: Parameters::decode(b)?,
            }),
            SUBSCRIBE_OK => Self::SubscribeOk(SubscribeOk {
                track_alias: get_varint(b)?,
                params: Parameters::decode(b)?,
                track_properties: Properties::decode_all(b)?,
            }),
            REQUEST_OK => Self::RequestOk(RequestOk {
                params: Parameters::decode(b)?,
                track_properties: Properties::decode_all(b)?,
            }),
            REQUEST_ERROR => {
                let code = get_varint(b)?;
                let retry_interval = get_varint(b)?;
                let reason = get_reason(b)?;
                let redirect = (code == request_error::REDIRECT).then(|| b.copy_to_bytes(b.remaining()));
                Self::RequestError(RequestError { code, retry_interval, reason, redirect })
            }
            REQUEST_UPDATE => Self::RequestUpdate(RequestUpdate {
                request_id: get_varint(b)?,
                params: Parameters::decode(b)?,
            }),
            PUBLISH_DONE => Self::PublishDone(PublishDone {
                status: get_varint(b)?,
                stream_count: get_varint(b)?,
                reason: get_reason(b)?,
            }),
            PUBLISH_NAMESPACE => Self::PublishNamespace(PublishNamespace {
                request_id: get_varint(b)?,
                prefix: TrackNamespace::decode(b)?,
                params: Parameters::decode(b)?,
            }),
            SUBSCRIBE_NAMESPACE => Self::SubscribeNamespace(SubscribeNamespace {
                request_id: get_varint(b)?,
                prefix: TrackNamespace::decode(b)?,
                params: Parameters::decode(b)?,
            }),
            NAMESPACE => Self::Namespace(Namespace { suffix: TrackNamespace::decode(b)? }),
            NAMESPACE_DONE => Self::NamespaceDone(NamespaceDone { suffix: TrackNamespace::decode(b)? }),
            TRACK_STATUS | PUBLISH | FETCH | FETCH_OK | SUBSCRIBE_TRACKS | PUBLISH_SKIPPED
            | PUBLISH_STATE_NOTIFY => {
                return Ok(Self::Unsupported { ty, body });
            }
            _ => return Err(DecodeError::UnknownMessage(ty)),
        };
        if b.has_remaining() {
            // Length must match the body exactly (§9).
            return Err(DecodeError::ProtocolViolation("message length mismatch"));
        }
        Ok(msg)
    }
}

impl Encode for Message {
    fn encode<B: BufMut>(&self, buf: &mut B) {
        let mut body = Vec::with_capacity(64);
        self.encode_body(&mut body);
        assert!(body.len() <= u16::MAX as usize, "control message exceeds 65535 bytes");
        put_varint(buf, self.type_id());
        buf.put_u16(body.len() as u16);
        buf.put_slice(&body);
    }
}

impl Message {
    /// Decode one framed message. Returns `Short` without consuming input when
    /// the buffer does not yet hold the whole frame.
    pub fn decode_framed(buf: &mut impl Buf) -> Result<Self> {
        // Peek the header without consuming in case the body isn't complete.
        let chunk = buf.chunk();
        let mut peek = chunk;
        let ty = get_varint(&mut peek)?;
        let len = get_u16(&mut peek)? as usize;
        let header = chunk.len() - peek.len();
        if buf.remaining() < header + len {
            return Err(DecodeError::Short);
        }
        buf.advance(header);
        let body = buf.copy_to_bytes(len);
        Self::decode_body(ty, body).map_err(|e| match e {
            // A complete frame whose body is truncated is malformed, not short.
            DecodeError::Short => DecodeError::ProtocolViolation("truncated message body"),
            e => e,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn roundtrip(m: Message) {
        let enc = m.encode_to_vec();
        let mut b = enc.as_slice();
        assert_eq!(Message::decode_framed(&mut b).unwrap(), m);
        assert!(b.is_empty());
        // Every strict prefix must report Short.
        for i in 0..enc.len() {
            let mut p = &enc[..i];
            assert_eq!(Message::decode_framed(&mut p), Err(DecodeError::Short), "prefix {i}");
        }
    }

    #[test]
    fn messages_roundtrip() {
        let ns = TrackNamespace::new(["moqi", "room1", "alice"]);
        let mut opts = Properties::default();
        opts.0.push((setup_option::MOQT_IMPLEMENTATION, KvValue::Bytes(Bytes::from_static(b"moq-rs-relay/0.1"))));
        roundtrip(Message::Setup(Setup { options: opts }));
        roundtrip(Message::Subscribe(Subscribe {
            request_id: 2,
            track: FullTrackName::new(ns.clone(), "video"),
            params: Parameters::new()
                .with(param::SUBSCRIBER_PRIORITY, ParamValue::U8(10))
                .with(param::LOCATION_FILTER, ParamValue::LocationFilter(LocationFilter::RelativeStart { start_group: 1 })),
        }));
        roundtrip(Message::SubscribeOk(SubscribeOk {
            track_alias: 5,
            params: Parameters::new().with(param::LARGEST_OBJECT, ParamValue::Location(Location::new(3, 9))),
            track_properties: Properties::default(),
        }));
        roundtrip(Message::RequestOk(RequestOk { params: Parameters::new(), track_properties: Properties::default() }));
        roundtrip(Message::RequestError(RequestError::new(request_error::DOES_NOT_EXIST, "nope")));
        roundtrip(Message::PublishDone(PublishDone { status: publish_done::TRACK_ENDED, stream_count: 4, reason: "bye".into() }));
        roundtrip(Message::PublishNamespace(PublishNamespace { request_id: 0, prefix: ns.clone(), params: Parameters::new() }));
        roundtrip(Message::SubscribeNamespace(SubscribeNamespace {
            request_id: 4,
            prefix: ns.truncated(2),
            params: Parameters::new(),
        }));
        roundtrip(Message::Namespace(Namespace { suffix: TrackNamespace::new(["bob"]) }));
        roundtrip(Message::NamespaceDone(NamespaceDone { suffix: TrackNamespace::new(["bob"]) }));
        roundtrip(Message::Goaway(Goaway { new_session_uri: Bytes::new(), timeout_ms: 1000 }));
    }

    #[test]
    fn unknown_type_is_error() {
        let mut v = Vec::new();
        put_varint(&mut v, 0x7e);
        v.extend_from_slice(&[0, 0]);
        assert_eq!(Message::decode_framed(&mut v.as_slice()), Err(DecodeError::UnknownMessage(0x7e)));
    }

    #[test]
    fn length_mismatch_is_violation() {
        let mut v = Vec::new();
        put_varint(&mut v, msg_type::NAMESPACE);
        v.extend_from_slice(&[0, 3, 0x00, 0xAA, 0xBB]); // empty namespace + 2 junk bytes
        assert!(matches!(Message::decode_framed(&mut v.as_slice()), Err(DecodeError::ProtocolViolation(_))));
    }
}
