//! Sans-IO codec for Media over QUIC Transport, draft-ietf-moq-transport-22.
//!
//! - [`coding`]: MoQT variable-length integers and primitives (§8.1)
//! - [`types`]: Location, Track Namespace, Properties, Message Parameters (§8, §9.20)
//! - [`message`]: control / request-stream messages (§9)
//! - [`data`]: subgroup streams and object datagrams (§11)

pub mod coding;
pub mod data;
pub mod message;
pub mod types;

pub use coding::{Decode, DecodeError, Encode};

/// ALPN / WT-Available-Protocols token for this draft (§6.2).
pub const ALPN: &str = "moqt-22";
