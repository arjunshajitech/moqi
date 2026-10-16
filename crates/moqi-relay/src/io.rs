//! Framed I/O over WebTransport streams using the sans-IO `moqi-proto` codec.

use bytes::{Buf, BytesMut};
use moqi_proto::coding::{self, DecodeError};
use moqi_proto::message::Message;
use moqi_proto::Encode;
use wtransport::{RecvStream, SendStream};

use crate::error::SessionError;

/// Buffered reader that feeds a `RecvStream` into incremental decoders.
pub struct Reader {
    stream: RecvStream,
    buf: BytesMut,
}

impl Reader {
    pub fn new(stream: RecvStream) -> Self {
        Self { stream, buf: BytesMut::with_capacity(4096) }
    }

    /// Runs `decode` until it yields a value. Returns `Ok(None)` on a clean FIN
    /// at a frame boundary; a FIN mid-frame is a protocol violation (§11.3).
    pub async fn decode<T>(
        &mut self,
        mut decode: impl FnMut(&mut &[u8]) -> coding::Result<T>,
    ) -> Result<Option<T>, SessionError> {
        loop {
            if !self.buf.is_empty() {
                let mut slice: &[u8] = &self.buf;
                match decode(&mut slice) {
                    Ok(v) => {
                        let used = self.buf.len() - slice.len();
                        self.buf.advance(used);
                        return Ok(Some(v));
                    }
                    Err(DecodeError::Short) => {}
                    Err(e) => return Err(e.into()),
                }
            }
            if !self.fill().await? {
                return if self.buf.is_empty() {
                    Ok(None)
                } else {
                    Err(SessionError::protocol("stream ended mid-frame"))
                };
            }
        }
    }

    pub async fn message(&mut self) -> Result<Option<Message>, SessionError> {
        self.decode(|b| Message::decode_framed(b)).await
    }

    /// Reads more bytes; returns false on FIN.
    async fn fill(&mut self) -> Result<bool, SessionError> {
        let mut tmp = [0u8; 16 * 1024];
        match self.stream.read(&mut tmp).await? {
            Some(n) => {
                self.buf.extend_from_slice(&tmp[..n]);
                Ok(true)
            }
            None => Ok(false),
        }
    }

    pub fn into_inner(self) -> RecvStream {
        self.stream
    }
}

pub async fn write_message(stream: &mut SendStream, msg: &Message) -> Result<(), SessionError> {
    tracing::trace!(?msg, "send");
    stream.write_all(&msg.encode_to_vec()).await?;
    Ok(())
}
