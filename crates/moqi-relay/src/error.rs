use moqi_proto::message::session_error;
use moqi_proto::DecodeError;
use wtransport::error::{ConnectionError, StreamOpeningError, StreamReadError, StreamWriteError};

/// Errors that terminate a whole MoQT session (§12.2) or one of its streams.
#[derive(Debug, thiserror::Error)]
pub enum SessionError {
    #[error("protocol error {code:#x}: {reason}")]
    Protocol { code: u32, reason: String },
    #[error("connection: {0}")]
    Connection(#[from] ConnectionError),
    #[error("stream read: {0}")]
    Read(#[from] StreamReadError),
    #[error("stream write: {0}")]
    Write(#[from] StreamWriteError),
    #[error("stream open: {0}")]
    Open(#[from] StreamOpeningError),
}

impl SessionError {
    pub fn protocol(reason: impl Into<String>) -> Self {
        Self::Protocol { code: session_error::PROTOCOL_VIOLATION, reason: reason.into() }
    }

    /// Session termination code if this error must close the session.
    pub fn close_code(&self) -> Option<u32> {
        match self {
            Self::Protocol { code, .. } => Some(*code),
            _ => None,
        }
    }
}

impl From<DecodeError> for SessionError {
    fn from(e: DecodeError) -> Self {
        let code = match e {
            DecodeError::KeyValueFormatting => session_error::KEY_VALUE_FORMATTING_ERROR,
            _ => session_error::PROTOCOL_VIOLATION,
        };
        Self::Protocol { code, reason: e.to_string() }
    }
}
