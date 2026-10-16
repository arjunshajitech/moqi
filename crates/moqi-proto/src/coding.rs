//! Primitive encoders/decoders (draft-ietf-moq-transport-22 §8).
//!
//! Note: MoQT-22 uses its *own* variable-length integer format (leading-ones
//! prefix, 1..=9 bytes), not the QUIC varint from RFC 9000 §16.

use bytes::{Buf, BufMut, Bytes};

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum DecodeError {
    /// Not enough bytes yet; the caller should read more and retry.
    #[error("buffer too short")]
    Short,
    #[error("protocol violation: {0}")]
    ProtocolViolation(&'static str),
    #[error("unknown message type {0:#x}")]
    UnknownMessage(u64),
    #[error("unknown parameter type {0:#x}")]
    UnknownParameter(u64),
    #[error("key-value formatting error")]
    KeyValueFormatting,
}

pub type Result<T> = std::result::Result<T, DecodeError>;

pub trait Encode {
    fn encode<B: BufMut>(&self, buf: &mut B);

    fn encode_to_vec(&self) -> Vec<u8> {
        let mut v = Vec::new();
        self.encode(&mut v);
        v
    }
}

pub trait Decode: Sized {
    fn decode<B: Buf>(buf: &mut B) -> Result<Self>;
}

pub const VARINT_MAX_LEN: usize = 9;

/// Number of bytes needed for the minimal encoding of `v`.
pub fn varint_len(v: u64) -> usize {
    // Each extra byte adds 7 usable bits, up to 8 bytes (56 bits); then 9 bytes.
    match 64 - v.leading_zeros() {
        0..=7 => 1,
        8..=14 => 2,
        15..=21 => 3,
        22..=28 => 4,
        29..=35 => 5,
        36..=42 => 6,
        43..=49 => 7,
        50..=56 => 8,
        _ => 9,
    }
}

pub fn put_varint<B: BufMut>(buf: &mut B, v: u64) {
    let len = varint_len(v);
    if len == 9 {
        buf.put_u8(0xff);
        buf.put_u64(v);
        return;
    }
    // (len-1) leading one bits followed by a zero bit.
    let prefix: u8 = !(0xffu8 >> (len - 1));
    let be = v.to_be_bytes();
    let mut out = [0u8; 8];
    out[8 - len..].copy_from_slice(&be[8 - len..]);
    out[8 - len] |= prefix;
    buf.put_slice(&out[8 - len..]);
}

pub fn get_varint<B: Buf>(buf: &mut B) -> Result<u64> {
    if !buf.has_remaining() {
        return Err(DecodeError::Short);
    }
    let first = buf.chunk()[0];
    let len = (first.leading_ones() as usize + 1).min(9);
    if buf.remaining() < len {
        return Err(DecodeError::Short);
    }
    buf.advance(1);
    if len == 9 {
        return Ok(buf.get_u64());
    }
    let mut v = (first as u64) & (0xff >> len);
    for _ in 1..len {
        v = (v << 8) | buf.get_u8() as u64;
    }
    Ok(v)
}

pub fn get_u8<B: Buf>(buf: &mut B) -> Result<u8> {
    if !buf.has_remaining() {
        return Err(DecodeError::Short);
    }
    Ok(buf.get_u8())
}

pub fn get_u16<B: Buf>(buf: &mut B) -> Result<u16> {
    if buf.remaining() < 2 {
        return Err(DecodeError::Short);
    }
    Ok(buf.get_u16())
}

pub fn get_bytes<B: Buf>(buf: &mut B, len: usize) -> Result<Bytes> {
    if buf.remaining() < len {
        return Err(DecodeError::Short);
    }
    Ok(buf.copy_to_bytes(len))
}

/// Length-prefixed (vi64) byte string.
pub fn get_lp_bytes<B: Buf>(buf: &mut B) -> Result<Bytes> {
    let len = get_varint(buf)? as usize;
    get_bytes(buf, len)
}

pub fn put_lp_bytes<B: BufMut>(buf: &mut B, data: &[u8]) {
    put_varint(buf, data.len() as u64);
    buf.put_slice(data);
}

/// Reason Phrase (§8.5): length-prefixed UTF-8, max 1024 bytes.
pub fn get_reason<B: Buf>(buf: &mut B) -> Result<String> {
    let len = get_varint(buf)? as usize;
    if len > 1024 {
        return Err(DecodeError::ProtocolViolation("reason phrase too long"));
    }
    let raw = get_bytes(buf, len)?;
    Ok(String::from_utf8_lossy(&raw).into_owned())
}

pub fn put_reason<B: BufMut>(buf: &mut B, reason: &str) {
    let raw = reason.as_bytes();
    put_lp_bytes(buf, &raw[..raw.len().min(1024)]);
}

#[cfg(test)]
mod tests {
    use super::*;

    fn dec(hex: &[u8]) -> u64 {
        let mut b = hex;
        let v = get_varint(&mut b).unwrap();
        assert!(b.is_empty());
        v
    }

    #[test]
    fn spec_vectors() {
        // Table 4 of draft-ietf-moq-transport-22.
        assert_eq!(dec(&[0x25]), 37);
        assert_eq!(dec(&[0x80, 0x25]), 37);
        assert_eq!(dec(&[0xbb, 0xbd]), 15_293);
        assert_eq!(dec(&[0xed, 0x7f, 0x3e, 0x7d]), 226_442_877);
        assert_eq!(dec(&[0xfa, 0xa1, 0xa0, 0xe4, 0x03, 0xd8]), 2_893_212_287_960);
        assert_eq!(dec(&[0xfc, 0x89, 0x98, 0xab, 0xc6, 0x6b, 0xc0]), 151_288_809_941_952);
        assert_eq!(dec(&[0xfe, 0xfa, 0x31, 0x8f, 0xa8, 0xe3, 0xca, 0x11]), 70_423_237_261_249_041);
        assert_eq!(dec(&[0xff; 9]), u64::MAX);
    }

    #[test]
    fn roundtrip_boundaries() {
        let mut vals = vec![0u64, u64::MAX];
        for bits in [7, 14, 21, 28, 35, 42, 49, 56] {
            let edge = (1u64 << bits) - 1;
            vals.extend([edge, edge + 1]);
        }
        for v in vals {
            let mut out = Vec::new();
            put_varint(&mut out, v);
            assert_eq!(out.len(), varint_len(v), "len for {v}");
            assert_eq!(dec(&out), v);
        }
    }

    #[test]
    fn short_reads() {
        let mut b: &[u8] = &[0xbb];
        assert_eq!(get_varint(&mut b), Err(DecodeError::Short));
        assert_eq!(b.len(), 1, "short read must not consume");
    }
}
