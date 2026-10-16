//! Common structures (§8): Location, Track Namespace, Key-Value-Pairs
//! (Properties) and Message Parameters (§9.20).

use std::fmt;

use bytes::{Buf, BufMut, Bytes};

use crate::coding::*;

// ---------------------------------------------------------------------------
// Location (§8.2)
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Location {
    pub group: u64,
    pub object: u64,
}

impl Location {
    pub const fn new(group: u64, object: u64) -> Self {
        Self { group, object }
    }
}

impl Encode for Location {
    fn encode<B: BufMut>(&self, buf: &mut B) {
        put_varint(buf, self.group);
        put_varint(buf, self.object);
    }
}

impl Decode for Location {
    fn decode<B: Buf>(buf: &mut B) -> Result<Self> {
        Ok(Self { group: get_varint(buf)?, object: get_varint(buf)? })
    }
}

// ---------------------------------------------------------------------------
// Track Namespace (§8.7)
// ---------------------------------------------------------------------------

pub const MAX_NAMESPACE_FIELDS: usize = 32;
pub const MAX_FULL_TRACK_NAME: usize = 4096;

#[derive(Clone, Default, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct TrackNamespace(pub Vec<Bytes>);

impl TrackNamespace {
    pub fn new<I, S>(fields: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<Bytes>,
    {
        Self(fields.into_iter().map(Into::into).collect())
    }

    pub fn len(&self) -> usize {
        self.0.len()
    }

    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    pub fn byte_len(&self) -> usize {
        self.0.iter().map(Bytes::len).sum()
    }

    /// Namespace prefix match (§2.4.2): matches field-by-field, whole fields only.
    pub fn has_prefix(&self, prefix: &TrackNamespace) -> bool {
        prefix.0.len() <= self.0.len() && self.0.iter().zip(&prefix.0).all(|(a, b)| a == b)
    }

    pub fn suffix_after(&self, prefix: &TrackNamespace) -> TrackNamespace {
        TrackNamespace(self.0[prefix.0.len().min(self.0.len())..].to_vec())
    }

    pub fn truncated(&self, n: usize) -> TrackNamespace {
        TrackNamespace(self.0[..n.min(self.0.len())].to_vec())
    }
}

/// Renders using the recommended format from §8.8.
fn render_field(f: &mut fmt::Formatter<'_>, field: &[u8]) -> fmt::Result {
    for &b in field {
        if b.is_ascii_alphanumeric() || b == b'_' {
            write!(f, "{}", b as char)?;
        } else {
            write!(f, ".{b:02x}")?;
        }
    }
    Ok(())
}

impl fmt::Display for TrackNamespace {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        for (i, field) in self.0.iter().enumerate() {
            if i > 0 {
                f.write_str("-")?;
            }
            render_field(f, field)?;
        }
        Ok(())
    }
}

impl fmt::Debug for TrackNamespace {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "TrackNamespace({self})")
    }
}

impl Encode for TrackNamespace {
    fn encode<B: BufMut>(&self, buf: &mut B) {
        put_varint(buf, self.0.len() as u64);
        for field in &self.0 {
            put_lp_bytes(buf, field);
        }
    }
}

impl Decode for TrackNamespace {
    fn decode<B: Buf>(buf: &mut B) -> Result<Self> {
        let count = get_varint(buf)? as usize;
        if count > MAX_NAMESPACE_FIELDS {
            return Err(DecodeError::ProtocolViolation("too many namespace fields"));
        }
        let mut fields = Vec::with_capacity(count);
        let mut total = 0;
        for _ in 0..count {
            let field = get_lp_bytes(buf)?;
            if field.is_empty() {
                return Err(DecodeError::ProtocolViolation("empty namespace field"));
            }
            total += field.len();
            if total > MAX_FULL_TRACK_NAME {
                return Err(DecodeError::ProtocolViolation("namespace too long"));
            }
            fields.push(field);
        }
        Ok(Self(fields))
    }
}

/// Track Namespace + Track Name.
#[derive(Clone, PartialEq, Eq, Hash)]
pub struct FullTrackName {
    pub namespace: TrackNamespace,
    pub name: Bytes,
}

impl FullTrackName {
    pub fn new(namespace: TrackNamespace, name: impl Into<Bytes>) -> Self {
        Self { namespace, name: name.into() }
    }

    pub(crate) fn decode_parts<B: Buf>(buf: &mut B) -> Result<Self> {
        let namespace = TrackNamespace::decode(buf)?;
        let name = get_lp_bytes(buf)?;
        if namespace.byte_len() + name.len() > MAX_FULL_TRACK_NAME {
            return Err(DecodeError::ProtocolViolation("full track name too long"));
        }
        Ok(Self { namespace, name })
    }

    pub(crate) fn encode_parts<B: BufMut>(&self, buf: &mut B) {
        self.namespace.encode(buf);
        put_lp_bytes(buf, &self.name);
    }
}

impl fmt::Display for FullTrackName {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}--", self.namespace)?;
        render_field(f, &self.name)
    }
}

impl fmt::Debug for FullTrackName {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "FullTrackName({self})")
    }
}

// ---------------------------------------------------------------------------
// Key-Value-Pairs / Properties (§8.3, §8.4)
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum KvValue {
    /// Even types carry a single varint.
    Varint(u64),
    /// Odd types carry length-prefixed bytes (max 65535).
    Bytes(Bytes),
}

/// A sequence of Key-Value-Pairs, kept in ascending type order (delta-encoded).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Properties(pub Vec<(u64, KvValue)>);

impl Properties {
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    pub fn get_varint(&self, ty: u64) -> Option<u64> {
        self.0.iter().find_map(|(t, v)| match v {
            KvValue::Varint(x) if *t == ty => Some(*x),
            _ => None,
        })
    }

    pub fn get_bytes(&self, ty: u64) -> Option<&Bytes> {
        self.0.iter().find_map(|(t, v)| match v {
            KvValue::Bytes(x) if *t == ty => Some(x),
            _ => None,
        })
    }

    pub fn set_varint(&mut self, ty: u64, v: u64) {
        debug_assert!(ty.is_multiple_of(2), "varint properties use even types");
        self.0.retain(|(t, _)| *t != ty);
        self.0.push((ty, KvValue::Varint(v)));
        self.0.sort_by_key(|(t, _)| *t);
    }

    /// Decode KVPs until `buf` is exhausted (the caller bounds the length).
    pub fn decode_all<B: Buf>(buf: &mut B) -> Result<Self> {
        let mut out = Vec::new();
        let mut ty: u64 = 0;
        while buf.has_remaining() {
            let delta = get_varint(buf).map_err(short_is_format_error)?;
            ty = ty.checked_add(delta).ok_or(DecodeError::ProtocolViolation("kvp type overflow"))?;
            let value = if ty.is_multiple_of(2) {
                KvValue::Varint(get_varint(buf).map_err(short_is_format_error)?)
            } else {
                let len = get_varint(buf).map_err(short_is_format_error)? as usize;
                if len > u16::MAX as usize {
                    return Err(DecodeError::ProtocolViolation("kvp value too long"));
                }
                KvValue::Bytes(get_bytes(buf, len).map_err(short_is_format_error)?)
            };
            out.push((ty, value));
        }
        Ok(Self(out))
    }

    /// Decode a length-prefixed property block (Object Properties, §11.1.2).
    pub fn decode_prefixed<B: Buf>(buf: &mut B) -> Result<Self> {
        let raw = get_lp_bytes(buf)?;
        Self::decode_all(&mut raw.clone())
    }

    pub fn encoded_len(&self) -> usize {
        let mut v = Vec::new();
        self.encode(&mut v);
        v.len()
    }

    pub fn encode_prefixed<B: BufMut>(&self, buf: &mut B) {
        let mut v = Vec::new();
        self.encode(&mut v);
        put_lp_bytes(buf, &v);
    }
}

fn short_is_format_error(e: DecodeError) -> DecodeError {
    // KVPs are always parsed with a known length; running out is malformed.
    match e {
        DecodeError::Short => DecodeError::KeyValueFormatting,
        e => e,
    }
}

impl Encode for Properties {
    fn encode<B: BufMut>(&self, buf: &mut B) {
        let mut sorted: Vec<_> = self.0.iter().collect();
        sorted.sort_by_key(|(t, _)| *t);
        let mut prev = 0;
        for (ty, value) in sorted {
            put_varint(buf, ty - prev);
            prev = *ty;
            match value {
                KvValue::Varint(v) => put_varint(buf, *v),
                KvValue::Bytes(b) => put_lp_bytes(buf, b),
            }
        }
    }
}

/// Well-known property types (§10, §16.8 incl. draft-ietf-moq-loc provisional entries).
pub mod property {
    pub const OBJECT_DELIVERY_TIMEOUT: u64 = 0x02;
    pub const MAX_CACHE_DURATION: u64 = 0x04;
    pub const SUBGROUP_DELIVERY_TIMEOUT: u64 = 0x06;
    pub const DEFAULT_PUBLISHER_PRIORITY: u64 = 0x0E;
    pub const DEFAULT_PUBLISHER_GROUP_ORDER: u64 = 0x22;
    pub const DYNAMIC_GROUPS: u64 = 0x30;
    /// draft-ietf-moq-loc: capture timestamp (microseconds).
    pub const TIMESTAMP: u64 = 0x10;
}

// ---------------------------------------------------------------------------
// Message Parameters (§9.20)
// ---------------------------------------------------------------------------

pub mod param {
    pub const OBJECT_DELIVERY_TIMEOUT: u64 = 0x02;
    pub const AUTHORIZATION_TOKEN: u64 = 0x03;
    pub const RENDEZVOUS_TIMEOUT: u64 = 0x04;
    pub const SUBGROUP_DELIVERY_TIMEOUT: u64 = 0x06;
    pub const EXPIRES: u64 = 0x08;
    pub const LARGEST_OBJECT: u64 = 0x09;
    pub const FILL_TIMEOUT: u64 = 0x0A;
    pub const FORWARD: u64 = 0x10;
    pub const SUBSCRIBER_PRIORITY: u64 = 0x20;
    pub const LOCATION_FILTER: u64 = 0x21;
    pub const GROUP_ORDER: u64 = 0x22;
    pub const FILL_PARAMETERS: u64 = 0x23;
    pub const SUBGROUP_FILTER: u64 = 0x25;
    pub const OBJECTID_FILTER: u64 = 0x26;
    pub const PRIORITY_FILTER: u64 = 0x27;
    pub const OBJECT_PROPERTY_FILTER: u64 = 0x28;
    pub const TRACK_PROPERTY_FILTER: u64 = 0x29;
    pub const NEW_GROUP_REQUEST: u64 = 0x32;
    pub const TRACK_NAMESPACE_PREFIX: u64 = 0x34;
    pub const INCLUDE_PROPERTIES: u64 = 0x35;
}

/// Location Filter (§3.3.1, §9.20.9).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LocationFilter {
    None,
    RelativeStart { start_group: u64 },
    AbsoluteStart { start: Location },
    AbsoluteStartGroupEnd { start: Location, end_group_delta: u64 },
    AbsoluteRange { start: Location, end_group_delta: u64, end_object: u64 },
    NextObject,
}

impl Encode for LocationFilter {
    fn encode<B: BufMut>(&self, buf: &mut B) {
        match *self {
            Self::None => put_varint(buf, 0x00),
            Self::RelativeStart { start_group } => {
                put_varint(buf, 0x01);
                put_varint(buf, start_group);
            }
            Self::AbsoluteStart { start } => {
                put_varint(buf, 0x02);
                start.encode(buf);
            }
            Self::AbsoluteStartGroupEnd { start, end_group_delta } => {
                put_varint(buf, 0x03);
                start.encode(buf);
                put_varint(buf, end_group_delta);
            }
            Self::AbsoluteRange { start, end_group_delta, end_object } => {
                put_varint(buf, 0x04);
                start.encode(buf);
                put_varint(buf, end_group_delta);
                put_varint(buf, end_object);
            }
            Self::NextObject => put_varint(buf, 0x05),
        }
    }
}

impl Decode for LocationFilter {
    fn decode<B: Buf>(buf: &mut B) -> Result<Self> {
        Ok(match get_varint(buf)? {
            0x00 => Self::None,
            0x01 => Self::RelativeStart { start_group: get_varint(buf)? },
            0x02 => Self::AbsoluteStart { start: Location::decode(buf)? },
            0x03 => {
                let start = Location::decode(buf)?;
                let end_group_delta = get_varint(buf)?;
                start
                    .group
                    .checked_add(end_group_delta)
                    .ok_or(DecodeError::ProtocolViolation("end group overflow"))?;
                Self::AbsoluteStartGroupEnd { start, end_group_delta }
            }
            0x04 => {
                let start = Location::decode(buf)?;
                let end_group_delta = get_varint(buf)?;
                start
                    .group
                    .checked_add(end_group_delta)
                    .ok_or(DecodeError::ProtocolViolation("end group overflow"))?;
                Self::AbsoluteRange { start, end_group_delta, end_object: get_varint(buf)? }
            }
            0x05 => Self::NextObject,
            _ => return Err(DecodeError::ProtocolViolation("invalid location filter type")),
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ParamValue {
    U8(u8),
    Varint(u64),
    Location(Location),
    /// Length-prefixed bytes (auth token, range filters, fill parameters).
    Bytes(Bytes),
    LocationFilter(LocationFilter),
    Namespace(TrackNamespace),
}

#[derive(Clone, Copy)]
enum ParamKind {
    U8,
    Varint,
    Location,
    Bytes,
    LocationFilter,
    Namespace,
}

fn param_kind(ty: u64) -> Option<ParamKind> {
    use param::*;
    Some(match ty {
        OBJECT_DELIVERY_TIMEOUT | RENDEZVOUS_TIMEOUT | SUBGROUP_DELIVERY_TIMEOUT | EXPIRES
        | FILL_TIMEOUT | NEW_GROUP_REQUEST => ParamKind::Varint,
        FORWARD | SUBSCRIBER_PRIORITY | GROUP_ORDER | INCLUDE_PROPERTIES => ParamKind::U8,
        LARGEST_OBJECT => ParamKind::Location,
        LOCATION_FILTER => ParamKind::LocationFilter,
        TRACK_NAMESPACE_PREFIX => ParamKind::Namespace,
        AUTHORIZATION_TOKEN | FILL_PARAMETERS | SUBGROUP_FILTER | OBJECTID_FILTER
        | PRIORITY_FILTER | OBJECT_PROPERTY_FILTER | TRACK_PROPERTY_FILTER => ParamKind::Bytes,
        _ => return None,
    })
}

/// Message Parameters: count-prefixed, delta-typed, ascending.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Parameters(pub Vec<(u64, ParamValue)>);

impl Parameters {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with(mut self, ty: u64, v: ParamValue) -> Self {
        self.set(ty, v);
        self
    }

    pub fn set(&mut self, ty: u64, v: ParamValue) {
        self.0.retain(|(t, _)| *t != ty);
        self.0.push((ty, v));
        self.0.sort_by_key(|(t, _)| *t);
    }

    pub fn get(&self, ty: u64) -> Option<&ParamValue> {
        self.0.iter().find(|(t, _)| *t == ty).map(|(_, v)| v)
    }

    pub fn get_u8(&self, ty: u64) -> Option<u8> {
        match self.get(ty) {
            Some(ParamValue::U8(v)) => Some(*v),
            _ => None,
        }
    }

    pub fn get_varint(&self, ty: u64) -> Option<u64> {
        match self.get(ty) {
            Some(ParamValue::Varint(v)) => Some(*v),
            _ => None,
        }
    }

    pub fn location_filter(&self) -> Option<LocationFilter> {
        match self.get(param::LOCATION_FILTER) {
            Some(ParamValue::LocationFilter(f)) => Some(*f),
            _ => None,
        }
    }
}

impl Encode for Parameters {
    fn encode<B: BufMut>(&self, buf: &mut B) {
        put_varint(buf, self.0.len() as u64);
        let mut prev = 0;
        for (ty, value) in &self.0 {
            put_varint(buf, ty - prev);
            prev = *ty;
            match value {
                ParamValue::U8(v) => buf.put_u8(*v),
                ParamValue::Varint(v) => put_varint(buf, *v),
                ParamValue::Location(l) => l.encode(buf),
                ParamValue::Bytes(b) => put_lp_bytes(buf, b),
                ParamValue::LocationFilter(f) => f.encode(buf),
                ParamValue::Namespace(ns) => ns.encode(buf),
            }
        }
    }
}

impl Decode for Parameters {
    fn decode<B: Buf>(buf: &mut B) -> Result<Self> {
        let count = get_varint(buf)?;
        let mut out = Vec::new();
        let mut ty: u64 = 0;
        for i in 0..count {
            let delta = get_varint(buf)?;
            if i > 0 && delta == 0 {
                return Err(DecodeError::ProtocolViolation("duplicate parameter"));
            }
            ty = ty.checked_add(delta).ok_or(DecodeError::ProtocolViolation("param type overflow"))?;
            let kind = param_kind(ty).ok_or(DecodeError::UnknownParameter(ty))?;
            let value = match kind {
                ParamKind::U8 => ParamValue::U8(get_u8(buf)?),
                ParamKind::Varint => ParamValue::Varint(get_varint(buf)?),
                ParamKind::Location => ParamValue::Location(Location::decode(buf)?),
                ParamKind::Bytes => ParamValue::Bytes(get_lp_bytes(buf)?),
                ParamKind::LocationFilter => ParamValue::LocationFilter(LocationFilter::decode(buf)?),
                ParamKind::Namespace => ParamValue::Namespace(TrackNamespace::decode(buf)?),
            };
            out.push((ty, value));
        }
        Ok(Self(out))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn namespace_roundtrip_and_display() {
        let ns = TrackNamespace::new(["example.net", "team2", "project_x"]);
        let enc = ns.encode_to_vec();
        assert_eq!(TrackNamespace::decode(&mut enc.as_slice()).unwrap(), ns);
        let full = FullTrackName::new(ns, "report");
        assert_eq!(full.to_string(), "example.2enet-team2-project_x--report");
    }

    #[test]
    fn namespace_prefix() {
        let ns = TrackNamespace::new(["a", "b", "c"]);
        assert!(ns.has_prefix(&TrackNamespace::new(["a", "b"])));
        assert!(!ns.has_prefix(&TrackNamespace::new(["a", "bc"])));
        assert_eq!(ns.suffix_after(&TrackNamespace::new(["a"])), TrackNamespace::new(["b", "c"]));
    }

    #[test]
    fn params_roundtrip() {
        let p = Parameters::new()
            .with(param::LOCATION_FILTER, ParamValue::LocationFilter(LocationFilter::RelativeStart { start_group: 1 }))
            .with(param::FORWARD, ParamValue::U8(1))
            .with(param::LARGEST_OBJECT, ParamValue::Location(Location::new(7, 3)))
            .with(param::AUTHORIZATION_TOKEN, ParamValue::Bytes(Bytes::from_static(b"tok")));
        let enc = p.encode_to_vec();
        let d = Parameters::decode(&mut enc.as_slice()).unwrap();
        assert_eq!(d, p);
        assert_eq!(d.get_u8(param::FORWARD), Some(1));
    }

    #[test]
    fn unknown_param_rejected() {
        let mut v = Vec::new();
        put_varint(&mut v, 1);
        put_varint(&mut v, 0x7777);
        put_varint(&mut v, 0);
        assert_eq!(Parameters::decode(&mut v.as_slice()), Err(DecodeError::UnknownParameter(0x7777)));
    }

    #[test]
    fn properties_roundtrip() {
        let mut p = Properties::default();
        p.set_varint(property::TIMESTAMP, 123_456);
        p.0.push((0x0D, KvValue::Bytes(Bytes::from_static(b"cfg"))));
        let mut v = Vec::new();
        p.encode_prefixed(&mut v);
        let d = Properties::decode_prefixed(&mut v.as_slice()).unwrap();
        assert_eq!(d.get_varint(property::TIMESTAMP), Some(123_456));
        assert_eq!(d.get_bytes(0x0D).unwrap().as_ref(), b"cfg");
    }
}
