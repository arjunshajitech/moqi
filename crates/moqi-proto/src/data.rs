//! Data plane: subgroup streams and object datagrams (§11).

use bytes::{Buf, BufMut, Bytes};

use crate::coding::*;
use crate::types::*;

/// Unidirectional stream types (§6.4.1).
pub mod stream_type {
    pub const FETCH_HEADER: u64 = 0x05;
    pub const SETUP: u64 = 0x2F00;
    pub const PADDING: u64 = 0x132B_3E28;

    /// `0b0XX1XXXX`, single-byte only.
    pub fn is_subgroup_header(ty: u64) -> bool {
        ty < 0x80 && ty & 0x10 != 0
    }
}

/// Object Status (§11.1.1).
pub mod object_status {
    pub const NORMAL: u64 = 0x0;
    pub const END_OF_GROUP: u64 = 0x3;
    pub const END_OF_TRACK: u64 = 0x4;
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SubgroupIdMode {
    /// Subgroup ID is 0, not on the wire.
    Zero,
    /// Subgroup ID equals the first Object ID, not on the wire.
    FirstObject,
    /// Subgroup ID is explicit in the header.
    Explicit(u64),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SubgroupHeader {
    pub track_alias: u64,
    pub group_id: u64,
    pub subgroup: SubgroupIdMode,
    /// `None` => DEFAULT_PRIORITY (inherit from the subscription).
    pub publisher_priority: Option<u8>,
    /// Every object carries an Object Properties block.
    pub has_properties: bool,
    /// This subgroup contains the largest object of the group.
    pub end_of_group: bool,
    /// First object on this stream is the first one in the subgroup.
    pub first_object: bool,
}

const SG_PROPERTIES: u64 = 0x01;
const SG_ID_MODE_MASK: u64 = 0x06;
const SG_END_OF_GROUP: u64 = 0x08;
const SG_MARKER: u64 = 0x10;
const SG_DEFAULT_PRIORITY: u64 = 0x20;
const SG_FIRST_OBJECT: u64 = 0x40;

impl SubgroupHeader {
    pub fn type_flags(&self) -> u64 {
        let mut f = SG_MARKER;
        if self.has_properties {
            f |= SG_PROPERTIES;
        }
        f |= match self.subgroup {
            SubgroupIdMode::Zero => 0b00,
            SubgroupIdMode::FirstObject => 0b01,
            SubgroupIdMode::Explicit(_) => 0b10,
        } << 1;
        if self.end_of_group {
            f |= SG_END_OF_GROUP;
        }
        if self.publisher_priority.is_none() {
            f |= SG_DEFAULT_PRIORITY;
        }
        if self.first_object {
            f |= SG_FIRST_OBJECT;
        }
        f
    }

    /// Decode the header *after* the stream type (= type flags) has been read.
    pub fn decode_with_flags<B: Buf>(flags: u64, buf: &mut B) -> Result<Self> {
        if !stream_type::is_subgroup_header(flags) {
            return Err(DecodeError::ProtocolViolation("invalid subgroup header flags"));
        }
        let track_alias = get_varint(buf)?;
        let group_id = get_varint(buf)?;
        let subgroup = match (flags & SG_ID_MODE_MASK) >> 1 {
            0b00 => SubgroupIdMode::Zero,
            0b01 => SubgroupIdMode::FirstObject,
            0b10 => SubgroupIdMode::Explicit(get_varint(buf)?),
            _ => return Err(DecodeError::ProtocolViolation("reserved subgroup id mode")),
        };
        let publisher_priority = if flags & SG_DEFAULT_PRIORITY != 0 { None } else { Some(get_u8(buf)?) };
        Ok(Self {
            track_alias,
            group_id,
            subgroup,
            publisher_priority,
            has_properties: flags & SG_PROPERTIES != 0,
            end_of_group: flags & SG_END_OF_GROUP != 0,
            first_object: flags & SG_FIRST_OBJECT != 0,
        })
    }
}

impl Encode for SubgroupHeader {
    fn encode<B: BufMut>(&self, buf: &mut B) {
        put_varint(buf, self.type_flags());
        put_varint(buf, self.track_alias);
        put_varint(buf, self.group_id);
        if let SubgroupIdMode::Explicit(id) = self.subgroup {
            put_varint(buf, id);
        }
        if let Some(p) = self.publisher_priority {
            buf.put_u8(p);
        }
    }
}

impl Decode for SubgroupHeader {
    fn decode<B: Buf>(buf: &mut B) -> Result<Self> {
        let flags = get_varint(buf)?;
        Self::decode_with_flags(flags, buf)
    }
}

/// An object as carried on a subgroup stream or datagram.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Object {
    pub id: u64,
    pub status: u64,
    pub properties: Properties,
    pub payload: Bytes,
}

impl Object {
    pub fn normal(id: u64, properties: Properties, payload: Bytes) -> Self {
        Self { id, status: object_status::NORMAL, properties, payload }
    }
}

/// Stateful (de)serializer for the objects following a SUBGROUP_HEADER,
/// tracking the Object ID delta encoding (§11.3.1).
#[derive(Debug, Clone)]
pub struct SubgroupObjectCodec {
    has_properties: bool,
    prev_id: Option<u64>,
}

impl SubgroupObjectCodec {
    pub fn new(header: &SubgroupHeader) -> Self {
        Self { has_properties: header.has_properties, prev_id: None }
    }

    pub fn encode<B: BufMut>(&mut self, obj: &Object, buf: &mut B) {
        let delta = match self.prev_id {
            None => obj.id,
            Some(prev) => {
                assert!(obj.id > prev, "object ids must increase within a subgroup");
                obj.id - prev - 1
            }
        };
        self.prev_id = Some(obj.id);
        put_varint(buf, delta);
        if self.has_properties {
            obj.properties.encode_prefixed(buf);
        }
        put_varint(buf, obj.payload.len() as u64);
        if obj.payload.is_empty() {
            put_varint(buf, obj.status);
        } else {
            buf.put_slice(&obj.payload);
        }
    }

    /// Decode one object. On `Short`, nothing is consumed and codec state is unchanged.
    pub fn decode(&mut self, buf: &mut impl Buf) -> Result<Object> {
        let chunk = buf.chunk();
        let mut b = chunk;
        let delta = get_varint(&mut b)?;
        let id = match self.prev_id {
            None => delta,
            Some(prev) => prev
                .checked_add(delta)
                .and_then(|v| v.checked_add(1))
                .ok_or(DecodeError::ProtocolViolation("object id overflow"))?,
        };
        let properties =
            if self.has_properties { Properties::decode_prefixed(&mut b)? } else { Properties::default() };
        let len = get_varint(&mut b)? as usize;
        let (status, payload_len) = if len == 0 { (get_varint(&mut b)?, 0) } else { (object_status::NORMAL, len) };
        if b.len() < payload_len {
            return Err(DecodeError::Short);
        }
        if status != object_status::NORMAL && !properties.is_empty() {
            return Err(DecodeError::ProtocolViolation("properties on non-normal object"));
        }
        if !matches!(status, object_status::NORMAL | object_status::END_OF_GROUP | object_status::END_OF_TRACK) {
            return Err(DecodeError::ProtocolViolation("invalid object status"));
        }
        let header_len = chunk.len() - b.len();
        buf.advance(header_len);
        let payload = buf.copy_to_bytes(payload_len);
        self.prev_id = Some(id);
        Ok(Object { id, status, properties, payload })
    }
}

// ---------------------------------------------------------------------------
// OBJECT_DATAGRAM (§11.2.1)
// ---------------------------------------------------------------------------

const DG_PROPERTIES: u64 = 0x01;
const DG_END_OF_GROUP: u64 = 0x02;
const DG_ZERO_OBJECT_ID: u64 = 0x04;
const DG_DEFAULT_PRIORITY: u64 = 0x08;
const DG_RESERVED: u64 = 0x10;
const DG_STATUS: u64 = 0x20;
const DG_KNOWN: u64 = DG_PROPERTIES | DG_END_OF_GROUP | DG_ZERO_OBJECT_ID | DG_DEFAULT_PRIORITY | DG_STATUS;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ObjectDatagram {
    pub track_alias: u64,
    pub group_id: u64,
    pub publisher_priority: Option<u8>,
    pub end_of_group: bool,
    pub object: Object,
}

impl Encode for ObjectDatagram {
    fn encode<B: BufMut>(&self, buf: &mut B) {
        let o = &self.object;
        let status_form = o.payload.is_empty() && o.status != object_status::NORMAL;
        let mut flags = 0;
        if !o.properties.is_empty() {
            flags |= DG_PROPERTIES;
        }
        if self.end_of_group && !status_form {
            flags |= DG_END_OF_GROUP;
        }
        if o.id == 0 {
            flags |= DG_ZERO_OBJECT_ID;
        }
        if self.publisher_priority.is_none() {
            flags |= DG_DEFAULT_PRIORITY;
        }
        if status_form {
            flags |= DG_STATUS;
        }
        put_varint(buf, flags);
        put_varint(buf, self.track_alias);
        put_varint(buf, self.group_id);
        if o.id != 0 {
            put_varint(buf, o.id);
        }
        if let Some(p) = self.publisher_priority {
            buf.put_u8(p);
        }
        if !o.properties.is_empty() {
            o.properties.encode_prefixed(buf);
        }
        if status_form {
            put_varint(buf, o.status);
        } else {
            buf.put_slice(&o.payload);
        }
    }
}

impl Decode for ObjectDatagram {
    fn decode<B: Buf>(buf: &mut B) -> Result<Self> {
        let flags = get_varint(buf)?;
        if flags & DG_RESERVED != 0 || flags & !DG_KNOWN != 0 {
            return Err(DecodeError::ProtocolViolation("invalid datagram flags"));
        }
        if flags & DG_STATUS != 0 && flags & DG_END_OF_GROUP != 0 {
            return Err(DecodeError::ProtocolViolation("datagram STATUS with END_OF_GROUP"));
        }
        let track_alias = get_varint(buf)?;
        let group_id = get_varint(buf)?;
        let id = if flags & DG_ZERO_OBJECT_ID != 0 { 0 } else { get_varint(buf)? };
        let publisher_priority = if flags & DG_DEFAULT_PRIORITY != 0 { None } else { Some(get_u8(buf)?) };
        let properties = if flags & DG_PROPERTIES != 0 {
            let p = Properties::decode_prefixed(buf)?;
            if p.is_empty() {
                return Err(DecodeError::ProtocolViolation("empty datagram properties"));
            }
            p
        } else {
            Properties::default()
        };
        let (status, payload) = if flags & DG_STATUS != 0 {
            let status = get_varint(buf)?;
            if status != object_status::NORMAL && !properties.is_empty() {
                return Err(DecodeError::ProtocolViolation("properties on non-normal object"));
            }
            (status, Bytes::new())
        } else {
            (object_status::NORMAL, buf.copy_to_bytes(buf.remaining()))
        };
        Ok(Self {
            track_alias,
            group_id,
            publisher_priority,
            end_of_group: flags & DG_END_OF_GROUP != 0,
            object: Object { id, status, properties, payload },
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn subgroup_stream_roundtrip() {
        let header = SubgroupHeader {
            track_alias: 3,
            group_id: 42,
            subgroup: SubgroupIdMode::Zero,
            publisher_priority: Some(7),
            has_properties: true,
            end_of_group: true,
            first_object: true,
        };
        let mut props = Properties::default();
        props.set_varint(property::TIMESTAMP, 99);
        let objs = vec![
            Object::normal(0, props.clone(), Bytes::from_static(b"key")),
            Object::normal(1, Properties::default(), Bytes::from_static(b"delta")),
            Object::normal(5, props, Bytes::from_static(b"gap")),
            Object { id: 6, status: object_status::END_OF_GROUP, properties: Properties::default(), payload: Bytes::new() },
        ];
        let mut wire = header.encode_to_vec();
        let mut enc = SubgroupObjectCodec::new(&header);
        for o in &objs {
            enc.encode(o, &mut wire);
        }

        let mut b = wire.as_slice();
        let h = SubgroupHeader::decode(&mut b).unwrap();
        assert_eq!(h, header);
        let mut dec = SubgroupObjectCodec::new(&h);
        for o in &objs {
            assert_eq!(&dec.decode(&mut b).unwrap(), o);
        }
        assert!(b.is_empty());
    }

    #[test]
    fn subgroup_object_short_is_non_consuming() {
        let header = SubgroupHeader {
            track_alias: 1,
            group_id: 0,
            subgroup: SubgroupIdMode::Explicit(9),
            publisher_priority: None,
            has_properties: false,
            end_of_group: false,
            first_object: false,
        };
        let mut wire = Vec::new();
        SubgroupObjectCodec::new(&header).encode(&Object::normal(0, Properties::default(), Bytes::from_static(b"abcdef")), &mut wire);
        let mut dec = SubgroupObjectCodec::new(&header);
        for i in 0..wire.len() {
            let mut p = &wire[..i];
            assert_eq!(dec.decode(&mut p), Err(DecodeError::Short));
            assert_eq!(p.len(), i);
        }
    }

    #[test]
    fn datagram_roundtrip() {
        let mut props = Properties::default();
        props.set_varint(property::TIMESTAMP, 1_000_000);
        for (id, prio) in [(0, None), (4, Some(3u8))] {
            let d = ObjectDatagram {
                track_alias: 9,
                group_id: 1234,
                publisher_priority: prio,
                end_of_group: true,
                object: Object::normal(id, props.clone(), Bytes::from_static(b"opus")),
            };
            let enc = d.encode_to_vec();
            assert_eq!(ObjectDatagram::decode(&mut enc.as_slice()).unwrap(), d);
        }
    }
}
