// Wire codec for MoQ Transport draft-ietf-moq-transport-22 (browser side).
// Mirrors crates/moqi-proto. Integers are JS numbers (exact up to 2^53).

export const ALPN = "moqt-22";

/** Thrown when a decoder needs more bytes. Never consumes input. */
export const SHORT = Symbol("short");

export class ProtocolError extends Error {
  constructor(message, code = 0x3 /* PROTOCOL_VIOLATION */) {
    super(message);
    this.code = code;
  }
}

const enc = new TextEncoder();
const dec = new TextDecoder();
export const utf8 = (s) => enc.encode(s);
export const fromUtf8 = (b) => dec.decode(b);

// ---------------------------------------------------------------------------
// Variable-length integers (§8.1): leading-ones prefix, 1..9 bytes.
// ---------------------------------------------------------------------------

export function varintLen(v) {
  if (v < 2 ** 7) return 1;
  if (v < 2 ** 14) return 2;
  if (v < 2 ** 21) return 3;
  if (v < 2 ** 28) return 4;
  if (v < 2 ** 35) return 5;
  if (v < 2 ** 42) return 6;
  if (v < 2 ** 49) return 7;
  if (v < 2 ** 56) return 8;
  return 9;
}

export class Writer {
  constructor(capacity = 128) {
    this.buf = new Uint8Array(capacity);
    this.len = 0;
  }
  #ensure(n) {
    if (this.len + n <= this.buf.length) return;
    let cap = this.buf.length * 2;
    while (cap < this.len + n) cap *= 2;
    const next = new Uint8Array(cap);
    next.set(this.buf.subarray(0, this.len));
    this.buf = next;
  }
  u8(v) {
    this.#ensure(1);
    this.buf[this.len++] = v & 0xff;
    return this;
  }
  u16(v) {
    this.#ensure(2);
    this.buf[this.len++] = (v >> 8) & 0xff;
    this.buf[this.len++] = v & 0xff;
    return this;
  }
  varint(v) {
    if (!Number.isInteger(v) || v < 0) throw new RangeError(`invalid varint ${v}`);
    const len = varintLen(v);
    this.#ensure(len);
    if (len === 9) {
      this.buf[this.len++] = 0xff;
      let big = BigInt(v);
      for (let i = 7; i >= 0; i--) {
        this.buf[this.len + i] = Number(big & 0xffn);
        big >>= 8n;
      }
      this.len += 8;
      return this;
    }
    let x = v;
    for (let i = len - 1; i >= 0; i--) {
      this.buf[this.len + i] = x % 256;
      x = Math.floor(x / 256);
    }
    this.buf[this.len] |= (0xff << (8 - (len - 1))) & 0xff;
    this.len += len;
    return this;
  }
  bytes(b) {
    this.#ensure(b.length);
    this.buf.set(b, this.len);
    this.len += b.length;
    return this;
  }
  lpBytes(b) {
    return this.varint(b.length).bytes(b);
  }
  string(s) {
    return this.lpBytes(utf8(s));
  }
  finish() {
    return this.buf.slice(0, this.len);
  }
}

/** Synchronous reader over a byte array. Throws SHORT on underflow. */
export class Reader {
  constructor(bytes, offset = 0, end = bytes.length) {
    this.bytes = bytes;
    this.pos = offset;
    this.end = end;
  }
  get remaining() {
    return this.end - this.pos;
  }
  u8() {
    if (this.remaining < 1) throw SHORT;
    return this.bytes[this.pos++];
  }
  u16() {
    if (this.remaining < 2) throw SHORT;
    const v = (this.bytes[this.pos] << 8) | this.bytes[this.pos + 1];
    this.pos += 2;
    return v;
  }
  varint() {
    if (this.remaining < 1) throw SHORT;
    const first = this.bytes[this.pos];
    let ones = 0;
    while (ones < 8 && first & (0x80 >> ones)) ones++;
    const len = Math.min(ones + 1, 9);
    if (this.remaining < len) throw SHORT;
    this.pos++;
    if (len === 9) {
      let big = 0n;
      for (let i = 0; i < 8; i++) big = (big << 8n) | BigInt(this.bytes[this.pos++]);
      if (big > BigInt(Number.MAX_SAFE_INTEGER)) throw new ProtocolError("varint exceeds 2^53");
      return Number(big);
    }
    let v = first & (0xff >> len);
    for (let i = 1; i < len; i++) v = v * 256 + this.bytes[this.pos++];
    return v;
  }
  bytesN(n) {
    if (this.remaining < n) throw SHORT;
    const out = this.bytes.subarray(this.pos, this.pos + n);
    this.pos += n;
    return out;
  }
  lpBytes() {
    return this.bytesN(this.varint());
  }
  rest() {
    return this.bytesN(this.remaining);
  }
}

// ---------------------------------------------------------------------------
// Track Namespace (§8.7). Represented as an array of strings.
// ---------------------------------------------------------------------------

export function writeNamespace(w, ns) {
  w.varint(ns.length);
  for (const f of ns) w.lpBytes(typeof f === "string" ? utf8(f) : f);
}

export function readNamespace(r) {
  const n = r.varint();
  if (n > 32) throw new ProtocolError("too many namespace fields");
  const out = [];
  for (let i = 0; i < n; i++) {
    const f = r.lpBytes();
    if (f.length === 0) throw new ProtocolError("empty namespace field");
    out.push(fromUtf8(f));
  }
  return out;
}

export const nsKey = (ns, name = "") => JSON.stringify([ns, name]);
export const hasPrefix = (ns, prefix) => prefix.length <= ns.length && prefix.every((f, i) => ns[i] === f);

// ---------------------------------------------------------------------------
// Key-Value-Pairs / Properties (§8.3). Map<type, number | Uint8Array>.
// ---------------------------------------------------------------------------

export const Prop = {
  TIMESTAMP: 0x10, // draft-ietf-moq-loc, microseconds
};

export function writeProperties(w, props) {
  const entries = [...(props ?? new Map())].sort((a, b) => a[0] - b[0]);
  let prev = 0;
  for (const [ty, value] of entries) {
    w.varint(ty - prev);
    prev = ty;
    if (ty % 2 === 0) w.varint(value);
    else w.lpBytes(value);
  }
}

export function readProperties(r) {
  const props = new Map();
  let ty = 0;
  try {
    while (r.remaining > 0) {
      ty += r.varint();
      props.set(ty, ty % 2 === 0 ? r.varint() : r.lpBytes());
    }
  } catch (e) {
    if (e === SHORT) throw new ProtocolError("malformed key-value pair", 0x6);
    throw e;
  }
  return props;
}

function writePrefixedProperties(w, props) {
  const inner = new Writer();
  writeProperties(inner, props);
  w.lpBytes(inner.finish());
}

function readPrefixedProperties(r) {
  const raw = r.lpBytes();
  return readProperties(new Reader(raw));
}

// ---------------------------------------------------------------------------
// Message Parameters (§9.20). Map<type, value>; encoding depends on type.
// ---------------------------------------------------------------------------

export const Param = {
  OBJECT_DELIVERY_TIMEOUT: 0x02,
  AUTHORIZATION_TOKEN: 0x03,
  RENDEZVOUS_TIMEOUT: 0x04,
  SUBGROUP_DELIVERY_TIMEOUT: 0x06,
  EXPIRES: 0x08,
  LARGEST_OBJECT: 0x09,
  FILL_TIMEOUT: 0x0a,
  FORWARD: 0x10,
  SUBSCRIBER_PRIORITY: 0x20,
  LOCATION_FILTER: 0x21,
  GROUP_ORDER: 0x22,
  FILL_PARAMETERS: 0x23,
  SUBGROUP_FILTER: 0x25,
  OBJECTID_FILTER: 0x26,
  PRIORITY_FILTER: 0x27,
  OBJECT_PROPERTY_FILTER: 0x28,
  TRACK_PROPERTY_FILTER: 0x29,
  NEW_GROUP_REQUEST: 0x32,
  TRACK_NAMESPACE_PREFIX: 0x34,
  INCLUDE_PROPERTIES: 0x35,
};

const PARAM_KIND = new Map([
  [Param.OBJECT_DELIVERY_TIMEOUT, "varint"],
  [Param.RENDEZVOUS_TIMEOUT, "varint"],
  [Param.SUBGROUP_DELIVERY_TIMEOUT, "varint"],
  [Param.EXPIRES, "varint"],
  [Param.FILL_TIMEOUT, "varint"],
  [Param.NEW_GROUP_REQUEST, "varint"],
  [Param.FORWARD, "u8"],
  [Param.SUBSCRIBER_PRIORITY, "u8"],
  [Param.GROUP_ORDER, "u8"],
  [Param.INCLUDE_PROPERTIES, "u8"],
  [Param.LARGEST_OBJECT, "location"],
  [Param.LOCATION_FILTER, "filter"],
  [Param.TRACK_NAMESPACE_PREFIX, "namespace"],
  [Param.AUTHORIZATION_TOKEN, "bytes"],
  [Param.FILL_PARAMETERS, "bytes"],
  [Param.SUBGROUP_FILTER, "bytes"],
  [Param.OBJECTID_FILTER, "bytes"],
  [Param.PRIORITY_FILTER, "bytes"],
  [Param.OBJECT_PROPERTY_FILTER, "bytes"],
  [Param.TRACK_PROPERTY_FILTER, "bytes"],
]);

/** Location filter: { type, startGroup?, startObject?, endGroupDelta?, endObject? } */
export const Filter = {
  NONE: 0x0,
  RELATIVE_START: 0x1,
  ABSOLUTE_START: 0x2,
  ABSOLUTE_START_GROUP_END: 0x3,
  ABSOLUTE_RANGE: 0x4,
  NEXT_OBJECT: 0x5,
};

function writeFilter(w, f) {
  w.varint(f.type);
  if (f.type === Filter.RELATIVE_START) w.varint(f.startGroup);
  if (f.type >= Filter.ABSOLUTE_START && f.type <= Filter.ABSOLUTE_RANGE) w.varint(f.startGroup).varint(f.startObject);
  if (f.type >= Filter.ABSOLUTE_START_GROUP_END && f.type <= Filter.ABSOLUTE_RANGE) w.varint(f.endGroupDelta);
  if (f.type === Filter.ABSOLUTE_RANGE) w.varint(f.endObject);
}

function readFilter(r) {
  const type = r.varint();
  if (type > Filter.NEXT_OBJECT) throw new ProtocolError("invalid location filter");
  const f = { type };
  if (type === Filter.RELATIVE_START) f.startGroup = r.varint();
  if (type >= Filter.ABSOLUTE_START && type <= Filter.ABSOLUTE_RANGE) {
    f.startGroup = r.varint();
    f.startObject = r.varint();
  }
  if (type >= Filter.ABSOLUTE_START_GROUP_END && type <= Filter.ABSOLUTE_RANGE) f.endGroupDelta = r.varint();
  if (type === Filter.ABSOLUTE_RANGE) f.endObject = r.varint();
  return f;
}

export function writeParams(w, params) {
  const entries = [...(params ?? new Map())].sort((a, b) => a[0] - b[0]);
  w.varint(entries.length);
  let prev = 0;
  for (const [ty, v] of entries) {
    w.varint(ty - prev);
    prev = ty;
    switch (PARAM_KIND.get(ty)) {
      case "varint": w.varint(v); break;
      case "u8": w.u8(v); break;
      case "location": w.varint(v.group).varint(v.object); break;
      case "filter": writeFilter(w, v); break;
      case "namespace": writeNamespace(w, v); break;
      case "bytes": w.lpBytes(v); break;
      default: throw new Error(`unknown parameter ${ty}`);
    }
  }
}

export function readParams(r) {
  const count = r.varint();
  const params = new Map();
  let ty = 0;
  for (let i = 0; i < count; i++) {
    const delta = r.varint();
    if (i > 0 && delta === 0) throw new ProtocolError("duplicate parameter");
    ty += delta;
    switch (PARAM_KIND.get(ty)) {
      case "varint": params.set(ty, r.varint()); break;
      case "u8": params.set(ty, r.u8()); break;
      case "location": params.set(ty, { group: r.varint(), object: r.varint() }); break;
      case "filter": params.set(ty, readFilter(r)); break;
      case "namespace": params.set(ty, readNamespace(r)); break;
      case "bytes": params.set(ty, r.lpBytes()); break;
      default: throw new ProtocolError(`unknown parameter 0x${ty.toString(16)}`);
    }
  }
  return params;
}

// ---------------------------------------------------------------------------
// Control / request messages (§9). Framing: Type (vi64) | Length (16) | Body.
// ---------------------------------------------------------------------------

export const Msg = {
  SETUP: 0x2f00,
  GOAWAY: 0x10,
  REQUEST_UPDATE: 0x2,
  SUBSCRIBE: 0x3,
  SUBSCRIBE_OK: 0x4,
  REQUEST_ERROR: 0x5,
  PUBLISH_NAMESPACE: 0x6,
  REQUEST_OK: 0x7,
  NAMESPACE: 0x8,
  PUBLISH_DONE: 0xb,
  TRACK_STATUS: 0xd,
  NAMESPACE_DONE: 0xe,
  PUBLISH_SKIPPED: 0xf,
  FETCH: 0x16,
  FETCH_OK: 0x18,
  PUBLISH: 0x1d,
  PUBLISH_STATE_NOTIFY: 0x22,
  SUBSCRIBE_NAMESPACE: 0x50,
  SUBSCRIBE_TRACKS: 0x51,
};

export const SetupOption = { PATH: 0x01, AUTHORITY: 0x05, MOQT_IMPLEMENTATION: 0x07 };

export const RequestErrorCode = {
  INTERNAL_ERROR: 0x0,
  UNAUTHORIZED: 0x1,
  TIMEOUT: 0x2,
  NOT_SUPPORTED: 0x3,
  EXCESSIVE_LOAD: 0x9,
  DOES_NOT_EXIST: 0x10,
  UNINTERESTED: 0x20,
  PREFIX_OVERLAP: 0x30,
};

export const PublishDoneCode = { INTERNAL_ERROR: 0x0, TRACK_ENDED: 0x2, GOING_AWAY: 0x4 };
export const ResetCode = { INTERNAL_ERROR: 0x0, CANCELLED: 0x1, DELIVERY_TIMEOUT: 0x2, SESSION_CLOSED: 0x3 };

const REQUEST_FIRST = new Set([
  Msg.TRACK_STATUS, Msg.SUBSCRIBE, Msg.PUBLISH, Msg.FETCH,
  Msg.PUBLISH_NAMESPACE, Msg.SUBSCRIBE_NAMESPACE, Msg.SUBSCRIBE_TRACKS,
]);
export const isRequestFirst = (ty) => REQUEST_FIRST.has(ty);

function writeBody(w, m) {
  switch (m.type) {
    case Msg.SETUP: writeProperties(w, m.options); break;
    case Msg.GOAWAY: w.string(m.uri ?? "").varint(m.timeoutMs ?? 0); break;
    case Msg.SUBSCRIBE:
      w.varint(m.requestId);
      writeNamespace(w, m.namespace);
      w.string(m.name);
      writeParams(w, m.params);
      break;
    case Msg.SUBSCRIBE_OK:
      w.varint(m.trackAlias);
      writeParams(w, m.params);
      writeProperties(w, m.trackProperties);
      break;
    case Msg.REQUEST_OK:
      writeParams(w, m.params);
      writeProperties(w, m.trackProperties);
      break;
    case Msg.REQUEST_ERROR:
      w.varint(m.code).varint(m.retryInterval ?? 0).string(m.reason ?? "");
      break;
    case Msg.REQUEST_UPDATE:
      w.varint(m.requestId);
      writeParams(w, m.params);
      break;
    case Msg.PUBLISH_DONE:
      w.varint(m.status).varint(m.streamCount).string(m.reason ?? "");
      break;
    case Msg.PUBLISH_NAMESPACE:
    case Msg.SUBSCRIBE_NAMESPACE:
      w.varint(m.requestId);
      writeNamespace(w, m.prefix);
      writeParams(w, m.params);
      break;
    case Msg.NAMESPACE:
    case Msg.NAMESPACE_DONE:
      writeNamespace(w, m.suffix);
      break;
    default:
      throw new Error(`cannot encode message type ${m.type}`);
  }
}

export function encodeMessage(m) {
  const body = new Writer();
  writeBody(body, m);
  const b = body.finish();
  if (b.length > 0xffff) throw new Error("control message too large");
  return new Writer(b.length + 4).varint(m.type).u16(b.length).bytes(b).finish();
}

function readBody(type, r) {
  switch (type) {
    case Msg.SETUP: return { type, options: readProperties(r) };
    case Msg.GOAWAY: return { type, uri: fromUtf8(r.lpBytes()), timeoutMs: r.varint() };
    case Msg.SUBSCRIBE:
      return { type, requestId: r.varint(), namespace: readNamespace(r), name: fromUtf8(r.lpBytes()), params: readParams(r) };
    case Msg.SUBSCRIBE_OK:
      return { type, trackAlias: r.varint(), params: readParams(r), trackProperties: readProperties(r) };
    case Msg.REQUEST_OK:
      return { type, params: readParams(r), trackProperties: readProperties(r) };
    case Msg.REQUEST_ERROR: {
      const m = { type, code: r.varint(), retryInterval: r.varint(), reason: fromUtf8(r.lpBytes()) };
      r.rest(); // optional Redirect, unused here
      return m;
    }
    case Msg.REQUEST_UPDATE: return { type, requestId: r.varint(), params: readParams(r) };
    case Msg.PUBLISH_DONE:
      return { type, status: r.varint(), streamCount: r.varint(), reason: fromUtf8(r.lpBytes()) };
    case Msg.PUBLISH_NAMESPACE:
    case Msg.SUBSCRIBE_NAMESPACE:
      return { type, requestId: r.varint(), prefix: readNamespace(r), params: readParams(r) };
    case Msg.NAMESPACE:
    case Msg.NAMESPACE_DONE:
      return { type, suffix: readNamespace(r) };
    default:
      if (Object.values(Msg).includes(type)) return { type, unsupported: true, body: r.rest() };
      throw new ProtocolError(`unknown message type 0x${type.toString(16)}`);
  }
}

/** Decode one framed message from `r`, or throw SHORT without consuming. */
export function decodeMessage(r) {
  const start = r.pos;
  try {
    const type = r.varint();
    const len = r.u16();
    if (r.remaining < len) throw SHORT;
    const body = new Reader(r.bytes, r.pos, r.pos + len);
    r.pos += len;
    let msg;
    try {
      msg = readBody(type, body);
    } catch (e) {
      throw e === SHORT ? new ProtocolError("truncated message body") : e;
    }
    if (body.remaining !== 0) throw new ProtocolError("message length mismatch");
    return msg;
  } catch (e) {
    if (e === SHORT) r.pos = start;
    throw e;
  }
}

// ---------------------------------------------------------------------------
// Data streams and datagrams (§11)
// ---------------------------------------------------------------------------

export const StreamType = { FETCH_HEADER: 0x05, SETUP: 0x2f00, PADDING: 0x132b3e28 };
export const isSubgroupHeader = (ty) => ty < 0x80 && (ty & 0x10) !== 0;
export const ObjectStatus = { NORMAL: 0x0, END_OF_GROUP: 0x3, END_OF_TRACK: 0x4 };

/**
 * header = { trackAlias, groupId, subgroupId (number | "first" | 0),
 *            priority (number | undefined), hasProperties, endOfGroup, firstObject }
 */
export function encodeSubgroupHeader(h) {
  let flags = 0x10;
  if (h.hasProperties) flags |= 0x01;
  const explicit = typeof h.subgroupId === "number" && h.subgroupId !== 0;
  flags |= (h.subgroupId === "first" ? 0b01 : explicit ? 0b10 : 0b00) << 1;
  if (h.endOfGroup) flags |= 0x08;
  if (h.priority === undefined) flags |= 0x20;
  if (h.firstObject) flags |= 0x40;
  const w = new Writer().varint(flags).varint(h.trackAlias).varint(h.groupId);
  if (explicit) w.varint(h.subgroupId);
  if (h.priority !== undefined) w.u8(h.priority);
  return w.finish();
}

export function decodeSubgroupHeader(r) {
  const start = r.pos;
  try {
    const flags = r.varint();
    if (!isSubgroupHeader(flags)) throw new ProtocolError("invalid subgroup header");
    const mode = (flags & 0x06) >> 1;
    if (mode === 0b11) throw new ProtocolError("reserved subgroup id mode");
    const h = { trackAlias: r.varint(), groupId: r.varint() };
    h.subgroupId = mode === 0b10 ? r.varint() : mode === 0b01 ? "first" : 0;
    h.priority = flags & 0x20 ? undefined : r.u8();
    h.hasProperties = !!(flags & 0x01);
    h.endOfGroup = !!(flags & 0x08);
    h.firstObject = !!(flags & 0x40);
    return h;
  } catch (e) {
    if (e === SHORT) r.pos = start;
    throw e;
  }
}

/** Stateful encoder/decoder for objects on one subgroup stream (§11.3.1). */
export class SubgroupObjects {
  constructor(hasProperties) {
    this.hasProperties = hasProperties;
    this.prevId = -1;
  }
  /** obj = { id, status?, properties?: Map, payload: Uint8Array } */
  encode(obj) {
    const w = new Writer(obj.payload.length + 16);
    w.varint(this.prevId < 0 ? obj.id : obj.id - this.prevId - 1);
    this.prevId = obj.id;
    if (this.hasProperties) writePrefixedProperties(w, obj.properties);
    w.varint(obj.payload.length);
    if (obj.payload.length === 0) w.varint(obj.status ?? ObjectStatus.NORMAL);
    else w.bytes(obj.payload);
    return w.finish();
  }
  decode(r) {
    const start = r.pos;
    try {
      const delta = r.varint();
      const id = this.prevId < 0 ? delta : this.prevId + delta + 1;
      const properties = this.hasProperties ? readPrefixedProperties(r) : new Map();
      const len = r.varint();
      const status = len === 0 ? r.varint() : ObjectStatus.NORMAL;
      const payload = len === 0 ? new Uint8Array(0) : r.bytesN(len).slice();
      this.prevId = id;
      return { id, status, properties, payload };
    } catch (e) {
      if (e === SHORT) r.pos = start;
      throw e;
    }
  }
}

/** d = { trackAlias, groupId, objectId, priority?, endOfGroup?, properties?: Map, payload } */
export function encodeDatagram(d) {
  const hasProps = d.properties && d.properties.size > 0;
  let flags = 0;
  if (hasProps) flags |= 0x01;
  if (d.endOfGroup) flags |= 0x02;
  if (d.objectId === 0) flags |= 0x04;
  if (d.priority === undefined) flags |= 0x08;
  const w = new Writer(d.payload.length + 24).varint(flags).varint(d.trackAlias).varint(d.groupId);
  if (d.objectId !== 0) w.varint(d.objectId);
  if (d.priority !== undefined) w.u8(d.priority);
  if (hasProps) writePrefixedProperties(w, d.properties);
  w.bytes(d.payload);
  return w.finish();
}

export function decodeDatagram(bytes) {
  const r = new Reader(bytes);
  try {
    const flags = r.varint();
    if (flags & 0x10 || flags & ~0x2f) throw new ProtocolError("invalid datagram flags");
    if (flags & 0x20 && flags & 0x02) throw new ProtocolError("datagram STATUS with END_OF_GROUP");
    const d = { trackAlias: r.varint(), groupId: r.varint() };
    d.objectId = flags & 0x04 ? 0 : r.varint();
    d.priority = flags & 0x08 ? undefined : r.u8();
    d.endOfGroup = !!(flags & 0x02);
    d.properties = flags & 0x01 ? readPrefixedProperties(r) : new Map();
    if (flags & 0x20) {
      d.status = r.varint();
      d.payload = new Uint8Array(0);
    } else {
      d.status = ObjectStatus.NORMAL;
      d.payload = r.rest();
    }
    return d;
  } catch (e) {
    throw e === SHORT ? new ProtocolError("truncated datagram") : e;
  }
}
