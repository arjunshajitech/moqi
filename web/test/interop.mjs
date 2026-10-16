// Checks the browser codec against the Rust codec, byte for byte.
//   cargo run -q -p moqi-proto --example vectors | node web/test/interop.mjs

import assert from "node:assert/strict";
import {
  Msg, Param, Filter, Prop, Reader, SetupOption, RequestErrorCode, PublishDoneCode,
  encodeMessage, decodeMessage, encodeSubgroupHeader, decodeSubgroupHeader,
  SubgroupObjects, encodeDatagram, decodeDatagram, utf8, Writer, varintLen,
} from "../js/moq/codec.js";

const input = await new Promise((resolve) => {
  let s = "";
  process.stdin.on("data", (d) => (s += d)).on("end", () => resolve(s));
});
const rust = JSON.parse(input);
const hex = (b) => Buffer.from(b).toString("hex");
const bytes = (h) => new Uint8Array(Buffer.from(h, "hex"));

const ns = ["moqi", "demo", "a1b2c3d4"];
const ts = new Map([[Prop.TIMESTAMP, 1_234_567_890]]);

const messages = {
  setup: { type: Msg.SETUP, options: new Map([[SetupOption.MOQT_IMPLEMENTATION, utf8("x/1")]]) },
  subscribe: {
    type: Msg.SUBSCRIBE, requestId: 2, namespace: ns, name: "video",
    params: new Map([[Param.SUBSCRIBER_PRIORITY, 3], [Param.LOCATION_FILTER, { type: Filter.RELATIVE_START, startGroup: 1 }]]),
  },
  subscribe_ok: { type: Msg.SUBSCRIBE_OK, trackAlias: 7, params: new Map([[Param.LARGEST_OBJECT, { group: 300, object: 20 }]]) },
  request_ok: { type: Msg.REQUEST_OK },
  request_error: { type: Msg.REQUEST_ERROR, code: RequestErrorCode.EXCESSIVE_LOAD, reason: "room is full" },
  publish_done: { type: Msg.PUBLISH_DONE, status: PublishDoneCode.TRACK_ENDED, streamCount: 12, reason: "bye" },
  publish_namespace: { type: Msg.PUBLISH_NAMESPACE, requestId: 0, prefix: ns },
  subscribe_namespace: { type: Msg.SUBSCRIBE_NAMESPACE, requestId: 4, prefix: ns.slice(0, 2) },
  namespace: { type: Msg.NAMESPACE, suffix: ["a1b2c3d4"] },
};

for (const [name, msg] of Object.entries(messages)) {
  assert.equal(hex(encodeMessage(msg)), rust[name], `encode ${name}`);
  const decoded = decodeMessage(new Reader(bytes(rust[name])));
  assert.equal(decoded.type, msg.type, `decode ${name}`);
  assert.equal(hex(encodeMessage(decoded)), rust[name], `re-encode ${name}`);
}

// Subgroup stream
const header = { trackAlias: 7, groupId: 300, subgroupId: 0, priority: 3, hasProperties: true, endOfGroup: true, firstObject: true };
const enc = new SubgroupObjects(true);
const stream = [
  encodeSubgroupHeader(header),
  enc.encode({ id: 0, properties: ts, payload: utf8("key") }),
  enc.encode({ id: 1, properties: ts, payload: utf8("delta") }),
];
assert.equal(stream.map(hex).join(""), rust.subgroup_stream, "subgroup stream");
const r = new Reader(bytes(rust.subgroup_stream));
const h = decodeSubgroupHeader(r);
assert.deepEqual(h, header);
const dec = new SubgroupObjects(h.hasProperties);
assert.equal(Buffer.from(dec.decode(r).payload).toString(), "key");
const second = dec.decode(r);
assert.equal(second.id, 1);
assert.equal(second.properties.get(Prop.TIMESTAMP), 1_234_567_890);
assert.equal(r.remaining, 0);

// Datagram
const dg = { trackAlias: 9, groupId: 70_000, objectId: 0, priority: 1, endOfGroup: true, properties: ts, payload: utf8("opus") };
assert.equal(hex(encodeDatagram(dg)), rust.datagram, "datagram");
const d = decodeDatagram(bytes(rust.datagram));
assert.equal(d.groupId, 70_000);
assert.equal(Buffer.from(d.payload).toString(), "opus");

// Varint spec vectors (§8.1 Table 4) and boundaries
for (const [h, v] of [["25", 37], ["8025", 37], ["bbbd", 15293], ["ed7f3e7d", 226442877], ["faa1a0e403d8", 2893212287960]]) {
  assert.equal(new Reader(bytes(h)).varint(), v);
}
for (const bits of [7, 14, 21, 28, 35, 42, 49]) {
  for (const v of [2 ** bits - 1, 2 ** bits]) {
    const b = new Writer().varint(v).finish();
    assert.equal(b.length, varintLen(v));
    assert.equal(new Reader(b).varint(), v);
  }
}

console.log(`interop OK: ${Object.keys(messages).length} messages, subgroup stream, datagram, varints`);
