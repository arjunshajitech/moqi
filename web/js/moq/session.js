// MoQT session over WebTransport (browser endpoint = publisher + subscriber).
//
// Stream model (draft-22 §6.3/§6.4):
//   - one uni control stream each way, starting with SETUP
//   - one bidi stream per request; closing it ends the request
//   - uni data streams (SUBGROUP_HEADER + objects), OBJECT_DATAGRAMs

import {
  ALPN, SHORT, ProtocolError, Reader, Writer, Msg, SetupOption, Param, Filter,
  RequestErrorCode, PublishDoneCode, ResetCode, StreamType,
  encodeMessage, decodeMessage, isRequestFirst, isSubgroupHeader,
  encodeSubgroupHeader, decodeSubgroupHeader, SubgroupObjects,
  encodeDatagram, decodeDatagram, nsKey, utf8, fromUtf8,
} from "./codec.js";

const IMPLEMENTATION = "moqi-web/0.1";
const UNKNOWN_ALIAS_WAIT_MS = 2000;

/** Incremental reader over a WebTransport ReadableStream. */
class StreamReader {
  constructor(readable) {
    this.reader = readable.getReader();
    this.buf = new Uint8Array(0);
    this.pos = 0;
  }
  /** Run `fn(Reader)` until it stops throwing SHORT. Returns undefined on clean FIN. */
  async decode(fn) {
    for (;;) {
      if (this.pos < this.buf.length) {
        const r = new Reader(this.buf, this.pos);
        try {
          const v = fn(r);
          this.pos = r.pos;
          return v;
        } catch (e) {
          if (e !== SHORT) throw e;
        }
      }
      const { value, done } = await this.reader.read();
      if (done) {
        if (this.pos < this.buf.length) throw new ProtocolError("stream ended mid-frame");
        return undefined;
      }
      const rest = this.buf.subarray(this.pos);
      const next = new Uint8Array(rest.length + value.length);
      next.set(rest);
      next.set(value, rest.length);
      this.buf = next;
      this.pos = 0;
    }
  }
  message() {
    return this.decode((r) => decodeMessage(r));
  }
  cancel(code = ResetCode.CANCELLED) {
    this.reader.cancel(code).catch(() => {});
  }
}

export class RequestError extends Error {
  constructor(msg) {
    super(msg.reason || `request error 0x${msg.code.toString(16)}`);
    this.code = msg.code;
  }
}

// ---------------------------------------------------------------------------
// Publishing: LocalTrack
// ---------------------------------------------------------------------------

/**
 * A track this endpoint publishes. Media code calls startGroup()/writeObject()
 * or sendDatagram(); every active subscription (normally just the relay's one)
 * gets its own subgroup streams.
 */
export class LocalTrack {
  /** @param {{priority?: number, onSubscribe?: () => void}} opts */
  constructor(session, namespace, name, opts = {}) {
    this.session = session;
    this.namespace = namespace;
    this.name = name;
    this.priority = opts.priority ?? 128;
    this.onSubscribe = opts.onSubscribe ?? (() => {});
    this.subscriptions = new Set();
    this.groupId = -1;
    this.objectId = 0;
    this.largest = undefined;
    this.datagramGroup = 0;
  }

  get hasSubscribers() {
    return this.subscriptions.size > 0;
  }

  /** Begin a new group (e.g. at a video keyframe). */
  startGroup() {
    this.groupId += 1;
    this.objectId = 0;
    for (const sub of this.subscriptions) sub.openGroup(this.groupId);
  }

  /** Append an object to the current group. */
  writeObject(payload, properties) {
    if (this.groupId < 0) this.startGroup();
    const obj = { id: this.objectId++, payload, properties };
    this.largest = { group: this.groupId, object: obj.id };
    for (const sub of this.subscriptions) sub.write(this.groupId, obj);
  }

  /** One object per group, delivered as an OBJECT_DATAGRAM (lossy, no HOL blocking). */
  sendDatagram(payload, properties) {
    const groupId = this.datagramGroup++;
    this.largest = { group: groupId, object: 0 };
    for (const sub of this.subscriptions) {
      this.session.sendDatagram(
        encodeDatagram({ trackAlias: sub.alias, groupId, objectId: 0, priority: this.priority, endOfGroup: true, properties, payload }),
      );
    }
  }

  close() {
    for (const sub of [...this.subscriptions]) sub.done(PublishDoneCode.TRACK_ENDED, "track closed");
  }
}

/** One downstream subscription to a LocalTrack. */
class OutgoingSubscription {
  constructor(track, alias, control) {
    this.track = track;
    this.alias = alias;
    this.control = control; // { writer } of the request stream
    this.groups = new Map(); // groupId -> { writer, codec }
    this.streamCount = 0;
  }

  openGroup(groupId, firstObject = true) {
    // Finish older groups: queued data still flushes, then FIN.
    for (const [g, s] of this.groups) {
      if (g < groupId) {
        s.writer.close().catch(() => {});
        this.groups.delete(g);
      }
    }
    const session = this.track.session;
    const header = { trackAlias: this.alias, groupId, subgroupId: 0, priority: this.track.priority, hasProperties: true, endOfGroup: true, firstObject };
    const state = { writer: null, codec: new SubgroupObjects(true), queue: [encodeSubgroupHeader(header)] };
    this.groups.set(groupId, state);
    this.streamCount++;
    // Newer groups get larger sendOrder so they preempt stale ones.
    session.transport
      .createUnidirectionalStream({ sendOrder: this.track.sendOrderBase + groupId })
      .then((stream) => {
        state.writer = stream.getWriter();
        for (const chunk of state.queue) state.writer.write(chunk).catch(() => {});
        state.queue = null;
        if (!this.groups.has(groupId)) state.writer.close().catch(() => {});
      })
      .catch((e) => console.debug("open data stream failed", e));
  }

  write(groupId, obj) {
    let state = this.groups.get(groupId);
    if (!state) {
      // Subscribed mid-group: open a stream starting at this object.
      this.openGroup(groupId, obj.id === 0);
      state = this.groups.get(groupId);
    }
    const bytes = state.codec.encode(obj);
    if (state.writer) state.writer.write(bytes).catch(() => {});
    else state.queue.push(bytes);
  }

  async done(status, reason) {
    this.track.subscriptions.delete(this);
    for (const s of this.groups.values()) s.writer?.close().catch(() => {});
    this.groups.clear();
    try {
      await this.control.writer.write(encodeMessage({ type: Msg.PUBLISH_DONE, status, streamCount: this.streamCount, reason }));
      await this.control.writer.close();
    } catch {}
  }

  cancel() {
    this.track.subscriptions.delete(this);
    for (const s of this.groups.values()) s.writer?.abort(ResetCode.CANCELLED).catch(() => {});
    this.groups.clear();
  }
}

// ---------------------------------------------------------------------------
// Subscribing: RemoteSubscription
// ---------------------------------------------------------------------------

/**
 * handlers: {
 *   onObject?(groupId, object)   // stream objects, in order within a group
 *   onDatagram?(datagram)
 *   onDone?(status, reason)
 * }
 */
export class RemoteSubscription {
  constructor(session, namespace, name, handlers) {
    this.session = session;
    this.namespace = namespace;
    this.name = name;
    this.handlers = handlers;
    this.alias = undefined;
    this.closed = false;
  }

  close() {
    if (this.closed) return;
    this.closed = true;
    if (this.alias !== undefined) this.session.incoming.delete(this.alias);
    this.writer?.abort(ResetCode.CANCELLED).catch(() => {});
    this.reader?.cancel();
  }
}

// ---------------------------------------------------------------------------
// Session
// ---------------------------------------------------------------------------

export class Session {
  /**
   * @param {string} url  https URL of the relay (WebTransport endpoint)
   * @param {{fingerprint?: string}} opts  hex SHA-256 of a self-signed cert
   */
  static async connect(url, opts = {}) {
    const options = { protocols: [ALPN], congestionControl: "low-latency" };
    if (opts.fingerprint) {
      const hash = new Uint8Array(opts.fingerprint.match(/../g).map((h) => parseInt(h, 16)));
      options.serverCertificateHashes = [{ algorithm: "sha-256", value: hash }];
    }
    const transport = new WebTransport(url, options);
    await transport.ready;
    if (transport.protocol && transport.protocol !== ALPN) {
      transport.close({ closeCode: 0x3, reason: "version mismatch" });
      throw new Error(`relay selected unsupported protocol ${transport.protocol}`);
    }
    const session = new Session(transport);
    await session.#start();
    return session;
  }

  constructor(transport) {
    this.transport = transport;
    this.nextRequestId = 0; // clients use even IDs
    this.nextAlias = 0;
    this.localTracks = new Map(); // nsKey -> LocalTrack
    this.incoming = new Map(); // track alias -> RemoteSubscription
    this.aliasWaiters = new Map(); // alias -> [resolve]
    this.peerSetup = null;
    this.isClosed = false;
    this.closed = transport.closed.then(
      (info) => info,
      (err) => ({ closeCode: -1, reason: String(err) }),
    );
    this.closed.then(() => (this.isClosed = true));
    this.datagramWriter = (transport.datagrams.createWritable?.() ?? transport.datagrams.writable).getWriter();
  }

  async #start() {
    const control = await this.transport.createUnidirectionalStream({ sendOrder: Number.MAX_SAFE_INTEGER });
    this.controlWriter = control.getWriter();
    const options = new Map([[SetupOption.MOQT_IMPLEMENTATION, utf8(IMPLEMENTATION)]]);
    await this.controlWriter.write(encodeMessage({ type: Msg.SETUP, options }));

    let setupResolve;
    this.setupDone = new Promise((r) => (setupResolve = r));
    this.#acceptUni(setupResolve);
    this.#acceptBidi();
    this.#readDatagrams();
    await this.setupDone;
  }

  close(code = 0, reason = "bye") {
    this.isClosed = true;
    try {
      this.transport.close({ closeCode: code, reason });
    } catch {}
  }

  #fail(e) {
    if (this.isClosed) return; // streams erroring after close() is expected
    console.error("MoQT session error:", e);
    this.close(e instanceof ProtocolError ? e.code : 0x1, String(e.message ?? e).slice(0, 100));
  }

  // ---- inbound streams ----

  async #acceptUni(setupResolve) {
    const reader = this.transport.incomingUnidirectionalStreams.getReader();
    for (;;) {
      const { value: stream, done } = await reader.read().catch(() => ({ done: true }));
      if (done) return;
      this.#handleUni(new StreamReader(stream), setupResolve).catch((e) => this.#fail(e));
    }
  }

  async #handleUni(sr, setupResolve) {
    // Peek the stream type; for SETUP it is also the message type.
    const ty = await sr.decode((r) => {
      const start = r.pos;
      const v = r.varint();
      r.pos = start; // don't consume
      return v;
    });
    if (ty === undefined) return;
    if (ty === StreamType.SETUP) {
      const setup = await sr.message();
      if (this.peerSetup) throw new ProtocolError("second control stream");
      this.peerSetup = setup;
      const impl = setup.options.get(SetupOption.MOQT_IMPLEMENTATION);
      console.info("MoQT SETUP from relay:", impl ? fromUtf8(impl) : "(no implementation id)");
      setupResolve();
      for (;;) {
        const msg = await sr.message();
        if (!msg) throw new ProtocolError("control stream closed");
        if (msg.type === Msg.GOAWAY) console.warn("relay sent GOAWAY", msg);
        else throw new ProtocolError(`unexpected message 0x${msg.type.toString(16)} on control stream`);
      }
    }
    if (isSubgroupHeader(ty)) return this.#handleSubgroup(sr);
    if (ty === StreamType.PADDING) {
      while ((await sr.decode((r) => r.rest())) !== undefined);
      return;
    }
    throw new ProtocolError(`unexpected stream type 0x${ty.toString(16)}`);
  }

  async #waitAlias(alias) {
    if (this.incoming.has(alias)) return this.incoming.get(alias);
    return new Promise((resolve) => {
      const list = this.aliasWaiters.get(alias) ?? [];
      list.push(resolve);
      this.aliasWaiters.set(alias, list);
      setTimeout(() => resolve(this.incoming.get(alias)), UNKNOWN_ALIAS_WAIT_MS);
    });
  }

  async #handleSubgroup(sr) {
    const header = await sr.decode((r) => decodeSubgroupHeader(r));
    if (!header) throw new ProtocolError("stream ended in subgroup header");
    const sub = await this.#waitAlias(header.trackAlias);
    if (!sub || sub.closed) {
      sr.cancel();
      return;
    }
    const codec = new SubgroupObjects(header.hasProperties);
    try {
      for (;;) {
        const obj = await sr.decode((r) => codec.decode(r));
        if (!obj) return; // FIN: subgroup complete
        if (sub.closed) return sr.cancel();
        sub.handlers.onObject?.(header.groupId, obj);
      }
    } catch (e) {
      if (e instanceof ProtocolError) throw e;
      // Stream reset by the relay (e.g. too far behind) — not a session error.
    }
  }

  async #readDatagrams() {
    const reader = this.transport.datagrams.readable.getReader();
    for (;;) {
      const { value, done } = await reader.read().catch(() => ({ done: true }));
      if (done) return;
      try {
        const d = decodeDatagram(value);
        const sub = this.incoming.get(d.trackAlias);
        if (sub && !sub.closed) sub.handlers.onDatagram?.(d);
      } catch (e) {
        return this.#fail(e);
      }
    }
  }

  async #acceptBidi() {
    const reader = this.transport.incomingBidirectionalStreams.getReader();
    for (;;) {
      const { value: stream, done } = await reader.read().catch(() => ({ done: true }));
      if (done) return;
      this.#handleRequest(stream).catch((e) => this.#fail(e));
    }
  }

  async #handleRequest(stream) {
    await this.setupDone;
    const sr = new StreamReader(stream.readable);
    const writer = stream.writable.getWriter();
    const msg = await sr.message();
    if (!msg) return;
    if (msg.type === Msg.SUBSCRIBE) {
      if (msg.requestId % 2 !== 1) throw new ProtocolError("relay request ids must be odd", 0x4);
      return this.#serveSubscribe(msg, sr, writer);
    }
    if (isRequestFirst(msg.type)) {
      await writer.write(encodeMessage({ type: Msg.REQUEST_ERROR, code: RequestErrorCode.NOT_SUPPORTED, reason: "not supported" }));
      return writer.close();
    }
    throw new ProtocolError(`0x${msg.type.toString(16)} cannot open a request stream`);
  }

  async #serveSubscribe(msg, sr, writer) {
    const track = this.localTracks.get(nsKey(msg.namespace, msg.name));
    if (!track) {
      await writer.write(encodeMessage({ type: Msg.REQUEST_ERROR, code: RequestErrorCode.DOES_NOT_EXIST, reason: "no such track" }));
      return writer.close();
    }
    const alias = this.nextAlias++;
    const params = new Map();
    if (track.largest) params.set(Param.LARGEST_OBJECT, track.largest);
    await writer.write(encodeMessage({ type: Msg.SUBSCRIBE_OK, trackAlias: alias, params }));

    const sub = new OutgoingSubscription(track, alias, { writer });
    track.subscriptions.add(sub);
    console.info(`relay subscribed to ${msg.name} (alias ${alias})`);
    track.onSubscribe();

    // Read REQUEST_UPDATEs until the relay cancels (reset) or FINs.
    try {
      for (;;) {
        const m = await sr.message();
        if (!m) break; // FIN only: keep publishing until reset or PUBLISH_DONE
        if (m.type === Msg.REQUEST_UPDATE) await writer.write(encodeMessage({ type: Msg.REQUEST_OK }));
        else throw new ProtocolError(`unexpected 0x${m.type.toString(16)} on SUBSCRIBE stream`);
      }
      await writer.closed; // resolves/rejects when the relay stops the stream
    } catch (e) {
      if (e instanceof ProtocolError) throw e;
    }
    if (track.subscriptions.has(sub)) {
      console.info(`relay unsubscribed from ${msg.name}`);
      sub.cancel();
    }
  }

  sendDatagram(bytes) {
    // WebTransport drops datagrams under congestion, which is the point.
    this.datagramWriter.write(bytes).catch(() => {});
  }

  // ---- outbound requests ----

  #requestId() {
    const id = this.nextRequestId;
    this.nextRequestId += 2;
    return id;
  }

  async #openRequest(msg) {
    const stream = await this.transport.createBidirectionalStream({ sendOrder: Number.MAX_SAFE_INTEGER - 1 });
    const writer = stream.writable.getWriter();
    const sr = new StreamReader(stream.readable);
    await writer.write(encodeMessage(msg));
    return { writer, sr };
  }

  /** Register a track that the relay may SUBSCRIBE to. */
  createTrack(namespace, name, opts) {
    const track = new LocalTrack(this, namespace, name, opts);
    track.sendOrderBase = (255 - track.priority) * 1e9;
    this.localTracks.set(nsKey(namespace, name), track);
    return track;
  }

  /** PUBLISH_NAMESPACE; resolves to a handle whose close() withdraws it. */
  async publishNamespace(prefix) {
    const { writer, sr } = await this.#openRequest({ type: Msg.PUBLISH_NAMESPACE, requestId: this.#requestId(), prefix });
    const reply = await sr.message();
    if (reply?.type === Msg.REQUEST_ERROR) {
      writer.close().catch(() => {});
      throw new RequestError(reply);
    }
    if (reply?.type !== Msg.REQUEST_OK) throw new ProtocolError("bad PUBLISH_NAMESPACE response");
    return {
      close: () => {
        writer.abort(ResetCode.CANCELLED).catch(() => {});
        sr.cancel();
      },
    };
  }

  /** SUBSCRIBE_NAMESPACE; callbacks receive the namespace suffix (array). */
  async subscribeNamespace(prefix, { onNamespace, onNamespaceDone }) {
    const { writer, sr } = await this.#openRequest({ type: Msg.SUBSCRIBE_NAMESPACE, requestId: this.#requestId(), prefix });
    const reply = await sr.message();
    if (reply?.type === Msg.REQUEST_ERROR) throw new RequestError(reply);
    if (reply?.type !== Msg.REQUEST_OK) throw new ProtocolError("bad SUBSCRIBE_NAMESPACE response");
    (async () => {
      for (;;) {
        const m = await sr.message();
        if (!m) return;
        if (m.type === Msg.NAMESPACE) onNamespace(m.suffix);
        else if (m.type === Msg.NAMESPACE_DONE) onNamespaceDone(m.suffix);
      }
    })().catch((e) => (e instanceof ProtocolError ? this.#fail(e) : undefined));
    return { close: () => writer.abort(ResetCode.CANCELLED).catch(() => {}) };
  }

  /** SUBSCRIBE; resolves once SUBSCRIBE_OK arrives. */
  async subscribe(namespace, name, handlers, { priority = 128 } = {}) {
    const sub = new RemoteSubscription(this, namespace, name, handlers);
    const params = new Map([
      [Param.SUBSCRIBER_PRIORITY, priority],
      // Start at the current group so video begins at a keyframe.
      [Param.LOCATION_FILTER, { type: Filter.RELATIVE_START, startGroup: 1 }],
    ]);
    const { writer, sr } = await this.#openRequest({ type: Msg.SUBSCRIBE, requestId: this.#requestId(), namespace, name, params });
    sub.writer = writer;
    sub.reader = sr;
    const reply = await sr.message();
    if (reply?.type === Msg.REQUEST_ERROR) {
      writer.close().catch(() => {});
      throw new RequestError(reply);
    }
    if (reply?.type !== Msg.SUBSCRIBE_OK) throw new ProtocolError("bad SUBSCRIBE response");
    if (this.incoming.has(reply.trackAlias)) throw new ProtocolError("duplicate track alias", 0x5);
    sub.alias = reply.trackAlias;
    this.incoming.set(sub.alias, sub);
    for (const resolve of this.aliasWaiters.get(sub.alias) ?? []) resolve(sub);
    this.aliasWaiters.delete(sub.alias);

    (async () => {
      for (;;) {
        const m = await sr.message();
        if (!m) break;
        if (m.type === Msg.PUBLISH_DONE) {
          handlers.onDone?.(m.status, m.reason);
          break;
        }
      }
    })()
      .catch(() => {})
      .finally(() => {
        if (!sub.closed) {
          sub.closed = true;
          // Late objects may still arrive briefly; then forget the alias.
          setTimeout(() => this.incoming.get(sub.alias) === sub && this.incoming.delete(sub.alias), 1000);
        }
      });
    return sub;
  }
}
