// MoQT tracks -> WebCodecs -> canvas / AudioWorklet, for one remote participant.

import { Prop, fromUtf8 } from "../moq/codec.js";
import { TRACKS } from "./publish.js";

/** Decodes a VP8 track (one group per GOP) and paints frames onto a canvas. */
class VideoRenderer {
  constructor(canvas, onActivity) {
    this.canvas = canvas;
    this.ctx = canvas.getContext("2d");
    this.onActivity = onActivity;
    this.codec = "vp8";
    this.group = -1;
    this.nextObject = 0;
    this.needKey = true;
    this.stats = { frames: 0, bytes: 0 };
    this.#createDecoder();
  }

  #createDecoder() {
    this.decoder = new VideoDecoder({
      output: (frame) => this.#paint(frame),
      error: (e) => {
        console.warn("video decoder error; waiting for next keyframe", e);
        this.needKey = true;
      },
    });
  }

  #paint(frame) {
    if (this.canvas.width !== frame.displayWidth || this.canvas.height !== frame.displayHeight) {
      this.canvas.width = frame.displayWidth;
      this.canvas.height = frame.displayHeight;
    }
    this.ctx.drawImage(frame, 0, 0);
    frame.close();
    this.stats.frames++;
    this.onActivity?.();
  }

  onObject(groupId, obj) {
    this.stats.bytes += obj.payload.length;
    if (groupId < this.group) return; // stale group still draining
    if (groupId > this.group) {
      this.group = groupId;
      this.nextObject = 0;
      this.needKey = obj.id !== 0;
    }
    if (obj.id !== this.nextObject) this.needKey = true; // gap within group
    this.nextObject = obj.id + 1;
    if (this.needKey && obj.id !== 0) return;

    if (this.decoder.state === "closed") this.#createDecoder();
    if (this.decoder.state !== "configured") this.decoder.configure({ codec: this.codec, optimizeForLatency: true });
    // Falling behind: skip the rest of this GOP rather than add latency.
    if (obj.id !== 0 && this.decoder.decodeQueueSize > 3) {
      this.needKey = true;
      return;
    }
    this.needKey = false;
    this.decoder.decode(
      new EncodedVideoChunk({
        type: obj.id === 0 ? "key" : "delta",
        timestamp: obj.properties.get(Prop.TIMESTAMP) ?? 0,
        data: obj.payload,
      }),
    );
  }

  close() {
    if (this.decoder.state !== "closed") this.decoder.close();
  }
}

/** Decodes Opus datagrams into a jitter-buffered AudioWorklet. */
class AudioPlayer {
  constructor(audioContext, onLevel) {
    this.node = new AudioWorkletNode(audioContext, "moq-playback", { numberOfInputs: 0, outputChannelCount: [1] });
    this.node.connect(audioContext.destination);
    this.onLevel = onLevel;
    this.lastGroup = -1;
    this.stats = { frames: 0, bytes: 0, lost: 0 };
    this.decoder = new AudioDecoder({
      output: (data) => {
        const pcm = new Float32Array(data.numberOfFrames);
        data.copyTo(pcm, { planeIndex: 0, format: "f32-planar" });
        data.close();
        let peak = 0;
        for (let i = 0; i < pcm.length; i += 16) peak = Math.max(peak, Math.abs(pcm[i]));
        this.onLevel?.(peak);
        this.node.port.postMessage(pcm, [pcm.buffer]);
      },
      error: (e) => console.warn("audio decoder error", e),
    });
    this.decoder.configure({ codec: "opus", sampleRate: 48_000, numberOfChannels: 1 });
  }

  onDatagram(d) {
    if (d.groupId <= this.lastGroup) return; // reordered/duplicate: too late to play
    if (this.lastGroup >= 0) this.stats.lost += d.groupId - this.lastGroup - 1;
    this.lastGroup = d.groupId;
    this.stats.frames++;
    this.stats.bytes += d.payload.length;
    if (this.decoder.state !== "configured") return;
    this.decoder.decode(new EncodedAudioChunk({ type: "key", timestamp: d.properties.get(Prop.TIMESTAMP) ?? 0, data: d.payload }));
  }

  close() {
    this.node.disconnect();
    if (this.decoder.state !== "closed") this.decoder.close();
  }
}

/** Subscribes to every track of one remote participant. */
export class RemotePeer {
  /**
   * @param {{audioContext: AudioContext, video: HTMLCanvasElement, screen: HTMLCanvasElement,
   *          onCatalog(info), onAudioLevel(peak), onEnded()}} ui
   */
  constructor(session, namespace, ui) {
    this.session = session;
    this.namespace = namespace;
    this.ui = ui;
    this.subs = [];
    this.video = new VideoRenderer(ui.video);
    this.screen = new VideoRenderer(ui.screen);
    this.audio = new AudioPlayer(ui.audioContext, ui.onAudioLevel);
  }

  async start() {
    const sub = (name, handlers) =>
      this.session
        .subscribe(this.namespace, name, handlers, { priority: TRACKS[name].priority })
        .then((s) => this.subs.push(s))
        .catch((e) => console.warn(`subscribe ${name} failed:`, e.message));

    await Promise.all([
      sub("catalog", {
        onObject: (_g, obj) => {
          try {
            this.ui.onCatalog(JSON.parse(fromUtf8(obj.payload)));
          } catch (e) {
            console.warn("bad catalog", e);
          }
        },
        onDone: () => this.ui.onEnded?.(),
      }),
      sub("audio", { onDatagram: (d) => this.audio.onDatagram(d) }),
      sub("video", { onObject: (g, o) => this.video.onObject(g, o) }),
      sub("screen", { onObject: (g, o) => this.screen.onObject(g, o) }),
    ]);
  }

  stats() {
    return { video: this.video.stats, screen: this.screen.stats, audio: this.audio.stats };
  }

  close() {
    for (const s of this.subs) s.close();
    this.video.close();
    this.screen.close();
    this.audio.close();
  }
}
