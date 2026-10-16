// Local capture -> WebCodecs -> MoQT tracks.
//
// Track layout under namespace ["moqi", <room>, <peer>]:
//   catalog  JSON describing the other tracks; one group per update
//   audio    Opus, one 20 ms frame per OBJECT_DATAGRAM (lossy, no HOL blocking)
//   video    VP8 camera; one group per GOP, object 0 = keyframe, one stream per group
//   screen   VP8 screen share; same layout as video
// Capture time travels in the TIMESTAMP object property (draft-ietf-moq-loc).

import { Prop, utf8 } from "../moq/codec.js";

export const TRACKS = {
  catalog: { priority: 0 },
  audio: { priority: 1 },
  screen: { priority: 2 },
  video: { priority: 3 },
};

const KEYFRAME_INTERVAL_US = 2_000_000;
const AUDIO_RATE = 48_000;
const AUDIO_FRAME = 960; // 20 ms

const tsProps = (us) => new Map([[Prop.TIMESTAMP, Math.max(0, Math.round(us))]]);

/** Calls onFrame(VideoFrame) for each frame of `track`; returns stop(). */
function frameSource(track, fps, onFrame) {
  if ("MediaStreamTrackProcessor" in window) {
    const reader = new MediaStreamTrackProcessor({ track }).readable.getReader();
    let stopped = false;
    (async () => {
      while (!stopped) {
        const { value, done } = await reader.read().catch(() => ({ done: true }));
        if (done) return;
        onFrame(value);
      }
    })();
    return () => {
      stopped = true;
      reader.cancel().catch(() => {});
    };
  }
  // Fallback: sample a <video> element.
  const video = document.createElement("video");
  video.muted = true;
  video.playsInline = true;
  video.srcObject = new MediaStream([track]);
  video.play().catch(() => {});
  const timer = setInterval(() => {
    if (video.readyState >= 2 && video.videoWidth > 0) {
      onFrame(new VideoFrame(video, { timestamp: performance.now() * 1000 }));
    }
  }, 1000 / fps);
  return () => {
    clearInterval(timer);
    video.srcObject = null;
  };
}

class VideoPublisher {
  constructor(mediaTrack, localTrack, { bitrate, fps, codec = "vp8", onConfig }) {
    this.mediaTrack = mediaTrack;
    this.track = localTrack;
    this.bitrate = bitrate;
    this.fps = fps;
    this.codec = codec;
    this.onConfig = onConfig;
    this.forceKey = true;
    this.lastKey = -Infinity;
    this.paused = false;
    this.stats = { frames: 0, bytes: 0 };
    this.encoder = new VideoEncoder({
      output: (chunk) => this.#onChunk(chunk),
      error: (e) => {
        console.error("video encoder error", e);
        this.config = null;
      },
    });
    this.stopSource = frameSource(mediaTrack, fps, (frame) => this.#encode(frame));
  }

  #encode(frame) {
    try {
      if (this.paused || this.encoder.state === "closed") return;
      const width = frame.displayWidth & ~1;
      const height = frame.displayHeight & ~1;
      if (!this.config || this.encoder.state !== "configured" || this.config.width !== width || this.config.height !== height) {
        this.config = { codec: this.codec, width, height, bitrate: this.bitrate, framerate: this.fps, latencyMode: "realtime" };
        if (this.encoder.state === "configured") this.encoder.reset();
        this.encoder.configure(this.config);
        this.forceKey = true;
        this.onConfig?.({ codec: this.codec, width, height });
      }
      // Never queue: a late frame is worse than a dropped one.
      if (this.encoder.encodeQueueSize > 2) return;
      const keyFrame = this.forceKey || frame.timestamp - this.lastKey >= KEYFRAME_INTERVAL_US;
      this.encoder.encode(frame, { keyFrame });
      if (keyFrame) {
        this.forceKey = false;
        this.lastKey = frame.timestamp;
      }
    } finally {
      frame.close();
    }
  }

  #onChunk(chunk) {
    const data = new Uint8Array(chunk.byteLength);
    chunk.copyTo(data);
    if (chunk.type === "key") this.track.startGroup();
    else if (this.track.groupId < 0) return; // no keyframe yet
    this.track.writeObject(data, tsProps(chunk.timestamp));
    this.stats.frames++;
    this.stats.bytes += data.length;
  }

  requestKeyframe() {
    this.forceKey = true;
  }

  setPaused(paused) {
    this.paused = paused;
    if (!paused) this.forceKey = true;
  }

  stop() {
    this.stopSource();
    if (this.encoder.state !== "closed") this.encoder.close();
    this.mediaTrack.stop();
  }
}

class AudioPublisher {
  /** Captures through an AudioWorklet at 48 kHz mono (the context resamples). */
  constructor(mediaTrack, localTrack, audioContext) {
    this.mediaTrack = mediaTrack;
    this.track = localTrack;
    this.muted = false;
    this.samples = 0;
    this.stats = { frames: 0, bytes: 0 };
    this.encoder = new AudioEncoder({
      output: (chunk) => {
        const data = new Uint8Array(chunk.byteLength);
        chunk.copyTo(data);
        this.track.sendDatagram(data, tsProps(chunk.timestamp));
        this.stats.frames++;
        this.stats.bytes += data.length;
      },
      error: (e) => console.error("audio encoder error", e),
    });
    this.encoder.configure({ codec: "opus", sampleRate: AUDIO_RATE, numberOfChannels: 1, bitrate: 32_000 });

    this.source = audioContext.createMediaStreamSource(new MediaStream([mediaTrack]));
    this.node = new AudioWorkletNode(audioContext, "moq-capture", { numberOfInputs: 1, numberOfOutputs: 1, channelCount: 1, channelCountMode: "explicit" });
    this.sink = audioContext.createGain();
    this.sink.gain.value = 0; // keep the node pulled without playing it back
    this.source.connect(this.node).connect(this.sink).connect(audioContext.destination);
    this.node.port.onmessage = (e) => this.#encode(e.data);
  }

  #encode(pcm) {
    const timestamp = (this.samples * 1e6) / AUDIO_RATE;
    this.samples += pcm.length;
    if (this.muted || this.encoder.state !== "configured") return;
    const data = new AudioData({ format: "f32-planar", sampleRate: AUDIO_RATE, numberOfFrames: AUDIO_FRAME, numberOfChannels: 1, timestamp, data: pcm });
    this.encoder.encode(data);
    data.close();
  }

  stop() {
    this.node.port.onmessage = null;
    this.source.disconnect();
    this.node.disconnect();
    this.sink.disconnect();
    if (this.encoder.state !== "closed") this.encoder.close();
    this.mediaTrack.stop();
  }
}

/** Everything one participant publishes. */
export class Broadcast {
  constructor(session, namespace, { name, audioContext }) {
    this.session = session;
    this.namespace = namespace;
    this.audioContext = audioContext;
    this.info = {
      version: 1,
      name,
      tracks: {
        audio: { active: false, codec: "opus", sampleRate: AUDIO_RATE, numberOfChannels: 1 },
        video: { active: false, codec: "vp8" },
        screen: { active: false, codec: "vp8" },
      },
    };
    const make = (name, onSubscribe) => session.createTrack(namespace, name, { priority: TRACKS[name].priority, onSubscribe });
    this.catalog = make("catalog", () => this.#publishCatalog());
    this.audio = make("audio");
    this.video = make("video", () => this.camera?.requestKeyframe());
    this.screen = make("screen", () => this.screenShare?.requestKeyframe());
  }

  #publishCatalog() {
    this.catalog.startGroup();
    this.catalog.writeObject(utf8(JSON.stringify(this.info)));
  }

  #update(track, patch) {
    Object.assign(this.info.tracks[track], patch);
    this.#publishCatalog();
  }

  startCamera(mediaTrack) {
    this.camera = new VideoPublisher(mediaTrack, this.video, {
      bitrate: 1_200_000,
      fps: 30,
      onConfig: (cfg) => this.#update("video", { ...cfg, active: !this.camera?.paused }),
    });
    this.#update("video", { active: true });
  }

  setCameraEnabled(enabled) {
    this.camera?.setPaused(!enabled);
    this.#update("video", { active: enabled && !!this.camera });
  }

  startMic(mediaTrack) {
    this.mic = new AudioPublisher(mediaTrack, this.audio, this.audioContext);
    this.#update("audio", { active: true });
  }

  setMicEnabled(enabled) {
    if (this.mic) this.mic.muted = !enabled;
    this.#update("audio", { active: enabled && !!this.mic });
  }

  startScreen(mediaTrack) {
    this.screenShare = new VideoPublisher(mediaTrack, this.screen, {
      bitrate: 2_500_000,
      fps: 15,
      onConfig: (cfg) => this.#update("screen", { ...cfg, active: true }),
    });
    this.#update("screen", { active: true });
  }

  stopScreen() {
    this.screenShare?.stop();
    this.screenShare = null;
    this.#update("screen", { active: false });
  }

  stats() {
    return { video: this.camera?.stats, screen: this.screenShare?.stats, audio: this.mic?.stats };
  }

  close() {
    this.camera?.stop();
    this.mic?.stop();
    this.screenShare?.stop();
    for (const t of [this.catalog, this.audio, this.video, this.screen]) t.close();
  }
}
