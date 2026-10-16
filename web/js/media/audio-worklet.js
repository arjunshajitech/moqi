// AudioWorklet processors.
//  - "moq-playback": per-peer jitter buffer fed with decoded PCM.
//  - "moq-capture": mic PCM tap for browsers without MediaStreamTrackProcessor.

const TARGET_MS = 60; // start playout once this much is buffered
const MAX_MS = 250; // trim backlog beyond this to bound latency

class PlaybackProcessor extends AudioWorkletProcessor {
  constructor() {
    super();
    this.capacity = sampleRate * 2; // 2 s ring
    this.ring = new Float32Array(this.capacity);
    this.read = 0;
    this.write = 0;
    this.playing = false;
    this.port.onmessage = (e) => this.push(e.data);
  }

  get buffered() {
    return this.write - this.read;
  }

  push(samples) {
    for (let i = 0; i < samples.length; i++) this.ring[(this.write + i) % this.capacity] = samples[i];
    this.write += samples.length;
    const max = (sampleRate * MAX_MS) / 1000;
    if (this.buffered > max) this.read = this.write - (sampleRate * TARGET_MS) / 1000;
  }

  process(_inputs, outputs) {
    const out = outputs[0];
    const frames = out[0].length;
    if (!this.playing && this.buffered >= (sampleRate * TARGET_MS) / 1000) this.playing = true;
    if (this.playing && this.buffered < frames) this.playing = false; // underrun: re-buffer
    for (let i = 0; i < frames; i++) {
      const s = this.playing ? this.ring[(this.read + i) % this.capacity] : 0;
      for (const ch of out) ch[i] = s;
    }
    if (this.playing) this.read += frames;
    return true;
  }
}

class CaptureProcessor extends AudioWorkletProcessor {
  constructor() {
    super();
    this.chunk = new Float32Array(960); // 20 ms @ 48 kHz
    this.fill = 0;
  }

  process(inputs) {
    const input = inputs[0]?.[0];
    if (!input) return true;
    for (let i = 0; i < input.length; i++) {
      this.chunk[this.fill++] = input[i];
      if (this.fill === this.chunk.length) {
        this.port.postMessage(this.chunk, [this.chunk.buffer]);
        this.chunk = new Float32Array(960);
        this.fill = 0;
      }
    }
    return true;
  }
}

registerProcessor("moq-playback", PlaybackProcessor);
registerProcessor("moq-capture", CaptureProcessor);
