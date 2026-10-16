// moqi: two-party call over a MoQT relay.
//
// Each participant publishes namespace ["moqi", room, peerId] and
// discovers the other through SUBSCRIBE_NAMESPACE ["moqi", room].

import { RequestErrorCode } from "./moq/codec.js";
import { Session, RequestError } from "./moq/session.js";
import { Broadcast } from "./media/publish.js";
import { RemotePeer } from "./media/watch.js";

const APP = "moqi";
const $ = (id) => document.getElementById(id);

const ui = {
  status: $("status"),
  lobby: $("lobby"),
  call: $("call"),
  form: $("join-form"),
  name: $("name"),
  room: $("room"),
  relay: $("relay"),
  joinBtn: $("join-btn"),
  joinError: $("join-error"),
  stage: $("stage"),
  toast: $("toast"),
};

let call = null;

// ---------------------------------------------------------------------------
// Lobby
// ---------------------------------------------------------------------------

const params = new URLSearchParams(location.search);
ui.room.value = params.get("room") ?? "demo";
ui.name.value = localStorageGet("moqi-name") ?? "";
ui.relay.value = params.get("relay") ?? `https://${location.hostname}:4443/moq`;

function localStorageGet(k) {
  try {
    return localStorage.getItem(k);
  } catch {
    return null;
  }
}

function setStatus(state, text) {
  ui.status.dataset.state = state;
  ui.status.textContent = text;
}

function toast(text, ms = 2500) {
  ui.toast.textContent = text;
  ui.toast.hidden = false;
  clearTimeout(toast.timer);
  toast.timer = setTimeout(() => (ui.toast.hidden = true), ms);
}

const initials = (name) => (name || "?").trim().slice(0, 1).toUpperCase() || "?";

ui.form.addEventListener("submit", async (e) => {
  e.preventDefault();
  ui.joinError.textContent = "";
  ui.joinBtn.disabled = true;
  try {
    localStorage.setItem("moqi-name", ui.name.value);
  } catch {}
  try {
    call = await Call.join({ name: ui.name.value.trim(), room: ui.room.value.trim(), relay: ui.relay.value.trim() });
    history.replaceState(null, "", `?room=${encodeURIComponent(call.room)}`);
  } catch (err) {
    console.error(err);
    ui.joinError.textContent = err.message;
    setStatus("error", "Not connected");
  } finally {
    ui.joinBtn.disabled = false;
  }
});

function checkSupport() {
  const missing = ["WebTransport", "VideoEncoder", "AudioEncoder", "AudioWorkletNode"].filter((k) => !(k in window));
  if (missing.length) {
    ui.joinError.textContent = `This browser lacks ${missing.join(", ")}. Use a recent Chrome or Edge.`;
    ui.joinBtn.disabled = true;
  }
}
checkSupport();

// ---------------------------------------------------------------------------
// Call
// ---------------------------------------------------------------------------

async function getMedia() {
  const audio = { echoCancellation: true, noiseSuppression: true, autoGainControl: true, channelCount: 1 };
  const video = { width: { ideal: 640 }, height: { ideal: 480 }, frameRate: { ideal: 30 } };
  // Degrade gracefully: a missing camera shouldn't block a voice call.
  for (const constraints of [{ audio, video }, { audio }, { video }]) {
    try {
      return await navigator.mediaDevices.getUserMedia(constraints);
    } catch (e) {
      console.warn("getUserMedia failed for", Object.keys(constraints), e.name);
    }
  }
  return new MediaStream();
}

class Call {
  static async join({ name, room, relay }) {
    setStatus("connecting", "Connecting…");
    // Created inside the click handler so audio may autoplay.
    const audioContext = new AudioContext({ sampleRate: 48_000, latencyHint: "interactive" });
    await audioContext.audioWorklet.addModule("js/media/audio-worklet.js");

    const media = await getMedia();
    const fingerprint = await fetch("/fingerprint")
      .then((r) => (r.ok ? r.text() : undefined))
      .catch(() => undefined);

    const session = await Session.connect(relay, { fingerprint });
    const c = new Call({ name, room, session, audioContext, media });
    try {
      await c.#start();
    } catch (e) {
      c.leave(false);
      throw e;
    }
    return c;
  }

  constructor({ name, room, session, audioContext, media }) {
    this.name = name;
    this.room = room;
    this.session = session;
    this.audioContext = audioContext;
    this.media = media;
    this.peerId = crypto.randomUUID().slice(0, 8);
    this.namespace = [APP, room, this.peerId];
    this.remote = null; // { id, peer: RemotePeer, info }
    this.stats = { last: performance.now(), prev: new Map() };
  }

  async #start() {
    this.broadcast = new Broadcast(this.session, this.namespace, { name: this.name, audioContext: this.audioContext });
    const [cam] = this.media.getVideoTracks();
    const [mic] = this.media.getAudioTracks();
    if (cam) this.broadcast.startCamera(cam);
    if (mic) this.broadcast.startMic(mic);

    try {
      this.announcement = await this.session.publishNamespace(this.namespace);
    } catch (e) {
      if (e instanceof RequestError && e.code === RequestErrorCode.EXCESSIVE_LOAD) {
        throw new Error(`Room “${this.room}” is full: ${e.message}. Try another room name.`);
      }
      throw e;
    }

    this.discovery = await this.session.subscribeNamespace([APP, this.room], {
      onNamespace: ([peerId]) => this.#addPeer(peerId),
      onNamespaceDone: ([peerId]) => this.#removePeer(peerId),
    });

    this.session.closed.then((info) => {
      if (call !== this) return;
      setStatus("error", "Disconnected");
      toast(`Connection closed${info?.reason ? `: ${info.reason}` : ""}`, 5000);
      this.leave(false);
    });

    this.#showCallUi(cam, mic);
    setStatus("connected", `Room “${this.room}” · connected`);
    this.statsTimer = setInterval(() => this.#renderStats(), 1000);
  }

  #showCallUi(cam, mic) {
    ui.lobby.hidden = true;
    ui.call.hidden = false;
    $("local-video").srcObject = cam ? new MediaStream([cam]) : null;
    $("local-name").textContent = `${this.name || "You"} (you)`;
    $("local-avatar").textContent = initials(this.name);
    $("local-placeholder").hidden = !!cam;
    $("local-mic").classList.toggle("on", !!mic);
    $("mic-btn").setAttribute("aria-pressed", String(!!mic));
    $("cam-btn").setAttribute("aria-pressed", String(!!cam));
    $("mic-btn").disabled = !mic;
    $("cam-btn").disabled = !cam;

    $("mic-btn").onclick = () => this.#toggle("mic-btn", (on) => {
      this.broadcast.setMicEnabled(on);
      $("local-mic").classList.toggle("on", on);
    });
    $("cam-btn").onclick = () => this.#toggle("cam-btn", (on) => {
      this.broadcast.setCameraEnabled(on);
      $("local-placeholder").hidden = on;
    });
    $("screen-btn").onclick = () => this.#toggleScreen();
    $("invite-btn").onclick = async () => {
      const url = `${location.origin}${location.pathname}?room=${encodeURIComponent(this.room)}`;
      try {
        await navigator.clipboard.writeText(url);
        toast("Invite link copied — open it in another tab");
      } catch {
        toast(url, 6000);
      }
    };
    $("leave-btn").onclick = () => this.leave();
  }

  #toggle(id, apply) {
    const btn = $(id);
    const on = btn.getAttribute("aria-pressed") !== "true";
    btn.setAttribute("aria-pressed", String(on));
    apply(on);
  }

  async #toggleScreen() {
    const btn = $("screen-btn");
    if (this.screenTrack) {
      this.#stopScreen();
      return;
    }
    try {
      const stream = await navigator.mediaDevices.getDisplayMedia({ video: { frameRate: 15 }, audio: false });
      const [track] = stream.getVideoTracks();
      track.contentHint = "detail";
      this.screenTrack = track;
      track.addEventListener("ended", () => this.#stopScreen()); // browser "Stop sharing"
      this.broadcast.startScreen(track);
      $("local-screen").srcObject = new MediaStream([track]);
      $("local-screen-tile").hidden = false;
      btn.setAttribute("aria-pressed", "true");
      this.#layout();
    } catch (e) {
      if (e.name !== "NotAllowedError") toast(`Screen share failed: ${e.message}`);
    }
  }

  #stopScreen() {
    if (!this.screenTrack) return;
    this.broadcast.stopScreen();
    this.screenTrack = null;
    $("local-screen").srcObject = null;
    $("local-screen-tile").hidden = true;
    $("screen-btn").setAttribute("aria-pressed", "false");
    this.#layout();
  }

  #layout() {
    const sharing = !$("local-screen-tile").hidden || !$("remote-screen-tile").hidden;
    ui.stage.classList.toggle("has-screen", sharing);
  }

  async #addPeer(peerId) {
    if (peerId === this.peerId) return;
    if (this.remote) {
      console.warn(`ignoring extra participant ${peerId}: this UI shows one remote peer`);
      return;
    }
    const namespace = [APP, this.room, peerId];
    const peer = new RemotePeer(this.session, namespace, {
      audioContext: this.audioContext,
      video: $("remote-video"),
      screen: $("remote-screen"),
      onCatalog: (info) => this.#onRemoteCatalog(info),
      onAudioLevel: (peak) => {
        if (peak <= 0.05) return;
        // Ring on while audio is loud; fades out shortly after it stops.
        $("remote-mic").classList.add("speaking");
        clearTimeout(this.speakingTimer);
        this.speakingTimer = setTimeout(() => $("remote-mic").classList.remove("speaking"), 300);
      },
    });
    this.remote = { id: peerId, peer, info: null };
    $("remote-waiting").textContent = "Connecting…";
    await peer.start();
    toast("Participant joined");
  }

  #onRemoteCatalog(info) {
    if (!this.remote) return;
    this.remote.info = info;
    const { tracks } = info;
    for (const el of document.querySelectorAll("[data-remote-name]")) el.textContent = info.name || "Guest";
    $("remote-avatar").textContent = initials(info.name);
    delete $("remote-placeholder").dataset.waiting;
    $("remote-waiting").textContent = tracks.video?.active ? "" : "Camera off";
    $("remote-placeholder").hidden = !!tracks.video?.active;
    $("remote-mic").classList.toggle("on", !!tracks.audio?.active);
    $("remote-screen-tile").hidden = !tracks.screen?.active;
    this.#layout();
  }

  #removePeer(peerId) {
    if (this.remote?.id !== peerId) return;
    this.remote.peer.close();
    this.remote = null;
    toast("Participant left");
    for (const el of document.querySelectorAll("[data-remote-name]")) el.textContent = "Peer";
    for (const id of ["remote-video", "remote-screen"]) {
      const c = $(id);
      c.getContext("2d").clearRect(0, 0, c.width, c.height);
    }
    $("remote-avatar").textContent = "?";
    $("remote-placeholder").dataset.waiting = "";
    $("remote-waiting").textContent = "Waiting for someone to join…";
    $("remote-placeholder").hidden = false;
    $("remote-mic").classList.remove("on", "speaking");
    $("remote-screen-tile").hidden = true;
    this.#layout();
  }

  #renderStats() {
    const now = performance.now();
    const dt = (now - this.stats.last) / 1000;
    this.stats.last = now;
    const rate = (key, s) => {
      if (!s) return "";
      const prev = this.stats.prev.get(key) ?? { frames: s.frames, bytes: s.bytes };
      this.stats.prev.set(key, { frames: s.frames, bytes: s.bytes });
      const fps = Math.round((s.frames - prev.frames) / dt);
      const kbps = Math.round(((s.bytes - prev.bytes) * 8) / dt / 1000);
      return `${fps} fps · ${kbps} kbps`;
    };
    const local = this.broadcast.stats();
    $("local-video-stats").textContent = rate("lv", local.video);
    $("local-screen-stats").textContent = rate("ls", local.screen);
    if (this.remote) {
      const r = this.remote.peer.stats();
      const lost = r.audio.lost ? ` · audio lost ${r.audio.lost}` : "";
      $("remote-video-stats").textContent = rate("rv", r.video) + lost;
      $("remote-screen-stats").textContent = rate("rs", r.screen);
    }
  }

  leave(closeSession = true) {
    if (call === this) call = null;
    clearInterval(this.statsTimer);
    this.remote?.peer.close();
    this.remote = null;
    this.broadcast?.close();
    this.announcement?.close();
    this.discovery?.close();
    if (closeSession) this.session.close(0, "left");
    for (const t of this.media.getTracks()) t.stop();
    this.screenTrack?.stop();
    this.audioContext.close().catch(() => {});
    $("local-video").srcObject = null;
    ui.call.hidden = true;
    ui.lobby.hidden = false;
    ui.stage.classList.remove("has-screen");
    if (closeSession) setStatus("idle", "Not connected");
  }
}

window.addEventListener("pagehide", () => call?.leave());

// Debug handle for the console / automated tests: `moqi.call.remote.peer.stats()`.
globalThis.moqi = {
  get call() {
    return call;
  },
};
