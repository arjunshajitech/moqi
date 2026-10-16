<p align="center">
  <img src="web/moqi.svg" alt="moqi mascot" width="140" />
</p>

<h1 align="center">moqi</h1>

<p align="center"><b>Video calls over Media over QUIC (MoQ)</b></p>

moqi has two parts:
- **A relay server, written in Rust.** It implements the MoQ Transport protocol ([draft-ietf-moq-transport-22](https://datatracker.ietf.org/doc/draft-ietf-moq-transport/)) over WebTransport/QUIC.
- **A web page.** Two browser tabs use it to share camera, microphone and screen through the relay.

Each room allows 2 participants by default.

## Requirements

- **Rust**: install from <https://rustup.rs>.
- **Chrome or Edge**: needed for WebTransport and WebCodecs.
- **Node.js**: only needed to run the tests.

## Build

```bash
cargo build --release
```

This produces `target/release/moqi-relay`.

## Run

```bash
./target/release/moqi-relay
```

Or build and run in one step:

```bash
cargo run --release -p moqi-relay
```

Then:

1. Open <http://localhost:8080/?room=demo> in **two tabs**.
2. Type a name in each tab and click **Join**.
3. Use the buttons at the bottom: 🎤 mute · 📷 camera · 🖥️ share screen · 🔗 copy invite link · ☎️ leave.

A third tab in the same room is told *"room is full"*.

> Tip: use headphones, or mute one tab, to avoid echo.

### Options

| Flag | Default | Meaning |
|---|---|---|
| `--max-peers` | `2` | People allowed per room |
| `--bind` | `[::]:4443` | UDP port for MoQ (WebTransport) |
| `--http` | `[::]:8080` | Port for the web page |
| `--web-dir` | `web` | Folder with the web page |
| `--cert`, `--key` | none | Your own TLS certificate. Without them, a temporary self-signed one is created. |

Example: `./target/release/moqi-relay --max-peers 4`

## Test

```bash
cargo test              # Rust unit tests
npm install
npm run test:interop    # checks the browser and Rust codecs encode identically
npm run test:e2e        # two headless Chrome tabs make a real call (relay must be running)
```

## Project layout

```
crates/moqi-proto   MoQ wire format (messages, streams, datagrams); no networking
crates/moqi-relay   The relay server
web/                The web page (plain HTML/CSS/JS, no build step)
web/test            Interop and end-to-end tests
```

## How it works

1. Each person **publishes** their tracks under `moqi/<room>/<id>`:
   - `catalog`: their name, plus which of camera, mic and screen are on
   - `audio`: Opus over QUIC datagrams
   - `video`: VP8 over QUIC streams, one stream per keyframe group
   - `screen`: VP8 over QUIC streams, one stream per keyframe group
2. Each person **subscribes** to the room. The relay tells them when someone joins or leaves.
3. When someone joins, they **subscribe** to the other person's tracks. The relay forwards the media.

The relay subscribes to each track only once, however many viewers there are, and lets each viewer read at their own pace.
