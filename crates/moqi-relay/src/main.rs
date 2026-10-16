//! MoQ Transport (draft-ietf-moq-transport-22) relay over WebTransport.
//!
//! - UDP `--bind`: WebTransport over HTTP/3 (RFC 9220 extended CONNECT), MoQT on top.
//! - TCP `--http`: serves the browser demo and `/fingerprint` (the self-signed
//!   certificate's SHA-256 for `serverCertificateHashes`).

mod error;
mod io;
mod relay;
mod session;
mod track;

use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use anyhow::Context;
use clap::Parser;
use sha2::{Digest, Sha256};
use tower_http::cors::CorsLayer;
use tower_http::services::ServeDir;
use tracing::Instrument;
use wtransport::config::QuicTransportConfig;
use wtransport::endpoint::IncomingSession;
use wtransport::quinn::VarInt as QuicVarInt;
use wtransport::{Endpoint, Identity, ServerConfig};

use crate::relay::{Relay, RelayConfig};
use crate::session::Session;

#[derive(Parser, Debug)]
#[command(version, about)]
struct Cli {
    /// UDP address for WebTransport / MoQT.
    #[arg(long, env = "MOQ_BIND", default_value = "[::]:4443")]
    bind: SocketAddr,

    /// TCP address for the demo web client and certificate fingerprint.
    #[arg(long, env = "MOQ_HTTP", default_value = "[::]:8080")]
    http: SocketAddr,

    /// Directory with the web client.
    #[arg(long, env = "MOQ_WEB_DIR", default_value = "web")]
    web_dir: PathBuf,

    /// Maximum publishers per room (namespace prefix of `--room-depth` fields).
    #[arg(long, env = "MOQ_MAX_PEERS", default_value_t = 2)]
    max_peers: usize,

    /// Number of namespace fields identifying a room: ("moqi", <room>) = 2.
    #[arg(long, default_value_t = 2)]
    room_depth: usize,

    /// PEM certificate chain. Without it a 10-day self-signed cert is generated.
    #[arg(long, requires = "key")]
    cert: Option<PathBuf>,

    /// PEM private key.
    #[arg(long, requires = "cert")]
    key: Option<PathBuf>,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "info,moqi_relay=info".into()),
        )
        .init();

    let cli = Cli::parse();

    let identity = match (&cli.cert, &cli.key) {
        (Some(cert), Some(key)) => Identity::load_pemfiles(cert, key).await.context("loading certificate")?,
        // Chrome only accepts serverCertificateHashes for ECDSA certs valid < 14 days.
        _ => Identity::self_signed_builder()
            .subject_alt_names(["localhost", "127.0.0.1", "::1"])
            .from_now_utc()
            .validity_days(10)
            .build()
            .context("generating self-signed certificate")?,
    };
    let fingerprint = hex(&Sha256::digest(identity.certificate_chain().as_slice()[0].der()));
    tracing::info!(%fingerprint, "certificate SHA-256");

    let mut transport = QuicTransportConfig::default();
    transport
        .max_concurrent_uni_streams(QuicVarInt::from_u32(1024))
        .max_concurrent_bidi_streams(QuicVarInt::from_u32(256))
        .keep_alive_interval(Some(Duration::from_secs(4)))
        .max_idle_timeout(Some(Duration::from_secs(30).try_into()?));

    let config = ServerConfig::builder()
        .with_bind_address(cli.bind)
        .with_custom_transport(identity, transport)
        .build();
    let endpoint = Endpoint::server(config).context("binding WebTransport endpoint")?;

    let relay = Relay::new(RelayConfig { room_depth: cli.room_depth, max_peers_per_room: cli.max_peers });

    tokio::spawn(serve_http(cli.http, cli.web_dir.clone(), fingerprint));
    tracing::info!(addr = %cli.bind, max_peers = cli.max_peers, "MoQT relay listening (WebTransport)");
    tracing::info!("open http://localhost:{} in two tabs", cli.http.port());

    let mut next_id = 0u64;
    loop {
        let incoming = endpoint.accept().await;
        let id = next_id;
        next_id += 1;
        let relay = relay.clone();
        tokio::spawn(
            async move {
                if let Err(e) = accept(relay, incoming, id).await {
                    tracing::warn!("handshake failed: {e:#}");
                }
            }
            .instrument(tracing::info_span!("session", id)),
        );
    }
}

async fn accept(relay: Arc<Relay>, incoming: IncomingSession, id: u64) -> anyhow::Result<()> {
    let request = incoming.await?;
    tracing::info!(authority = request.authority(), path = request.path(), "WebTransport CONNECT");

    // Version negotiation via WT-Available-Protocols (§6.2): an SF list of strings.
    let offered = request.headers().iter().find(|(k, _)| k.eq_ignore_ascii_case("wt-available-protocols"));
    let mut headers = Vec::new();
    if let Some((_, value)) = offered {
        let protocols: Vec<&str> = value.split(',').map(|p| p.trim().trim_matches('"')).collect();
        if !protocols.contains(&moqi_proto::ALPN) {
            tracing::warn!(?protocols, "client offered no supported MoQT version");
            request.forbidden().await;
            return Ok(());
        }
        headers.push(("wt-protocol", format!("\"{}\"", moqi_proto::ALPN)));
    }

    let conn = request.accept_with_headers(headers).await?;
    tracing::info!(remote = %conn.remote_address(), "session established");
    Session::run(relay, conn, id).await;
    Ok(())
}

async fn serve_http(addr: SocketAddr, web_dir: PathBuf, fingerprint: String) {
    use axum::routing::get;
    let app = axum::Router::new()
        .route("/fingerprint", get(move || async move { fingerprint }))
        .fallback_service(ServeDir::new(web_dir))
        .layer(CorsLayer::permissive());
    let listener = match tokio::net::TcpListener::bind(addr).await {
        Ok(l) => l,
        Err(e) => {
            tracing::error!(%addr, "HTTP bind failed: {e}");
            return;
        }
    };
    tracing::info!(%addr, "HTTP server listening");
    if let Err(e) = axum::serve(listener, app).await {
        tracing::error!("HTTP server failed: {e}");
    }
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}
