use axum::{Router, routing::get};
use tokio::sync::broadcast;
use tracing::info;

mod state;
mod websocket;

use state::AppState;
use websocket::websocket_handler;

const BROADCAST_CAPACITY: usize = 100;

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::INFO)
        .with_target(false)
        .with_writer(std::io::stdout)
        .init();

    let (messages, _) = broadcast::channel(BROADCAST_CAPACITY);
    let app = Router::new()
        .route("/ws", get(websocket_handler))
        .with_state(AppState { messages });

    let listener = tokio::net::TcpListener::bind("0.0.0.0:3000")
        .await
        .expect("failed to bind server to 0.0.0.0:3000");

    info!("WebSocket server listening on port 3000");
    axum::serve(listener, app)
        .await
        .expect("WebSocket server failed");
}
