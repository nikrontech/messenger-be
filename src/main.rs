use axum::{Router, routing::get};
use sqlx::sqlite::{SqliteConnectOptions, SqlitePoolOptions};
use std::path::Path;
use std::str::FromStr;
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
    let database_url =
        std::env::var("DATABASE_URL").unwrap_or_else(|_| "sqlite://messages.db".to_owned());

    // SQLite does not create missing parent directories. Support both relative
    // paths (sqlite://data/messages.db) and absolute paths (sqlite:///data/messages.db).
    if let Some(path) = database_url.strip_prefix("sqlite://") {
        let path = path.split('?').next().unwrap_or(path);
        let database_path = Path::new(path);
        if let Some(parent) = database_path.parent() {
            if parent != Path::new("") && parent != Path::new(".") {
                std::fs::create_dir_all(parent).unwrap_or_else(|error| {
                    panic!("failed to create SQLite directory {parent:?}: {error}")
                });
            }
        }
    }

    let database_options = SqliteConnectOptions::from_str(&database_url)
        .unwrap_or_else(|error| panic!("invalid SQLite database URL: {error}"))
        .create_if_missing(true);
    let database = SqlitePoolOptions::new()
        .max_connections(5)
        .connect_with(database_options)
        .await
        .unwrap_or_else(|error| panic!("failed to connect to SQLite database: {error}"));

    sqlx::query(
        "CREATE TABLE IF NOT EXISTS messages (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            username TEXT NOT NULL,
            message TEXT NOT NULL,
            created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
        )",
    )
    .execute(&database)
    .await
    .unwrap_or_else(|error| panic!("failed to initialize SQLite database: {error}"));

    info!(database_url = %database_url, "SQLite message history enabled");
    let app = Router::new()
        .route("/ws", get(websocket_handler))
        .with_state(AppState { messages, database });

    let listener = tokio::net::TcpListener::bind("0.0.0.0:3000")
        .await
        .expect("failed to bind server to 0.0.0.0:3000");

    info!("WebSocket server listening on port 3000");
    axum::serve(listener, app)
        .await
        .expect("WebSocket server failed");
}
