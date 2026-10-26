use serde::{Deserialize, Serialize};
use sqlx::SqlitePool;
use tokio::sync::broadcast;

#[derive(Clone)]
pub(crate) struct AppState {
    pub(crate) messages: broadcast::Sender<ChatMessage>,
    pub(crate) database: SqlitePool,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub(crate) struct ChatMessage {
    pub(crate) username: String,
    pub(crate) message: String,
}
