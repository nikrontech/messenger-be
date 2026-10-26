use serde::{Deserialize, Serialize};
use tokio::sync::broadcast;

#[derive(Clone)]
pub(crate) struct AppState {
    pub(crate) messages: broadcast::Sender<ChatMessage>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub(crate) struct ChatMessage {
    pub(crate) username: String,
    pub(crate) message: String,
}
