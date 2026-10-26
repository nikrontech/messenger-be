use axum::{
    extract::{
        State,
        ws::{Message, WebSocket, WebSocketUpgrade},
    },
    response::IntoResponse,
};
use futures_util::{SinkExt, StreamExt};
use tokio::sync::broadcast;
use tracing::{debug, error, info, warn};

use crate::state::{AppState, ChatMessage};

pub(crate) async fn websocket_handler(
    ws: WebSocketUpgrade,
    State(state): State<AppState>,
) -> impl IntoResponse {
    ws.on_upgrade(move |socket| handle_socket(socket, state))
}

async fn handle_socket(socket: WebSocket, state: AppState) {
    info!("WebSocket client connected");

    let (mut sender, mut receiver) = socket.split();
    let mut messages = state.messages.subscribe();

    loop {
        tokio::select! {
            incoming = receiver.next() => match incoming {
                Some(Ok(Message::Text(text))) => {
                    match serde_json::from_str::<ChatMessage>(&text) {
                        Ok(message) => {
                            info!(username = %message.username, "Broadcasting message");
                            if let Err(error) = state.messages.send(message) {
                                debug!(?error, "No clients available to receive message");
                            }
                        }
                        Err(error) => {
                            warn!(?error, "Ignoring malformed chat message");
                        }
                    }
                }
                Some(Ok(Message::Close(_))) => {
                    info!("WebSocket client requested disconnect");
                    break;
                }
                None => {
                    info!("WebSocket client disconnected");
                    break;
                }
                Some(Err(error)) => {
                    warn!(?error, "WebSocket receive error");
                    break;
                }
                Some(Ok(_)) => {}
            },
            outgoing = messages.recv() => match outgoing {
                Ok(message) => {
                    let text = serde_json::to_string(&message)
                        .expect("ChatMessage should always serialize");

                    if sender.send(Message::Text(text.into())).await.is_err() {
                        warn!("Failed to send message to WebSocket client");
                        break;
                    }
                }
                Err(broadcast::error::RecvError::Lagged(count)) => {
                    warn!(count, "WebSocket client lagged behind broadcast channel");
                }
                Err(broadcast::error::RecvError::Closed) => {
                    error!("Broadcast channel closed");
                    break;
                }
            },
        }
    }
}
