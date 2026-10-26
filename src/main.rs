use axum::{
    Router,
    extract::{
        State,
        ws::{Message, WebSocket, WebSocketUpgrade},
    },
    response::IntoResponse,
    routing::get,
};
use futures_util::{SinkExt, StreamExt};
use serde::{Deserialize, Serialize};
use tokio::sync::broadcast;

const BROADCAST_CAPACITY: usize = 100;

#[derive(Clone)]
struct AppState {
    messages: broadcast::Sender<ChatMessage>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
struct ChatMessage {
    username: String,
    message: String,
}

#[tokio::main]
async fn main() {
    let (messages, _) = broadcast::channel(BROADCAST_CAPACITY);
    let app = Router::new()
        .route("/ws", get(websocket_handler))
        .with_state(AppState { messages });

    let listener = tokio::net::TcpListener::bind("127.0.0.1:3000")
        .await
        .expect("failed to bind server to 127.0.0.1:3000");

    println!("WebSocket server listening at ws://127.0.0.1:3000/ws");
    axum::serve(listener, app)
        .await
        .expect("WebSocket server failed");
}

async fn websocket_handler(
    ws: WebSocketUpgrade,
    State(state): State<AppState>,
) -> impl IntoResponse {
    ws.on_upgrade(move |socket| handle_socket(socket, state))
}

async fn handle_socket(socket: WebSocket, state: AppState) {
    let (mut sender, mut receiver) = socket.split();
    let mut messages = state.messages.subscribe();

    loop {
        tokio::select! {
            incoming = receiver.next() => match incoming {
                Some(Ok(Message::Text(text))) => {
                    if let Ok(message) = serde_json::from_str::<ChatMessage>(&text) {
                        let _ = state.messages.send(message);
                    }
                }
                Some(Ok(Message::Close(_))) | None | Some(Err(_)) => break,
                Some(Ok(_)) => {}
            },
            outgoing = messages.recv() => match outgoing {
                Ok(message) => {
                    let text = serde_json::to_string(&message)
                        .expect("ChatMessage should always serialize");

                    if sender.send(Message::Text(text.into())).await.is_err() {
                        break;
                    }
                }
                Err(broadcast::error::RecvError::Lagged(_)) => continue,
                Err(broadcast::error::RecvError::Closed) => break,
            },
        }
    }
}
