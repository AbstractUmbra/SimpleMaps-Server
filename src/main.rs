// For clarity I'm not very good at Rust, so if you spot something glaringly bad, I'm sorry.

use std::{
    collections::{BTreeMap, HashSet},
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::Duration,
};
use tokio_util::sync::CancellationToken;

use axum::{
    Router,
    extract::{
        State,
        ws::{Message, WebSocket, WebSocketUpgrade},
    },
    response::Response,
    routing::get,
};
use futures_util::{SinkExt, StreamExt};
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use tokio::{
    sync::{Mutex, Notify},
    time::{self, Instant},
};
use tracing::{debug, error, info, warn};

const HEARTBEAT_INTERVAL: Duration = Duration::from_secs(10);
const FORCED_HEARTBEAT_DELAY: Duration = Duration::from_secs(1);
const MAX_PARTY_SIZE: usize = 7;

type SharedRegistry = Arc<Mutex<BTreeMap<u64, ClientEntry>>>;

#[derive(Clone, Debug, Default)]
struct ClientState {
    user_key: String,
    map_type: f64,
    map_spot: f64,
    party: Vec<String>,
}

struct ClientEntry {
    state: ClientState,
    force_notify: Arc<Notify>,
}

#[derive(Clone)]
struct AppState {
    connections: SharedRegistry,
    next_id: Arc<AtomicU64>,
    shutdown: CancellationToken,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct IncomingMessage {
    user: String,
    party: Map<String, Value>,
    map_type: f64,
    map_spot: f64,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct PartyMemberStatus {
    map_type: f64,
    map_spot: f64,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct OutgoingMessage {
    user: String,
    map_type: f64,
    map_spot: f64,
    party: BTreeMap<String, PartyMemberStatus>,
}

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();

    let shutdown = CancellationToken::new();

    let state = AppState {
        connections: Arc::new(Mutex::new(BTreeMap::new())),
        next_id: Arc::new(AtomicU64::new(1)),
        shutdown: shutdown.clone(),
    };

    let app = Router::new()
        .route("/", get(websocket_handler))
        .with_state(state);

    let listener = tokio::net::TcpListener::bind("0.0.0.0:44333")
        .await
        .expect("Failed to bind port 44333");

    info!("WebSocket server listening on 0.0.0.0:44333");

    axum::serve(listener, app)
        .with_graceful_shutdown(shutdown_signal(shutdown))
        .await
        .expect("HTTP server failed");
}

async fn shutdown_signal(shutdown: CancellationToken) {
    #[cfg(unix)]
    {
        use tokio::signal::unix::{SignalKind, signal};

        let mut terminate =
            signal(SignalKind::terminate()).expect("Failed to install SIGTERM handler");

        tokio::select! {
            result = tokio::signal::ctrl_c() => {
                if let Err(error) = result {
                    tracing::error!(%error, "Failed to listen for Ctrl+C");
                }
            }
            _ = terminate.recv() => {}
        }
    }

    #[cfg(not(unix))]
    {
        if let Err(error) = tokio::signal::ctrl_c().await {
            tracing::error!(%error, "Failed to listen for Ctrl+C");
        }
    }

    tracing::info!("Shutdown signal received");
    shutdown.cancel();
}

async fn websocket_handler(ws: WebSocketUpgrade, State(state): State<AppState>) -> Response {
    ws.max_message_size(16 * 1024)
        .max_frame_size(16 * 1024)
        .on_upgrade(move |socket| handle_connection(socket, state))
}

async fn handle_connection(socket: WebSocket, app: AppState) {
    let id = app.next_id.fetch_add(1, Ordering::Relaxed);
    let force_notify = Arc::new(Notify::new());

    {
        let mut connections = app.connections.lock().await;

        connections.insert(
            id,
            ClientEntry {
                state: ClientState::default(),
                force_notify: force_notify.clone(),
            },
        );
    }

    info!(id, "New connection");

    let (mut sender, mut receiver) = socket.split();

    let mut heartbeat = time::interval(HEARTBEAT_INTERVAL);
    heartbeat.set_missed_tick_behavior(time::MissedTickBehavior::Skip);

    // The first interval tick is immediate.
    let mut force_deadline: Option<Instant> = None;

    // Track whether the loop exited because of shutdown.
    let mut shutdown_requested = false;

    loop {
        // Copy the deadline so the timer future can own it.
        let deadline = force_deadline;

        tokio::select! {
            message = receiver.next() => {
                match message {
                    Some(Ok(Message::Text(text))) => {
                        debug!(payload = %text, "Received websocket payload.");

                        let incoming: IncomingMessage =
                            match serde_json::from_str::<IncomingMessage>(
                                text.as_str()
                            ) {
                                Ok(message) => message,
                                Err(err) => {
                                    warn!(id, %err, "Invalid JSON message");
                                    let _ = sender
                                        .send(Message::Close(None))
                                        .await;
                                    break;
                                }
                            };

                        let Some((is_empty_user, peers)) =
                            update_client(
                                &app.connections,
                                id,
                                incoming,
                            )
                            .await
                        else {
                            warn!(id, "Invalid data message or missing connection");
                            let _ = sender
                                .send(Message::Close(None))
                                .await;
                            break;
                        };

                        if !is_empty_user {
                            // Reset the forced-heartbeat deadline.
                            force_deadline =
                                Some(Instant::now() + FORCED_HEARTBEAT_DELAY);

                            // Wake reciprocal party members.
                            for peer_notify in peers {
                                peer_notify.notify_one();
                            }
                        }
                    }

                    Some(Ok(Message::Ping(payload))) => {
                        if sender
                            .send(Message::Pong(payload))
                            .await
                            .is_err()
                        {
                            break;
                        }
                    }

                    Some(Ok(Message::Close(_))) | None => {
                        break;
                    }

                    Some(Ok(Message::Pong(_))) => {}

                    Some(Ok(_)) => {
                        // Ignore unsupported message types.
                    }

                    Some(Err(err)) => {
                        error!(id, %err, "WebSocket error");
                        break;
                    }
                }
            }

            _ = heartbeat.tick() => {
                if !send_heartbeat(
                    &app.connections,
                    id,
                    &mut sender,
                )
                .await
                {
                    break;
                }
            }

            _ = force_notify.notified() => {
                // Notify is a coalescing signal, not a work queue.
                force_deadline =
                    Some(Instant::now() + FORCED_HEARTBEAT_DELAY);
            }

            _ = async move {
                match deadline {
                    Some(deadline) => time::sleep_until(deadline).await,
                    None => std::future::pending::<()>().await,
                }
            } => {
                force_deadline = None;

                if !send_heartbeat(
                    &app.connections,
                    id,
                    &mut sender,
                )
                .await
                {
                    break;
                }
            }

            _ = app.shutdown.cancelled() => {
                info!(id, "Closing WebSocket for shutdown");
                shutdown_requested = true;
                break;
            }
        }
    }

    if shutdown_requested {
        let _ = time::timeout(
            std::time::Duration::from_secs(1),
            sender.send(Message::Close(None)),
        )
        .await;
    }

    // Remove the connection immediately on disconnect.
    app.connections.lock().await.remove(&id);

    info!(id, "Disconnected");
}

async fn update_client(
    registry: &SharedRegistry,
    id: u64,
    message: IncomingMessage,
) -> Option<(bool, Vec<Arc<Notify>>)> {
    // Validate before modifying the connection state.
    if !(message.user.is_empty() || message.user.len() == 64)
        || !valid_map_value(message.map_type)
        || !valid_map_value(message.map_spot)
        || message.party.len() > MAX_PARTY_SIZE
        || message.party.keys().any(|key| key.len() != 64)
    {
        return None;
    }

    let is_empty_user = message.user.is_empty();

    let new_party: Vec<String> = message.party.into_iter().map(|(key, _)| key).collect();

    let mut registry = registry.lock().await;

    let entry = registry.get_mut(&id)?;

    entry.state.user_key = message.user;

    if is_empty_user {
        entry.state.map_type = 0.0;
        entry.state.map_spot = 0.0;
        entry.state.party.clear();

        // The original implementation doesn't force a heartbeat
        // when the user key is empty.
        return Some((true, Vec::new()));
    }

    entry.state.map_type = message.map_type;
    entry.state.map_spot = message.map_spot;
    entry.state.party = new_party;

    let user_key = entry.state.user_key.clone();
    let party: HashSet<String> = entry.state.party.iter().cloned().collect();

    // Find all connections that mutually list each other.
    let peers = registry
        .iter()
        .filter(|(other_id, other)| {
            **other_id != id
                && party.contains(&other.state.user_key)
                && other.state.party.contains(&user_key)
        })
        .map(|(_, other)| other.force_notify.clone())
        .collect();

    Some((false, peers))
}

fn valid_map_value(value: f64) -> bool {
    value.is_finite() && (0.0..=99.0).contains(&value)
}

async fn send_heartbeat(
    registry: &SharedRegistry,
    id: u64,
    sender: &mut futures_util::stream::SplitSink<WebSocket, Message>,
) -> bool {
    // Build a consistent snapshot under the lock. Don't hold the lock
    // while performing the network write.
    let outgoing = {
        let connections = registry.lock().await;

        let Some(entry) = connections.get(&id) else {
            return false;
        };

        let state = &entry.state;
        let mut party_status = BTreeMap::new();

        for member_key in state.party.iter().take(MAX_PARTY_SIZE) {
            let matching = connections.values().find(|candidate| {
                candidate.state.user_key == *member_key
                    && candidate.state.party.contains(&state.user_key)
            });

            let status = matching
                .map(|candidate| PartyMemberStatus {
                    map_type: candidate.state.map_type,
                    map_spot: candidate.state.map_spot,
                })
                .unwrap_or(PartyMemberStatus {
                    map_type: 0.0,
                    map_spot: 0.0,
                });

            party_status.insert(member_key.clone(), status);
        }

        OutgoingMessage {
            user: state.user_key.clone(),
            map_type: state.map_type,
            map_spot: state.map_spot,
            party: party_status,
        }
    };

    match serde_json::to_string(&outgoing) {
        Ok(payload) => sender.send(Message::Text(payload.into())).await.is_ok(),

        Err(err) => {
            error!(id, %err, "Failed to serialize heartbeat");
            false
        }
    }
}
