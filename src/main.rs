#![allow(unused)]

use std::{collections::HashMap, os::macos::raw::stat, sync::{Arc, RwLock}, time::Duration};

use axum::{Router, extract::{State, WebSocketUpgrade, ws::{Message as AxumMessage, WebSocket}}, response::IntoResponse, routing::get};
use futures::{SinkExt, StreamExt, stream::SplitSink};
use serde::{Deserialize, Serialize};
use tokio::{net::TcpListener, sync::{broadcast, mpsc}, task, time::sleep};
use tracing::{error, info, warn}; 


#[tokio::main]
async fn main() {
    tracing_subscriber::fmt().with_max_level(tracing::Level::INFO).init();

    let (tick_tx, _) = broadcast::channel(100);
    let state = AppState {
        store: Arc::new(RwLock::new(MarketDataStore::default())),
        tick_tx: tick_tx.clone(),
    };

    tokio::task::spawn(start_broadcast_listener(state.clone()));
    tokio::task::spawn(simulate_vendor_stream(tick_tx));

    let router = Router::new()
        .route("/ws", get(ws_handler))
        .with_state(state);

    let listener = TcpListener::bind("0.0.0.0:8080").await.unwrap();
    axum::serve(listener, router).await.unwrap();
}

async fn ws_handler(ws: WebSocketUpgrade, State(state): State<AppState>) -> impl IntoResponse {
    ws.on_upgrade(|socket| handle_client_connection(socket, state))
}

async fn handle_client_connection(socket: WebSocket, state: AppState) {
    let (ws_sender, mut ws_receiver) = socket.split();

    let client_id = {
        let mut write_gaurd = state.store.write().unwrap();
        write_gaurd.next_client_id += 1;
        write_gaurd.next_client_id
    };

    info!("New client connected: {client_id}");
    let (tx, rx) = mpsc::channel(10);

    task::spawn(client_writer(client_id, state.clone(), ws_sender, rx));

    while let Some(Ok(msg)) = ws_receiver.next().await {
        if let AxumMessage::Text(text) = msg {
            let msg: Message = match serde_json::from_str(&text) {
                Ok(m) => m,
                Err(e) => {
                    warn!("Client {client_id} sent invalid JSON: {e}. Message: {text}");
                    continue;
                }
            };

            let mut write_guard = state.store.write().unwrap();
            match msg.action.as_str() {
                "SUBSCRIBE" => {
                    for symbol in msg.symbols {
                        write_guard.add_subscription(&symbol, 
                            client_id, tx.clone());
                    }
                }
                "UNSUBSCRIBE" => {
                    for symbol in msg.symbols {
                        write_guard.remove_subscription(&symbol, client_id);
                    }
                }
                _ => {
                    warn!("Client {} sent unknown action: {}", client_id, msg.action);
                }
            }
        }
    }

    info!("Client handler for {} exiting. Dropping MPSC sender.", client_id);
}

/// Tick represents market data
#[derive(Debug, Clone, Serialize, Deserialize)] 
struct Tick {
    symbol: String,
    price: f64,
    time: i64,
}

/// Message represents client-server communication 
#[derive(Debug, Deserialize)] 
struct Message {
    action: String, 
    symbols: Vec<String>,
}

// ClientID is a simple way to track connections in the store
type ClientId = usize;

/// Client hold dedicated outbound channel (queue)
struct Client {
    send_queue: mpsc::Sender<Tick>,
}

/// MarketDataStore holds the map of symbols to client subscriptions
#[derive(Default)] 
struct MarketDataStore {
    symbol_subscriptions: HashMap<String, HashMap<ClientId, mpsc::Sender<Tick>>>,
    next_client_id: ClientId,
}

/// AppState holds all shared, mutable state for Axum handlers
#[derive(Clone)]
struct AppState {
    store: Arc<RwLock<MarketDataStore>>,
    tick_tx: broadcast::Sender<Tick>,
}

impl MarketDataStore {
    fn add_subscription(&mut self, symbol: &str, client_id: ClientId, send_queue: mpsc::Sender<Tick>) {
        info!("Client {client_id} subscribed to {symbol}");
        let clients = self.symbol_subscriptions.entry(symbol.to_string()).or_default();
        clients.insert(client_id, send_queue);
    }

    fn remove_subscription(&mut self, symbol: &str, client_id: ClientId) {
        if let Some(connections) = self.symbol_subscriptions.get_mut(symbol) {
            connections.remove(&client_id);
            if connections.is_empty() {
                self.symbol_subscriptions.remove(symbol);
                info!("All clients unsubscribed from {symbol}. Cleaning up map key");
            }
        }
    }

    fn cleanup_client(&mut self, client_id: ClientId) {
        let mut symbols_cleaned_up = 0;
        let symbols_to_clean: Vec<String> = self.symbol_subscriptions.keys().cloned().collect();

        for symbol in symbols_to_clean {
            if let Some(connections) = self.symbol_subscriptions.get_mut(&symbol) {
                if connections.remove(&client_id).is_some() {
                    symbols_cleaned_up += 1;
                    if connections.is_empty() {
                        self.symbol_subscriptions.remove(&symbol);
                    }
                }
            }
        }

        info!("Client {client_id} disconnected and cleaned up from {symbols_cleaned_up} symbols.");
    }
}

async fn client_writer(
    client_id: ClientId, 
    state: AppState, 
    mut ws_sender: SplitSink<WebSocket, AxumMessage>, 
    mut rx: mpsc::Receiver<Tick>
) {
    while let Some(tick) = rx.recv().await {
        let json_tick = match serde_json::to_string(&tick) {
            Ok(json) => json,
            Err(e) => {
                error!("Failed to serialize tick for client {client_id}: {e}");
                continue;
            }
        };

        if let Err(e) = ws_sender.send(AxumMessage::Text(json_tick.into())).await {
            warn!("Error writing to client {}: {}. Cleaning up...", client_id, e);
            break; // Exit the client_writer task
        }
    }

    info!("Client writer task for {} exiting. Starting cleanup.", client_id);
    let mut write_guard = state.store.write().unwrap();
    write_guard.cleanup_client(client_id);
}

async fn start_broadcast_listener(state: AppState) {
    let mut rx = state.tick_tx.subscribe();
    info!("Broadcast listener started.");

    while let Ok(tick) = rx.recv().await {
        let read_gaurd = state.store.read().unwrap();

        if let Some(connections) = read_gaurd.symbol_subscriptions.get(&tick.symbol) {
            let clients_to_send: Vec<mpsc::Sender<Tick>> = connections.values().cloned().collect();
            drop(read_gaurd);

            for sender in clients_to_send {
                match sender.try_send(tick.clone()) {
                    Ok(_) => {
                        
                    }
                    Err(mpsc::error::TrySendError::Full(_)) => {
                        warn!("Client buffer full. Dropping tick for {}.", tick.symbol);
                    }
                    Err(mpsc::error::TrySendError::Closed(_)) => {

                    }
                }
            }
        }
    }

    error!("Broadcast listener exited unexpectedly!");
}

// --- 7. Vendor Simulation/Connection (Corrected) ---

/// Simulates a vendor pushing ticks onto the global broadcast channel.
async fn simulate_vendor_stream(tx: broadcast::Sender<Tick>) {
    info!("Simulation mode active.");
    let symbols = vec!["AAPL".to_string(), "GOOG".to_string(), "MSFT".to_string()];
    let mut prices: HashMap<String, f64> = HashMap::from([
        ("AAPL".to_string(), 150.00),
        ("GOOG".to_string(), 2500.00),
        ("MSFT".to_string(), 300.00),
    ]);

    // *** THE CRITICAL FIX: Use StdRng::from_entropy() which is Send ***
    /*let mut rng = match StdRng::from_entropy() {
        Ok(rng) => rng,
        Err(e) => {
            error!("Failed to initialize StdRng: {}", e);
            return;
        }
    };*/


    // OR, if `rand::thread_rng()` was used implicitly, replace it with `StdRng::from_entropy()`
    // Ensure you are using `rand::Rng::gen_range` and not a thread-local accessor.
    
    // The rest of the logic remains the same
    loop {
        for symbol in &symbols {
            // ... (rest of your loop logic)
            // Use the initialized `rng` variable for generating random numbers
            let current_price = prices.get_mut(symbol).unwrap();
            
            // Use the rng instance here:
            let change: f64 = 10.0;
            //(rand::Rng(&mut rng, 0.0..100.0) - 50.0) / 1000.0;
            *current_price += change;
            if *current_price < 1.0 {
                *current_price = 1.0;
            }

            let tick = Tick {
                symbol: symbol.clone(),
                price: *current_price,
                time: std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap()
                    .as_millis() as i64,
            };

            if let Err(e) = tx.send(tick) {
                warn!("Broadcast channel failed to send tick: {}", e);
            }
        }
        sleep(Duration::from_millis(500)).await;
    }
}