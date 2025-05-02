use bytes::Bytes;
use futures_util::{SinkExt, StreamExt};
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;
use thiserror::Error;
use tokio::net::TcpListener;
use tokio::sync::{mpsc, Mutex};
use tokio::time::{interval, timeout};
use tokio_tungstenite::accept_async;
use tokio_tungstenite::tungstenite::{
    protocol::frame::coding::CloseCode, protocol::CloseFrame, Message,
};
use tracing::{debug, error, info, warn};

#[derive(Debug, Error)]
pub enum PublisherError {
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    #[error("WebSocket error: {0}")]
    Ws(#[from] tokio_tungstenite::tungstenite::Error),

    #[error("Channel error")]
    Channel,
}

/// Configuration for the WebSocket server
#[derive(Debug, Clone)]
pub struct ServerConfig {
    /// Interval between periodic messages (in seconds)
    pub message_interval: u64,

    /// Timeout for detecting inactive connections (in seconds)
    pub connection_timeout: u64,

    /// Whether to enable ping/pong heartbeats
    pub enable_heartbeat: bool,

    /// Heartbeat interval (in seconds)
    pub heartbeat_interval: u64,
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            message_interval: 2,
            connection_timeout: 30,
            enable_heartbeat: true,
            heartbeat_interval: 5,
        }
    }
}

/// Starts a WebSocket server that echoes messages back to the client
/// and sends periodic messages.
/// Supports graceful shutdown, ping/pong heartbeats, and configurable options.
pub async fn run_server(
    addr: SocketAddr,
    config: Option<ServerConfig>,
) -> Result<(), PublisherError> {
    let config = config.unwrap_or_default();
    let listener = TcpListener::bind(addr).await?;
    info!("WebSocket server listening on {}", addr);

    // Set up shutdown channel
    let (shutdown_tx, mut shutdown_rx) = mpsc::channel::<()>(1);
    let shutdown_tx = Arc::new(Mutex::new(shutdown_tx));

    // Set up clean termination on ctrl-c
    let shutdown_tx_clone = shutdown_tx.clone();
    tokio::spawn(async move {
        if let Err(e) = tokio::signal::ctrl_c().await {
            error!("Failed to listen for Ctrl-C: {}", e);
            return;
        }
        info!("Received Ctrl-C, initiating graceful shutdown...");
        let _ = shutdown_tx_clone.lock().await.send(()).await;
    });

    // Tracking active connections
    let connections = Arc::new(Mutex::new(Vec::new()));

    loop {
        tokio::select! {
            accept_result = listener.accept() => {
                match accept_result {
                    Ok((stream, peer_addr)) => {
                        info!("Accepted connection from {}", peer_addr);

                        // Connections tracking for graceful shutdown
                        let conn_tx = mpsc::channel::<()>(1).0;
                        connections.lock().await.push(conn_tx);

                        let connections = connections.clone();
                        // let shutdown_tx = shutdown_tx.clone();
                        let config = config.clone();

                        tokio::spawn(async move {
                            handle_connection(stream, peer_addr, config, connections).await;
                        });
                    }
                    Err(e) => {
                        error!("Failed to accept connection: {}", e);
                    }
                }
            }

            _ = shutdown_rx.recv() => {
                info!("Shutting down server...");
                // Wait for all connections to finish (could add timeout here)
                let mut conn_handles = connections.lock().await;
                for _ in conn_handles.drain(..) {
                    // We could wait for each connection to close if needed
                    // by receiving from the channel
                }
                break;
            }
        }
    }

    info!("Server shutdown complete");
    Ok(())
}

async fn handle_connection(
    stream: tokio::net::TcpStream,
    peer_addr: SocketAddr,
    config: ServerConfig,
    connections: Arc<Mutex<Vec<mpsc::Sender<()>>>>,
) {
    // Set TCP keepalive
    /*
    if let Err(e) = stream.set_keepalive(Some(Duration::from_secs(30))) {
        error!("Failed to set keepalive for {}: {}", peer_addr, e);
    }
     */

    // WebSocket handshake with timeout
    let ws_stream = match timeout(Duration::from_secs(5), accept_async(stream)).await {
        Ok(Ok(ws)) => ws,
        Ok(Err(e)) => {
            error!("WebSocket handshake failed with {}: {}", peer_addr, e);
            return;
        }
        Err(_) => {
            error!("WebSocket handshake timed out with {}", peer_addr);
            return;
        }
    };

    info!("WebSocket handshake successful with {}", peer_addr);

    // Split the WebSocket stream
    let (mut ws_sender, mut ws_receiver) = ws_stream.split();

    // Set up message sending channel
    let (msg_tx, mut msg_rx) = mpsc::channel::<Message>(100);
    let msg_tx = Arc::new(msg_tx);

    // Set up periodic message sender
    let msg_tx_clone = msg_tx.clone();
    let periodic_task = tokio::spawn(async move {
        let mut interval = interval(Duration::from_secs(config.message_interval));
        let mut counter = 0;

        loop {
            interval.tick().await;
            counter += 1;

            if let Err(_) = msg_tx_clone
                .send(Message::Text(
                    format!("Periodic message #{}", counter).into(),
                ))
                .await
            {
                break;
            }
        }
    });

    // Set up heartbeat sender if enabled
    let heartbeat_task = if config.enable_heartbeat {
        let msg_tx_clone = msg_tx.clone();
        Some(tokio::spawn(async move {
            let mut interval = interval(Duration::from_secs(config.heartbeat_interval));

            loop {
                interval.tick().await;
                if let Err(_) = msg_tx_clone.send(Message::Ping(Bytes::new())).await {
                    break;
                }
            }
        }))
    } else {
        None
    };

    // Task for forwarding messages from the channel to the WebSocket
    let sender_task = tokio::spawn(async move {
        while let Some(msg) = msg_rx.recv().await {
            let msg_clone = msg.clone();
            if let Err(e) = ws_sender.send(msg).await {
                error!("Failed to send message to {}: {}", peer_addr, e);
                break;
            }

            if let Message::Text(text) = &msg_clone {
                debug!("Sent to {}: {}", peer_addr, text);
            }
        }
    });

    // Handle incoming messages
    let msg_tx_clone = msg_tx.clone();

    // Main message processing loop
    while let Some(msg) = ws_receiver.next().await {
        let last_activity = tokio::time::Instant::now();

        match msg {
            Ok(Message::Text(text)) => {
                info!("Received from {}: {}", peer_addr, text);

                // Echo the message back
                if let Err(_) = msg_tx_clone
                    .send(Message::Text(format!("echo: {}", text).into()))
                    .await
                {
                    break;
                }
            }
            Ok(Message::Ping(data)) => {
                debug!("Received ping from {}", peer_addr);
                if let Err(_) = msg_tx_clone.send(Message::Pong(data)).await {
                    break;
                }
            }
            Ok(Message::Pong(_)) => {
                debug!("Received pong from {}", peer_addr);
            }
            Ok(Message::Close(frame)) => {
                info!("Received close frame from {}: {:?}", peer_addr, frame);
                // Echo close frame back
                if let Err(e) = msg_tx_clone.send(Message::Close(frame)).await {
                    error!("Failed to send close confirmation: {}", e);
                }
                break;
            }
            Ok(_) => {} // Ignore other message types
            Err(e) => {
                error!("WebSocket error with {}: {}", peer_addr, e);
                break;
            }
        }

        // Check for inactivity timeout
        if last_activity.elapsed() > Duration::from_secs(config.connection_timeout) {
            warn!("Connection with {} timed out due to inactivity", peer_addr);
            let _ = msg_tx_clone
                .send(Message::Close(Some(CloseFrame {
                    code: CloseCode::Normal,
                    reason: "Inactivity timeout".into(),
                })))
                .await;
            break;
        }
    }

    // Clean up tasks
    periodic_task.abort();
    if let Some(task) = heartbeat_task {
        task.abort();
    }
    sender_task.abort();

    // Remove this connection from tracking
    let mut conns = connections.lock().await;
    conns.retain(|_| true); // Just keeping the lock to prevent race conditions

    info!("Connection with {} closed", peer_addr);
}
