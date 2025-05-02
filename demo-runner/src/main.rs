// Standard library imports
use std::net::SocketAddr;
use std::time::Duration;

// External crate imports
use client::run_client;
use publisher::{run_server, ServerConfig};
use thiserror::Error;
use tokio::{signal, time::sleep};
use tracing::{error, info, warn};
use tracing_subscriber::FmtSubscriber;
use tungstenite::Error as WsError;

// thiserror keeps clear error handling
#[derive(Error, Debug)]
pub enum DemoError {
    #[error("Failed to parse address: {0}")]
    AddressParseError(#[from] std::net::AddrParseError),
    #[error("Failed to connect to server: {0}")]
    ConnectionError(#[from] WsError),
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize logging
    FmtSubscriber::builder()
        .with_max_level(tracing::Level::INFO)
        .with_target(false)
        .init();

    let addr: SocketAddr = match "127.0.0.1:9001".parse() {
        Ok(addr) => addr,
        Err(e) => {
            error!("Failed to parse address: {}", e);
            return Err(e.into());
        }
    };

    info!("Starting WebSocket demo on {}", addr);

    // Create custom server configuration
    let config = ServerConfig {
        message_interval: 2,    // Send message every 2 seconds
        connection_timeout: 60, // 60 second inactivity timeout
        enable_heartbeat: true, // Enable ping/pong heartbeats
        heartbeat_interval: 10, // Send ping every 10 seconds
    };

    // Start WebSocket server with configuration
    let server_handle = tokio::spawn(async move {
        if let Err(e) = run_server(addr, Some(config)).await {
            error!("Server error: {}", e);
        }
    });

    // Give the server a moment to start up
    sleep(Duration::from_millis(500)).await;

    // Connect client
    info!("Connecting client to server...");
    if let Err(e) = run_client("ws://127.0.0.1:9001", "hello from client").await {
        error!("Client error: {}", e);
    }

    // Wait for Ctrl-C or server shutdown
    tokio::select! {
        _ = signal::ctrl_c() => {
            info!("Received Ctrl-C, shutting down...");
        }
        _ = server_handle => {
            warn!("Server task completed unexpectedly");
        }
    }

    info!("Demo complete");
    Ok(())
}
