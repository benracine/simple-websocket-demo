use std::net::SocketAddr;
use std::time::Duration;

use client::run_client;
use publisher::{run_server, ServerConfig};
use tokio::time::sleep;
use tracing::{error, info};
use tracing_subscriber::FmtSubscriber;

#[tokio::main]
async fn main() {
    // Initialize logging
    FmtSubscriber::builder()
        .with_max_level(tracing::Level::INFO)
        .with_target(false)
        .init();

    let addr: SocketAddr = "127.0.0.1:9001".parse().expect("Invalid address");

    // Create custom server configuration
    let config = ServerConfig {
        message_interval: 2,    // Send message every 2 seconds
        connection_timeout: 60, // 60 second inactivity timeout
        enable_heartbeat: true, // Enable ping/pong heartbeats
        heartbeat_interval: 10, // Send ping every 10 seconds
    };

    // Start WebSocket server with configuration
    tokio::spawn(async move {
        if let Err(e) = run_server(addr, Some(config)).await {
            error!("Server error: {}", e);
        }
    });

    // Give the server a moment to start up
    sleep(Duration::from_millis(500)).await;

    // Connect client
    if let Err(e) = run_client("ws://127.0.0.1:9001", "hello from client").await {
        error!("Client error: {}", e);
    }

    // Gracefully exit
    info!("Demo complete");
}
