use futures_util::{SinkExt, StreamExt};
use std::time::Duration;
use thiserror::Error;
use tokio::{signal, time::timeout};
use tokio_tungstenite::{
    connect_async,
    tungstenite::protocol::frame::coding::CloseCode,
    tungstenite::protocol::{CloseFrame, Message},
};
use tracing::{error, info, warn};
use url::Url;

#[derive(Debug, Error)]
pub enum ClientError {
    #[error("WebSocket error: {0}")]
    Ws(#[from] tokio_tungstenite::tungstenite::Error),

    #[error("Invalid URL: {0}")]
    UrlParse(#[from] url::ParseError),

    #[error("Connection timeout")]
    ConnectionTimeout,

    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
}

/// Connects to a WebSocket server and sends a message.
/// Logs all incoming and outgoing messages.
/// Listens for Ctrl-C to gracefully terminate.
pub async fn run_client(ws_url: &str, msg: &str) -> Result<(), ClientError> {
    let url = Url::parse(ws_url)?;

    // Add connection timeout of 5 seconds
    let connection = timeout(Duration::from_secs(5), connect_async(url.as_str()));
    let (mut ws_stream, _) = match connection.await {
        Ok(Ok(conn)) => conn,
        Ok(Err(e)) => return Err(ClientError::Ws(e)),
        Err(_) => return Err(ClientError::ConnectionTimeout),
    };

    info!("Connected to WebSocket server at {}", ws_url);

    ws_stream.send(Message::Text(msg.into())).await?;
    info!("Sent: {}", msg);

    let signal = signal::ctrl_c();

    tokio::select! {
        _ = async {
            while let Some(msg) = ws_stream.next().await {
                match msg {
                    Ok(Message::Text(text)) => {
                        info!("Received: {}", text);
                    }
                    Ok(Message::Ping(data)) => {
                        // Automatically respond to ping with pong
                        if let Err(e) = ws_stream.send(Message::Pong(data)).await {
                            warn!("Failed to send pong: {}", e);
                        }
                    }
                    Ok(Message::Close(_)) => {
                        info!("Server closed the connection");
                        break;
                    }
                    Ok(_) => {} // Ignore other frames
                    Err(e) => {
                        error!("WebSocket error: {}", e);
                        break;
                    }
                }
            }
        } => {}

        _ = signal => {
            info!("Ctrl-C received, shutting down...");
            // Send close frame for graceful disconnection
            let _ = ws_stream
                .send(Message::Close(Some(CloseFrame {
                    code: CloseCode::Normal, // Normal closure
                    reason: "Client shutting down".into(),
                })))
                .await;
        }
    }

    Ok(())
}
