use anyhow::Result;
use async_trait::async_trait;
use databaseschema::{
    models::OrderBook, CustomAsyncPgConnectionManager,
};
use deadpool::managed::Pool;
use deadpool_redis::{Connection, Manager};
use futures_util::{
    stream::{SplitSink, SplitStream},
    SinkExt, StreamExt,
};
use serde_json::Value;
use std::sync::Arc;
use tokio::{
    net::TcpStream,
    sync::{broadcast::Receiver, Mutex},
};
use tokio_tungstenite::{connect_async, tungstenite::Message, MaybeTlsStream, WebSocketStream};

use crate::{
    endpoint::EndpointHandler,
    handlers::
        structs::responses::{parse_message, Response}
    ,
};
//-------------------------------------------------------------------------

pub struct KrakenWebSocketHandler {
    writer: Mutex<SplitSink<WebSocketStream<MaybeTlsStream<TcpStream>>, Message>>,
    reader: Mutex<SplitStream<WebSocketStream<MaybeTlsStream<TcpStream>>>>,
    unsubscribe_message: Value,
    order_book: OrderBook,
    redis_pool: Arc<Pool<Manager, Connection>>,
    postgres_pool: Arc<Pool<CustomAsyncPgConnectionManager>>,
}

impl KrakenWebSocketHandler {
    pub async fn new(endpoint: &str, order_book: OrderBook, subscribe_message: &Value, unsubscribe_message: &Value, redis_pool: Arc<Pool<Manager, Connection>>, postgres_pool: Arc<Pool<CustomAsyncPgConnectionManager>>) -> Result<Self> {
        let (ws_stream, _) = match connect_async(endpoint).await {
            Ok(val) => {
                println!("Connected to endpoint: {}", endpoint);
                val
            }
            Err(e) => {
                println!("Failed to connect to endpoint: {}", e);
                return Err(e.into());
            }
        };

        let (mut writer, reader) = ws_stream.split();
        
        writer
            .send(Message::Text(subscribe_message.to_string()))
            .await
            .expect("Failed to send message");

        let reader = Mutex::new(reader);
        let writer = Mutex::new(writer);
        Ok(Self {
            writer,
            reader,
            unsubscribe_message: unsubscribe_message.clone(),
            order_book,
            redis_pool,
            postgres_pool,
        })
    }
}

#[async_trait]
impl EndpointHandler for KrakenWebSocketHandler {
    async fn listen(self: Arc<Self>, mut shutdown_rx: Receiver<()>) -> Result<()> {
        let handler = self.clone();

        tokio::spawn(async move {
            tokio::select! {
                // Listening to WebSocket Messages
                _ = async {
                    loop {
                        let message = handler.reader.lock().await.next().await;

                        match message {
                            Some(Ok(Message::Text(text))) => {
                                match parse_message(&text) {
                                    Ok(Response::Level3Snapshot(snapshot)) => {
                                        println!("Received level 3 snapshot: {:?}", snapshot);
                                        // Process the snapshot
                                        match snapshot.process_snapshot(self.redis_pool.clone(), self.postgres_pool.clone(), handler.order_book.clone()).await {
                                            Ok(()) => {
                                                println!("Order book snapshot processed successfully");
                                            },
                                            Err(e) => {
                                                println!("Failed to process order book snapshot: {}", e);
                                            }
                                        }
                                    },
                                    Ok(Response::Level3Update(update)) => {
                                        println!("Received level 3 update: {:?}", update);
                                        // Process the update
                                        match update.process_update(self.redis_pool.clone(), self.postgres_pool.clone(), handler.order_book.clone()).await {
                                            Ok(()) => {
                                                println!("Order book update processed successfully");
                                            },
                                            Err(e) => {
                                                println!("Failed to process order book update: {}", e);
                                            }
                                        }
                                    },
                                    Ok(Response::TradeSnapshot(snapshot)) => {
                                        println!("Received trade snapshot: {:?}", snapshot);
                                        // Process the snapshot
                                        match snapshot.process_snapshot(self.redis_pool.clone(), self.postgres_pool.clone(), handler.order_book.clone()).await {
                                            Ok(()) => {
                                                println!("Trade snapshot processed successfully");
                                            },
                                            Err(e) => {
                                                println!("Failed to process trade snapshot: {}", e);
                                            }
                                        }
                                    },
                                    Ok(Response::TradeUpdate(update)) => {
                                        println!("Received trade update: {:?}", update);
                                        // Process the update
                                        match update.process_update(self.redis_pool.clone(), self.postgres_pool.clone(), handler.order_book.clone()).await {
                                            Ok(()) => {
                                                println!("Trade update processed successfully");
                                            },
                                            Err(e) => {
                                                println!("Failed to process trade update: {}", e);
                                            }
                                        }
                                    },
                                    Ok(Response::HeartBeat(heartbeat)) => {
                                        println!("Received heartbeat: {:?}", heartbeat);
                                    },
                                    Ok(Response::Status(status)) => {
                                        println!("Received status: {:?}", status);
                                    },
                                    Ok(Response::Level3Subscribe(subscribe)) => {
                                        println!("Received level 3 subscribe: {:?}", subscribe);
                                    },
                                    Ok(Response::Level3SubscribeAck(subscribe_ack)) => {
                                        println!("Received level 3 subscribe_ack: {:?}", subscribe_ack);
                                    },
                                    Ok(Response::Level3Unsubscribe(unsubscribe)) => {
                                        println!("Received level 3 unsubscribe: {:?}", unsubscribe);
                                    },
                                    Ok(Response::Level3UnsubscribeAck(unsubscribe_ack)) => {
                                        println!("Received level 3 unsubscribe_ack: {:?}", unsubscribe_ack);
                                    },
                                    Ok(Response::TradeSubscribe(subscribe)) => {
                                        println!("Received trade subscribe: {:?}", subscribe);
                                    },
                                    Ok(Response::TradeSubscribeAck(subscribe_ack)) => {
                                        println!("Received trade subscribe_ack: {:?}", subscribe_ack);
                                    },
                                    Ok(Response::TradeUnsubscribe(unsubscribe)) => {
                                        println!("Received trade unsubscribe: {:?}", unsubscribe);
                                    },
                                    Ok(Response::TradeUnsubscribeAck(unsubscribe_ack)) => {
                                        println!("Received trade unsubscribe_ack: {:?}", unsubscribe_ack);
                                    },
                                    Ok(Response::TokenResponse(token_response)) => {
                                        println!("Received token_response: {:?}", token_response);
                                    },
                                    Err(e) => {
                                        println!("Failed to parse message: {}", e);
                                    }
                                }
                            },
                            Some(Ok(Message::Close(_))) => {
                                println!("WebSocket connection closed");
                                break;
                            },
                            Some(Ok(Message::Ping(ping))) => {
                                println!("Received ping: {:?}", ping);
                            },
                            Some(Ok(Message::Pong(pong))) => {
                                println!("Received pong: {:?}", pong);
                            },
                            Some(Ok(Message::Binary(bin))) => {
                                println!("Received binary: {:?}", bin);
                            },
                            Some(Ok(Message::Frame(frame))) => {
                                println!("Received frame: {:?}", frame);
                            },
                            Some(Err(e)) => {
                                println!("Error receiving message: {}", e);
                            },
                            None => {
                                println!("WebSocket connection closed");
                                break;
                            }
                        }
                    }
                } => {},

                // Handling Shutdown
                _ = shutdown_rx.recv() => {

                    // Unsubscribe from the WebSocket
                    println!("Unsubscribing from WebSocket");
                    handler.writer.lock().await.send(Message::Text(handler.unsubscribe_message.to_string())).await.expect("Failed to send message");

                    println!("Shutting down KrakenWebSocketHandler");
                },
            }
        });

        Ok(())
    }
}
