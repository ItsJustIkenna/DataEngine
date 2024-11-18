mod config;
pub mod endpoint;
pub mod get_info;
pub mod handlers;

use anyhow::Result;
use async_trait::async_trait;
use config::{Config, DataEngineServerConfiguration};
use databaseschema::{establish_connection_pool, CustomAsyncPgConnectionManager};
use deadpool::managed::Pool;
use deadpool_redis::{Connection, Manager};
use get_info::{get_exchange, get_orderbooks, get_securities};
use handlers::{
    rest::kraken_rest_api::KrakenRestHandler,
    websockets::kraken_websocket_handler::KrakenWebSocketHandler,
};
use mockall::automock;
use redis::cmd;
use redis_utils::{create_redis_connection, create_redis_pool};
use serde_json::json;
use std::sync::Arc;
use tokio::{signal, sync::broadcast::channel};
use tracing::{error, info};

use crate::endpoint::EndpointHandler;

#[automock]
#[async_trait]
pub trait HostedObjectTrait {
    async fn run(&mut self) -> Result<()>;
}

pub struct HostedObject {
    config: Config,
    redis_pool: Arc<Pool<Manager, Connection>>,
    postgres_pool: Arc<Pool<CustomAsyncPgConnectionManager>>,
}

impl HostedObject {
    pub fn host(&self) -> &DataEngineServerConfiguration {
        &self.config.data_engine_server_configuration
    }

    pub async fn new() -> Result<Self> {
        let path = "C:/Users/ikenn/TradingPlatform/DataEngine/appsettings.json";
        let config = Config::new(path)?;
        let redis_pool = Arc::new(create_redis_pool().expect("Failed to create Redis pool"));
        let postgres_pool = Arc::new(establish_connection_pool());

        Ok(Self {
            config,
            redis_pool,
            postgres_pool,
        })
    }
}

#[async_trait]
impl HostedObjectTrait for HostedObject {
    async fn run(&mut self) -> Result<()> {
        let (shutdown_tx, _) = channel(1);
        let mut handles = vec![];
        let config = self.config.clone();

        for exchange in config.exchanges.iter().cloned() {
            if exchange.exchange == "Kraken" {
                let kraken_exchange = get_exchange(
                    self.redis_pool.clone(),
                    self.postgres_pool.clone(),
                    exchange.exchange,
                )
                .await;

                let auth_endpoint = exchange.websocket_token.clone();
                let api_response = KrakenRestHandler::authenticate(&auth_endpoint).await?;

                for api in exchange.apis.iter().cloned() {
                    for websocket in api.websockets.iter().cloned() {
                        let api_response = api_response.clone();

                        let securities = get_securities(
                            self.redis_pool.clone(),
                            self.postgres_pool.clone(),
                            &exchange.symbols,
                        )
                        .await;

                        let orderbooks = get_orderbooks(
                            self.redis_pool.clone(),
                            self.postgres_pool.clone(),
                            securities,
                            &kraken_exchange,
                        )
                        .await;

                        let orderbooks = orderbooks.clone();
                        let shutdown_tx = shutdown_tx.clone();
                        let redis_pool = self.redis_pool.clone();
                        let postgres_pool = self.postgres_pool.clone();
                        let symbols = exchange.symbols.clone();
                        let handle = tokio::spawn(async move {
                            info!("Connecting to {:?}", &websocket.endpoint);

                            match websocket.channel.as_str() {
                                "level3" => {
                                    let subscribe_message = json!({
                                        "method": "subscribe",
                                        "params": {
                                            "channel": &websocket.channel,
                                            "symbol": &symbols,
                                            "depth": 10,
                                            "snapshot": true,
                                            "token": &api_response.get_token(),
                                        }
                                    });

                                    let unsubscribe_message = json!({
                                        "method": "unsubscribe",
                                        "params": {
                                            "channel": &websocket.channel,
                                            "symbol": &symbols,
                                            "token": &api_response.get_token(),
                                        }
                                    });

                                    for orderbook in orderbooks.iter() {
                                        match KrakenWebSocketHandler::new(
                                            &websocket.endpoint,
                                            orderbook.clone(),
                                            &subscribe_message,
                                            &unsubscribe_message,
                                            redis_pool.clone(),
                                            postgres_pool.clone(),
                                        )
                                        .await
                                        {
                                            Ok(handler) => {
                                                let handler = Arc::new(handler);
                                                match handler.listen(shutdown_tx.subscribe()).await
                                                {
                                                    Ok(_) => {
                                                        println!(
                                                            "Listening to Kraken level 3 {}",
                                                            orderbook.symbol
                                                        );
                                                    }
                                                    Err(e) => error!("Failed to listen: {}", e),
                                                }
                                            }
                                            Err(e) => error!("Failed to create handler: {}", e),
                                        }
                                    }
                                }
                                "trade" => {
                                    let subscribe_message = json!({
                                        "method": "subscribe",
                                        "params": {
                                            "channel": &websocket.channel,
                                            "symbol": &symbols,
                                            "snapshot": true
                                        }
                                    });

                                    let unsubscribe_message = json!({
                                        "method": "unsubscribe",
                                        "params": {
                                            "channel": &websocket.channel,
                                            "symbol": symbols,
                                            "snapshot": true
                                        }
                                    });

                                    for orderbook in orderbooks.iter() {
                                        match KrakenWebSocketHandler::new(
                                            &websocket.endpoint,
                                            orderbook.clone(),
                                            &subscribe_message,
                                            &unsubscribe_message,
                                            redis_pool.clone(),
                                            postgres_pool.clone(),
                                        )
                                        .await
                                        {
                                            Ok(handler) => {
                                                let handler = Arc::new(handler);
                                                match handler.listen(shutdown_tx.subscribe()).await
                                                {
                                                    Ok(_) => {
                                                        println!(
                                                            "Listening to Kraken trade {}",
                                                            orderbook.symbol
                                                        );
                                                    }
                                                    Err(e) => error!("Failed to listen: {}", e),
                                                }
                                            }
                                            Err(e) => error!("Failed to create handler: {}", e),
                                        }
                                    }
                                }
                                _ => {
                                    info!("Unknown websocket type: {}", websocket.channel.as_str());
                                }
                            };
                        });
                        handles.push(handle);
                    }
                }
            } else {
                println!("Unknown exchange: {}", exchange.exchange);
            }
        }

        let redis_pool = self.redis_pool.clone();

        let shutdown_handle = tokio::spawn(async move {
            if let Err(e) = signal::ctrl_c().await {
                error!("Failed to listen for shutdown signal: {}", e);
            }

            let mut connection = create_redis_connection(&redis_pool)
                .await
                .expect("Failed to create redis connection");
            match cmd("FLUSHALL").query_async::<_, ()>(&mut connection).await {
                Ok(_) => {
                    println!("Successfully flushed Redis cache");
                }
                Err(e) => {
                    println!("Failed to flush Redis cache: {}", e);
                }
            }

            let _ = shutdown_tx.send(());
        });

        for handle in handles {
            let _ = handle.await;
        }

        let _ = shutdown_handle.await;
        Ok(())
    }
}