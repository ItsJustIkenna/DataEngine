use serde::{Deserialize, Serialize};
use serde_yaml;
use std::fs;
use anyhow::{Context, Result};

#[derive(Clone, Serialize, Deserialize, Debug)]
pub struct Config {
    pub data_engine_server_configuration: DataEngineServerConfiguration,
    pub message_broker_server_configuration: MessageBrokerServerConfiguration,
    pub topics: Vec<String>,
    pub exchanges: Vec<Exchange>,
}

#[derive(Clone, Serialize, Deserialize, Debug)]
pub struct DataEngineServerConfiguration {
    pub data_engine_server_settings: ServerSettings,
}

#[derive(Clone, Serialize, Deserialize, Debug)]
pub struct MessageBrokerServerConfiguration {
    pub message_broker_server_settings: ServerSettings,
}

#[derive(Clone, Serialize, Deserialize, Debug)]
pub struct ServerSettings {
    pub address: String,
    pub port: u16,
}

#[derive(Clone, Serialize, Deserialize, Debug)]
pub struct Exchange {
    pub exchange: String,
    pub websocket_token: String,
    pub symbols: Vec<String>,
    pub apis: Vec<Api>,
}

#[derive(Clone, Serialize, Deserialize, Debug)]
pub struct Api {
    pub websockets: Vec<Websocket>,
}

#[derive(Clone, Serialize, Deserialize, Debug)]
pub struct Websocket {
    pub channel: String,
    pub endpoint: String,
}

impl Config {
    pub fn new(file_path: &str) -> Result<Self> {
        let config_data = fs::read_to_string(file_path)
            .with_context(|| format!("Unable to read file: {}", file_path))?;
        let config: Config = serde_yaml::from_str(&config_data)
            .context("YAML was not well-formatted")?;
        Ok(config)
    }
}