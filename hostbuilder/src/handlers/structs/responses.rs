use std::{str::FromStr, sync::Arc};

use anyhow::{anyhow, Result};
use bigdecimal::{BigDecimal, Zero};
use databaseschema::{
    models::{
        NewModifiedBuyOrder, NewModifiedSellOrder, NewOpenBuyOrder, NewOpenSellOrder, NewTrade, OrderBook
    },
    ops::{
        modified_buy_order_ops::{
            create_modified_buy_orders, delete_modified_buy_orders,
        },
        modified_sell_order_ops::{
            create_modified_sell_orders, delete_modified_sell_orders,
        },
        open_buy_order_ops::{
            create_open_buy_orders, delete_open_buy_orders,
        },
        open_sell_order_ops::{
            create_open_sell_orders, delete_open_sell_orders,
        },
        order_book_ops::update_orderbook, trades_ops::create_trades,
    },
    CustomAsyncPgConnectionManager,
};
use deadpool::managed::Pool;
use deadpool_redis::{Connection, Manager};
use redis::cmd;
use redis_utils::create_redis_connection;
use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Debug, Serialize, Deserialize)]
pub enum Response {
    TokenResponse(TokenResponse),
    Status(Status),
    Level3Subscribe(Level3Subscribe),
    Level3Unsubscribe(Level3Unsubscribe),
    Level3SubscribeAck(Level3SubscribeAcknowledgement),
    Level3UnsubscribeAck(Level3UnsubscribeAcknowledgement),
    Level3Snapshot(Level3Snapshot),
    Level3Update(Level3Update),
    HeartBeat(HeartBeat),
    TradeSubscribe(TradeSubscribe),
    TradeUnsubscribe(TradeUnsubscribe),
    TradeSubscribeAck(TradeSubscribeAcknowledgement),
    TradeUnsubscribeAck(TradeUnsubscribeAcknowledgement),
    TradeSnapshot(TradeSnapshot),
    TradeUpdate(TradeUpdate),
}

//----------------------------------------------------------------------

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct TokenResponse {
    error: Vec<String>,
    result: TokenResultData,
}

impl TokenResponse {
    pub fn from_json(json: &serde_json::Value) -> Result<TokenResponse> {
        let token_response: TokenResponse = serde_json::from_value(json.clone())?;
        Ok(token_response)
    }

    pub fn get_token(&self) -> String {
        self.result.token.clone()
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct TokenResultData {
    token: String,
    expires: u64,
}

//-------------------------------------------------------------------------

#[derive(Debug, Serialize, Deserialize)]
pub struct Status {
    channel: String,
    data: Vec<StatusData>,
    r#type: String,
}

impl Status {
    pub fn from_json(json: &serde_json::Value) -> Result<Status> {
        let status: Status = serde_json::from_value(json.clone())?;
        Ok(status)
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct StatusData {
    api_version: String,
    connection_id: u64,
    system: String,
    version: String,
}

//-------------------------------------------------------------------------

#[derive(Debug, Serialize, Deserialize)]
pub struct Level3Subscribe {
    method: String,
    params: Level3SubscribeParams,
}

impl Level3Subscribe {
    pub fn from_json(json: &serde_json::Value) -> Result<Level3Subscribe> {
        let subscribe: Level3Subscribe = serde_json::from_value(json.clone())?;
        Ok(subscribe)
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Level3SubscribeParams {
    channel: String,
    symbol: Vec<String>,
    depth: u64,
    snapshot: bool,
    token: String,
}

//-------------------------------------------------------------------------

#[derive(Debug, Serialize, Deserialize)]
pub struct Level3Unsubscribe {
    method: String,
    params: Level3UnsubscribeParams,
}

impl Level3Unsubscribe {
    pub fn from_json(json: &serde_json::Value) -> Result<Level3Unsubscribe> {
        let unsubscribe: Level3Unsubscribe = serde_json::from_value(json.clone())?;
        Ok(unsubscribe)
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Level3UnsubscribeParams {
    channel: String,
    symbol: Vec<String>,
    token: String,
}

//-------------------------------------------------------------------------

#[derive(Debug, Serialize, Deserialize)]
pub struct Level3SubscribeAcknowledgement {
    method: String,
    result: Level3SubscribeResonseData,
    success: bool,
    time_in: String,
    time_out: String,
}

impl Level3SubscribeAcknowledgement {
    pub fn from_json(json: &serde_json::Value) -> Result<Level3SubscribeAcknowledgement> {
        let subscribe_ack: Level3SubscribeAcknowledgement = serde_json::from_value(json.clone())?;
        Ok(subscribe_ack)
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Level3SubscribeResonseData {
    channel: String,
    snapshot: bool,
    symbol: String,
}

//-------------------------------------------------------------------------

#[derive(Debug, Serialize, Deserialize)]
pub struct Level3UnsubscribeAcknowledgement {
    method: String,
    result: UnsubscribeResonseData,
    success: bool,
    time_in: String,
    time_out: String,
}

impl Level3UnsubscribeAcknowledgement {
    pub fn from_json(json: &serde_json::Value) -> Result<Level3UnsubscribeAcknowledgement> {
        let unsubscribe_ack: Level3UnsubscribeAcknowledgement = serde_json::from_value(json.clone())?;
        Ok(unsubscribe_ack)
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct UnsubscribeResonseData {
    channel: String,
    symbol: String,
}

//-------------------------------------------------------------------------

#[derive(Debug, Serialize, Deserialize)]
pub struct HeartBeat {
    channel: String,
}

impl HeartBeat {
    pub fn from_json(json: &serde_json::Value) -> Result<HeartBeat> {
        let heartbeat: HeartBeat = serde_json::from_value(json.clone())?;
        Ok(heartbeat)
    }
}

//-------------------------------------------------------------------------

#[derive(Debug, Serialize, Deserialize)]
pub struct TradeSubscribe {
    method: String,
    params: TradeSubscribeParams,
}

impl TradeSubscribe {
    pub fn from_json(json: &serde_json::Value) -> Result<TradeSubscribe> {
        let subscribe: TradeSubscribe = serde_json::from_value(json.clone())?;
        Ok(subscribe)
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct TradeSubscribeParams {
    channel: String,
    symbol: Vec<String>,
    snapshot: bool,
}

//-------------------------------------------------------------------------

#[derive(Debug, Serialize, Deserialize)]
pub struct TradeUnsubscribe {
    method: String,
    params: TradeUnsubscribeParams,
}

impl TradeUnsubscribe {
    pub fn from_json(json: &serde_json::Value) -> Result<TradeUnsubscribe> {
        let unsubscribe: TradeUnsubscribe = serde_json::from_value(json.clone())?;
        Ok(unsubscribe)
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct TradeUnsubscribeParams {
    channel: String,
    symbol: Vec<String>,
    snapshot: bool,
}

//-------------------------------------------------------------------------

#[derive(Debug, Serialize, Deserialize)]
pub struct TradeSubscribeAcknowledgement {
    method: String,
    result: TradeSubscribeResonseData,
    success: bool,
    time_in: String,
    time_out: String,
}

impl TradeSubscribeAcknowledgement {
    pub fn from_json(json: &serde_json::Value) -> Result<TradeSubscribeAcknowledgement> {
        let subscribe_ack: TradeSubscribeAcknowledgement = serde_json::from_value(json.clone())?;
        Ok(subscribe_ack)
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct TradeSubscribeResonseData {
    channel: String,
    snapshot: bool,
    symbol: String,
}

//-------------------------------------------------------------------------

#[derive(Debug, Serialize, Deserialize)]
pub struct TradeUnsubscribeAcknowledgement {
    method: String,
    result: TradeUnsubscribeResonseData,
    success: bool,
    time_in: String,
    time_out: String,
}

impl TradeUnsubscribeAcknowledgement {
    pub fn from_json(json: &serde_json::Value) -> Result<TradeUnsubscribeAcknowledgement> {
        let unsubscribe_ack: TradeUnsubscribeAcknowledgement = serde_json::from_value(json.clone())?;
        Ok(unsubscribe_ack)
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct TradeUnsubscribeResonseData {
    channel: String,
    symbol: String,
}

//-------------------------------------------------------------------------

#[derive(Debug, Serialize, Deserialize)]
pub struct Level3Snapshot {
    channel: String,
    r#type: String,
    data: Vec<Level3SnapshotData>,
}

impl Level3Snapshot {
    pub fn from_json(json: &serde_json::Value) -> Result<Level3Snapshot> {
        let snapshot: Level3Snapshot = serde_json::from_value(json.clone())?;
        Ok(snapshot)
    }

    pub async fn process_snapshot(
        &self,
        redis_pool: Arc<Pool<Manager, Connection>>,
        postgres_pool: Arc<Pool<CustomAsyncPgConnectionManager>>,
        order_book: OrderBook,
    ) -> Result<()> {
        let mut book_connection = create_redis_connection(&redis_pool)
            .await
            .expect("Failed to create redis connection");
        let mut bid_connection = create_redis_connection(&redis_pool)
            .await
            .expect("Failed to create redis connection");
        let mut ask_connection = create_redis_connection(&redis_pool)
            .await
            .expect("Failed to create redis connection");

        if self.channel == "level3" {
            if self.data.is_empty() {
                println!("Snapshot data is empty");
            }

            let snapshot_data = &self.data[0];

            if snapshot_data.bids.is_empty() && snapshot_data.asks.is_empty() {
                println!("Both bids and asks are empty");
            }

            let mut open_bid_volume = BigDecimal::zero();
            let mut open_ask_volume = BigDecimal::zero();

            let bid_futures = async {
                if !snapshot_data.bids.is_empty() {
                    let mut buy_orders = Vec::new();
                    let mut pipe = redis::pipe();

                    for bid in snapshot_data.bids.iter() {
                        let price_level = &bid.limit_price;
                        let buy_quantity = &bid.order_qty;
                        let unique_id = &bid.order_id;

                        open_bid_volume += buy_quantity;

                        let order = NewOpenBuyOrder::new(
                            &order_book.symbol,
                            &order_book.exchange,
                            order_book.security_id,
                            order_book.exchange_id,
                            order_book.buy_order_book_id,
                            unique_id,
                            price_level,
                            buy_quantity,
                        );

                        pipe.atomic()
                            .cmd("HSET")
                            .arg(format!("buy_order:{}", order.unique_id))
                            .arg("symbol")
                            .arg(&order.symbol)
                            .arg("exchange")
                            .arg(&order.exchange)
                            .arg("security_id")
                            .arg(&order.security_id.to_string())
                            .arg("exchange_id")
                            .arg(&order.exchange_id.to_string())
                            .arg("buy_order_book_id")
                            .arg(&order.buy_order_book_id.to_string())
                            .arg("unique_id")
                            .arg(&order.unique_id)
                            .arg("price_level")
                            .arg(&order.price_level.to_string())
                            .arg("buy_quantity")
                            .arg(&order.buy_quantity.to_string());

                        buy_orders.push(order);
                    }

                    match pipe.query_async::<_, ()>(&mut bid_connection).await {
                        Ok(_) => println!("Added buy orders to redis"),
                        Err(e) => println!("Failed to add buy orders to redis: {}", e),
                    }

                    create_open_buy_orders(postgres_pool.clone(), buy_orders).await;
                }
            };

            let ask_futures = async {
                if !snapshot_data.asks.is_empty() {
                    let mut add_sell_orders = Vec::new();
                    let mut pipe = redis::pipe();
                    for ask in snapshot_data.asks.iter() {
                        let price_level = &ask.limit_price;
                        let sell_quantity = &ask.order_qty;
                        let unique_id = &ask.order_id;

                        open_ask_volume += sell_quantity;

                        let order = NewOpenSellOrder::new(
                            &order_book.symbol,
                            &order_book.exchange,
                            order_book.security_id,
                            order_book.exchange_id,
                            order_book.sell_order_book_id,
                            unique_id,
                            price_level,
                            sell_quantity,
                        );

                        pipe.atomic()
                            .cmd("HSET")
                            .arg(format!("sell_order:{}", order.unique_id))
                            .arg("symbol")
                            .arg(&order.symbol)
                            .arg("exchange")
                            .arg(&order.exchange)
                            .arg("security_id")
                            .arg(&order.security_id.to_string())
                            .arg("exchange_id")
                            .arg(&order.exchange_id.to_string())
                            .arg("sell_order_book_id")
                            .arg(&order.sell_order_book_id.to_string())
                            .arg("unique_id")
                            .arg(&order.unique_id)
                            .arg("price_level")
                            .arg(&order.price_level.to_string())
                            .arg("sell_quantity")
                            .arg(&order.sell_quantity.to_string());

                        add_sell_orders.push(order);
                    }

                    match pipe.query_async::<_, ()>(&mut ask_connection).await {
                        Ok(_) => println!("Added sell orders to redis"),
                        Err(e) => println!("Failed to add sell orders to redis: {}", e),
                    }

                    create_open_sell_orders(postgres_pool.clone(), add_sell_orders).await;
                }
            };

            tokio::join!(bid_futures, ask_futures);


            let total_volume = open_bid_volume + open_ask_volume;

            println!("Total volume: {}", total_volume);

            match cmd("HSET")
                .arg(format!("order_book:{}", order_book.order_book_id))
                .arg("created_at")
                .arg(&order_book.created_at.to_string())
                .arg("updated_at")
                .arg(&order_book.updated_at.expect("").to_string())
                .arg("symbol")
                .arg(&order_book.symbol)
                .arg("exchange")
                .arg(&order_book.exchange)
                .arg("security_id")
                .arg(&order_book.security_id.to_string())
                .arg("exchange_id")
                .arg(&order_book.exchange_id.to_string())
                .arg("buy_order_book_id")
                .arg(&order_book.buy_order_book_id.to_string())
                .arg("sell_order_book_id")
                .arg(&order_book.sell_order_book_id.to_string())
                .arg("total_volume")
                .arg(&total_volume.to_string())
                .query_async::<_, ()>(&mut book_connection)
                .await
            {
                Ok(_) => {
                    println!("Added order book to redis");
                }
                Err(e) => {
                    println!("Failed to add order book to redis: {}", e);
                }
            }
            update_orderbook(postgres_pool.clone(), order_book, total_volume).await;
            Ok(())
        } else {
            println!("Channel is not level3");
            Ok(())
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
struct Level3SnapshotData {
    symbol: String,
    checksum: i64,
    bids: Vec<SnapshotOrder>,
    asks: Vec<SnapshotOrder>,
}

#[derive(Debug, Serialize, Deserialize)]
struct SnapshotOrder {
    order_id: String,
    limit_price: BigDecimal,
    order_qty: BigDecimal,
    timestamp: String,
}

//-------------------------------------------------------------------------

#[derive(Debug, Serialize, Deserialize)]
pub struct Level3Update {
    channel: String,
    r#type: String,
    data: Vec<Level3UpdateData>,
}

impl Level3Update {
    pub fn from_json(json: &serde_json::Value) -> Result<Level3Update> {
        let update: Level3Update = serde_json::from_value(json.clone())?;
        Ok(update)
    }

    pub async fn process_update(
        &self,
        redis_pool: Arc<Pool<Manager, Connection>>,
        postgres_pool: Arc<Pool<CustomAsyncPgConnectionManager>>,
        order_book: OrderBook,
    ) -> Result<()> {
        let mut book_connection = create_redis_connection(&redis_pool)
            .await
            .expect("Failed to create redis connection");
        let mut bid_connection = create_redis_connection(&redis_pool)
            .await
            .expect("Failed to create redis connection");
        let mut ask_connection = create_redis_connection(&redis_pool)
            .await
            .expect("Failed to create redis connection");

        if self.channel == "level3" {
            if self.data.is_empty() {
                println!("update data is empty");
            }

            let update_data = &self.data[0];

            if update_data.bids.is_empty() && update_data.asks.is_empty() {
                println!("Both bids and asks are empty");
            }

            let total_volume = cmd("HGET")
            .arg(format!("order_book:{}", order_book.order_book_id))
            .arg("total_volume")
            .query_async::<_, String>(&mut book_connection).await.expect("Failed to get total volume");

            let mut bid_volume = BigDecimal::zero();
            let mut ask_volume = BigDecimal::zero();

            let mut total_volume = BigDecimal::from_str(&total_volume).expect("Failed to convert total volume to BigDecimal");

            let bid_futures = async {
                let mut buy_orders = Vec::new();
                let mut modified_orders = Vec::new();
                let mut delete_orders = Vec::new();
                let mut pipe = redis::pipe();

                for bid in update_data.bids.iter() {
                    if bid.event == "add" {
                        let price_level = &bid.limit_price;
                        let buy_quantity = &bid.order_qty;
                        let unique_id = &bid.order_id;

                        {
                            bid_volume += buy_quantity;
                        }

                        let order = NewOpenBuyOrder::new(
                            &order_book.symbol,
                            &order_book.exchange,
                            order_book.security_id,
                            order_book.exchange_id,
                            order_book.buy_order_book_id,
                            unique_id,
                            price_level,
                            buy_quantity,
                        );

                        pipe.atomic()
                            .cmd("HSET")
                            .arg(format!("buy_order:{}", order.unique_id))
                            .arg("symbol")
                            .arg(&order.symbol)
                            .arg("exchange")
                            .arg(&order.exchange)
                            .arg("security_id")
                            .arg(&order.security_id.to_string())
                            .arg("exchange_id")
                            .arg(&order.exchange_id.to_string())
                            .arg("buy_order_book_id")
                            .arg(&order.buy_order_book_id.to_string())
                            .arg("unique_id")
                            .arg(&order.unique_id)
                            .arg("price_level")
                            .arg(&order.price_level.to_string())
                            .arg("buy_quantity")
                            .arg(&order.buy_quantity.to_string());

                        buy_orders.push(order);
                    } else if bid.event == "modify" {
                        let price_level = &bid.limit_price;
                        let new_buy_quantity = &bid.order_qty;
                        let unique_id = &bid.order_id;

                        {
                            bid_volume += new_buy_quantity;
                        }

                        let order = NewModifiedBuyOrder::new(
                            &order_book.symbol,
                            &order_book.exchange,
                            order_book.security_id,
                            order_book.exchange_id,
                            order_book.buy_order_book_id,
                            unique_id,
                            price_level,
                            new_buy_quantity,
                        );

                        pipe.atomic()
                            .cmd("HSET")
                            .arg(format!("buy_order:{}", order.unique_id))
                            .arg("symbol")
                            .arg(&order.symbol)
                            .arg("exchange")
                            .arg(&order.exchange)
                            .arg("security_id")
                            .arg(&order.security_id.to_string())
                            .arg("exchange_id")
                            .arg(&order.exchange_id.to_string())
                            .arg("buy_order_book_id")
                            .arg(&order.buy_order_book_id.to_string())
                            .arg("unique_id")
                            .arg(&order.unique_id)
                            .arg("price_level")
                            .arg(&order.price_level.to_string())
                            .arg("buy_quantity")
                            .arg(&order.new_buy_quantity.to_string());

                        delete_orders.push(unique_id);
                        modified_orders.push(order);
                    } else if bid.event == "delete" {
                        pipe.atomic()
                            .cmd("HDEL")
                            .arg(format!("buy_order:{}", bid.order_id))
                            .arg("symbol")
                            .arg(&order_book.symbol)
                            .arg("exchange")
                            .arg(&order_book.exchange)
                            .arg("security_id")
                            .arg(&order_book.security_id.to_string())
                            .arg("exchange_id")
                            .arg(&order_book.exchange_id.to_string())
                            .arg("buy_order_book_id")
                            .arg(&order_book.buy_order_book_id.to_string())
                            .arg("unique_id")
                            .arg(&bid.order_id)
                            .arg("price_level")
                            .arg(&bid.limit_price.to_string())
                            .arg("buy_quantity")
                            .arg(&bid.order_qty.to_string());

                        {
                            bid_volume -= &bid.order_qty;
                        }

                        delete_orders.push(&bid.order_id);
                    }
                }
                match pipe.query_async::<_, ()>(&mut bid_connection).await {
                    Ok(_) => {
                        println!("Batch added, modified, and deleted buy orders to/from redis")
                    }
                    Err(e) => println!(
                        "Failed to batch add, modify, and delete buy orders to/from redis: {}",
                        e
                    ),
                }
                create_open_buy_orders(postgres_pool.clone(), buy_orders).await;
                create_modified_buy_orders(postgres_pool.clone(), modified_orders).await;
                delete_open_buy_orders(postgres_pool.clone(), delete_orders.clone()).await;
                delete_modified_buy_orders(postgres_pool.clone(), delete_orders.clone()).await;
            };

            let ask_futures = async {
                let mut sell_orders = Vec::new();
                let mut modified_orders = Vec::new();
                let mut delete_orders = Vec::new();
                let mut pipe = redis::pipe();

                for ask in update_data.asks.iter() {
                    if ask.event == "add" {
                        let price_level = &ask.limit_price;
                        let sell_quantity = &ask.order_qty;
                        let unique_id = &ask.order_id;

                        {
                            ask_volume += sell_quantity;
                        }

                        let order = NewOpenSellOrder::new(
                            &order_book.symbol,
                            &order_book.exchange,
                            order_book.security_id,
                            order_book.exchange_id,
                            order_book.sell_order_book_id,
                            unique_id,
                            price_level,
                            sell_quantity,
                        );

                        pipe.atomic()
                            .cmd("HSET")
                            .arg(format!("sell_order:{}", order.unique_id))
                            .arg("symbol")
                            .arg(&order.symbol)
                            .arg("exchange")
                            .arg(&order.exchange)
                            .arg("security_id")
                            .arg(&order.security_id.to_string())
                            .arg("exchange_id")
                            .arg(&order.exchange_id.to_string())
                            .arg("sell_order_book_id")
                            .arg(&order.sell_order_book_id.to_string())
                            .arg("unique_id")
                            .arg(&order.unique_id)
                            .arg("price_level")
                            .arg(&order.price_level.to_string())
                            .arg("sell_quantity")
                            .arg(&order.sell_quantity.to_string());

                        sell_orders.push(order);
                    } else if ask.event == "modify" {
                        let price_level = &ask.limit_price;
                        let new_sell_quantity = &ask.order_qty;
                        let unique_id = &ask.order_id;

                        {
                            ask_volume += new_sell_quantity;
                        }

                        let order = NewModifiedSellOrder::new(
                            &order_book.symbol,
                            &order_book.exchange,
                            order_book.security_id,
                            order_book.exchange_id,
                            order_book.sell_order_book_id,
                            unique_id,
                            price_level,
                            new_sell_quantity,
                        );

                        pipe.atomic()
                            .cmd("HSET")
                            .arg(format!("sell_order:{}", order.unique_id))
                            .arg("symbol")
                            .arg(&order.symbol)
                            .arg("exchange")
                            .arg(&order.exchange)
                            .arg("security_id")
                            .arg(&order.security_id.to_string())
                            .arg("exchange_id")
                            .arg(&order.exchange_id.to_string())
                            .arg("sell_order_book_id")
                            .arg(&order.sell_order_book_id.to_string())
                            .arg("unique_id")
                            .arg(&order.unique_id)
                            .arg("price_level")
                            .arg(&order.price_level.to_string())
                            .arg("sell_quantity")
                            .arg(&order.new_sell_quantity.to_string());

                        delete_orders.push(unique_id);
                        modified_orders.push(order);
                    } else if ask.event == "delete" {
                        pipe.atomic()
                            .cmd("HDEL")
                            .arg(format!("sell_order:{}", ask.order_id))
                            .arg("symbol")
                            .arg(&order_book.symbol)
                            .arg("exchange")
                            .arg(&order_book.exchange)
                            .arg("security_id")
                            .arg(&order_book.security_id.to_string())
                            .arg("exchange_id")
                            .arg(&order_book.exchange_id.to_string())
                            .arg("sell_order_book_id")
                            .arg(&order_book.sell_order_book_id.to_string())
                            .arg("unique_id")
                            .arg(&ask.order_id)
                            .arg("price_level")
                            .arg(&ask.limit_price.to_string())
                            .arg("sell_quantity")
                            .arg(&ask.order_qty.to_string());

                        {
                            ask_volume -= &ask.order_qty;
                        }

                        delete_orders.push(&ask.order_id);
                    }
                }
                match pipe.query_async::<_, ()>(&mut ask_connection).await {
                    Ok(_) => {
                        println!("Batch added, modified, and deleted buy orders to/from redis")
                    }
                    Err(e) => println!(
                        "Failed to batch add, modify, and delete buy orders to/from redis: {}",
                        e
                    ),
                }
                create_open_sell_orders(postgres_pool.clone(), sell_orders).await;
                create_modified_sell_orders(postgres_pool.clone(), modified_orders).await;
                delete_open_sell_orders(postgres_pool.clone(), delete_orders.clone()).await;
                delete_modified_sell_orders(postgres_pool.clone(), delete_orders.clone()).await;
            };

            tokio::join!(bid_futures, ask_futures);

            total_volume += bid_volume + ask_volume;

            match cmd("HSET")
                .arg(format!("order_book:{}", order_book.order_book_id))
                .arg("created_at")
                .arg(&order_book.created_at.to_string())
                .arg("updated_at")
                .arg(&order_book.updated_at.expect("").to_string())
                .arg("symbol")
                .arg(&order_book.symbol)
                .arg("exchange")
                .arg(&order_book.exchange)
                .arg("security_id")
                .arg(&order_book.security_id.to_string())
                .arg("exchange_id")
                .arg(&order_book.exchange_id.to_string())
                .arg("buy_order_book_id")
                .arg(&order_book.buy_order_book_id.to_string())
                .arg("sell_order_book_id")
                .arg(&order_book.sell_order_book_id.to_string())
                .arg("total_volume")
                .arg(&total_volume.to_string())
                .query_async::<_, ()>(&mut book_connection)
                .await
            {
                Ok(_) => {
                    println!("Updated order book in redis");
                }
                Err(e) => {
                    println!("Failed to update order book in redis: {}", e);
                }
            }
            update_orderbook(postgres_pool.clone(), order_book, total_volume).await;
            Ok(())
        } else {
            println!("Channel is not level3");
            Ok(())
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
struct Level3UpdateData {
    symbol: String,
    checksum: i64,
    bids: Vec<UpdateOrder>,
    asks: Vec<UpdateOrder>,
}

#[derive(Debug, Serialize, Deserialize)]
struct UpdateOrder {
    event: String,
    order_id: String,
    limit_price: BigDecimal,
    order_qty: BigDecimal,
    timestamp: String,
}

//-------------------------------------------------------------------------

#[derive(Debug, Serialize, Deserialize)]
pub struct TradeSnapshot {
    channel: String,
    r#type: String,
    data: Vec<TradeData>,
}

impl TradeSnapshot {
    pub fn from_json(json: &serde_json::Value) -> Result<TradeSnapshot> {
        let snapshot: TradeSnapshot = serde_json::from_value(json.clone())?;
        Ok(snapshot)
    }

    pub async fn process_snapshot(
        &self,
        redis_pool: Arc<Pool<Manager, Connection>>,
        postgres_pool: Arc<Pool<CustomAsyncPgConnectionManager>>,
        order_book: OrderBook
    ) -> Result<()> {
        let mut trade_connection = create_redis_connection(&redis_pool)
            .await
            .expect("Failed to create redis connection");

        if self.channel == "trade" {
            if self.data.is_empty() {
                println!("Trade data is empty");
            }

            let mut trades = Vec::new();
            let mut pipe = redis::pipe();

            for trade in self.data.iter() {
                let new_trade = NewTrade::new(
                    &trade.symbol,
                    &order_book.exchange,
                    order_book.security_id,
                    order_book.exchange_id,
                    &trade.side,
                    &trade.price,
                    &trade.qty,
                );

                pipe.atomic()
                    .cmd("HSET")
                    .arg(format!("trade:{}", trade.trade_id))
                    .arg("symbol")
                    .arg(&trade.symbol)
                    .arg("side")
                    .arg(&trade.side)
                    .arg("price")
                    .arg(&trade.price.to_string())
                    .arg("qty")
                    .arg(&trade.qty.to_string())
                    .arg("ord_type")
                    .arg(&trade.ord_type)
                    .arg("trade_id")
                    .arg(&trade.trade_id)
                    .arg("timestamp")
                    .arg(&trade.timestamp);

                trades.push(new_trade);
            }

            match pipe.query_async::<_, ()>(&mut trade_connection).await {
                Ok(_) => println!("Added trades to redis"),
                Err(e) => println!("Failed to add trades to redis: {}", e),
            }

            create_trades(postgres_pool.clone(), trades).await;

            Ok(())
        } else {
            println!("Channel is not trade");
            Ok(())
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
struct TradeData {
    symbol: String,
    side: String,
    price: BigDecimal,
    qty: BigDecimal,
    ord_type: String,
    trade_id: u64,
    timestamp: String,
}

//-------------------------------------------------------------------------

#[derive(Debug, Serialize, Deserialize)]
pub struct TradeUpdate {
    channel: String,
    r#type: String,
    data: Vec<TradeData>,
}

impl TradeUpdate {
    pub fn from_json(json: &serde_json::Value) -> Result<TradeUpdate> {
        let update: TradeUpdate = serde_json::from_value(json.clone())?;
        Ok(update)
    }

    pub async fn process_update(
        &self,
        redis_pool: Arc<Pool<Manager, Connection>>,
        postgres_pool: Arc<Pool<CustomAsyncPgConnectionManager>>,
        order_book: OrderBook
    ) -> Result<()> {
        let mut trade_connection = create_redis_connection(&redis_pool)
            .await
            .expect("Failed to create redis connection");

        if self.channel == "trade" {
            if self.data.is_empty() {
                println!("Trade data is empty");
            }

            let mut trades = Vec::new();
            let mut pipe = redis::pipe();

            for trade in self.data.iter() {
                let new_trade = NewTrade::new(
                    &trade.symbol,
                    &order_book.exchange,
                    order_book.security_id,
                    order_book.exchange_id,
                    &trade.side,
                    &trade.price,
                    &trade.qty,
                );

                pipe.atomic()
                    .cmd("HSET")
                    .arg(format!("trade:{}", trade.trade_id))
                    .arg("symbol")
                    .arg(&trade.symbol)
                    .arg("side")
                    .arg(&trade.side)
                    .arg("price")
                    .arg(&trade.price.to_string())
                    .arg("qty")
                    .arg(&trade.qty.to_string())
                    .arg("ord_type")
                    .arg(&trade.ord_type)
                    .arg("trade_id")
                    .arg(&trade.trade_id)
                    .arg("timestamp")
                    .arg(&trade.timestamp);

                trades.push(new_trade);
            }

            match pipe.query_async::<_, ()>(&mut trade_connection).await {
                Ok(_) => println!("Added trades to redis"),
                Err(e) => println!("Failed to add trades to redis: {}", e),
            }

            create_trades(postgres_pool.clone(), trades).await;

            Ok(())
        } else {
            println!("Channel is not trade");
            Ok(())
        }
    }
}

pub fn parse_message(msg: &str) -> Result<Response> {
    // First, parse the string into a serde_json::Value
    let json_msg: Value =
        serde_json::from_str(msg).map_err(|e| anyhow!("Failed to parse message: {}", e))?;

    if json_msg.get("result").is_some() && json_msg["result"].get("token").is_some() {
        let token_response = TokenResponse::from_json(&json_msg)?;
        Ok(Response::TokenResponse(token_response))
    } else if json_msg.get("channel").is_some() && json_msg["channel"] == "status" {
        let status = Status::from_json(&json_msg)?;
        Ok(Response::Status(status))
    } else if json_msg.get("channel").is_some() && json_msg["channel"] == "heartbeat" {
        let heartbeat = HeartBeat::from_json(&json_msg)?;
        Ok(Response::HeartBeat(heartbeat))
    } else if json_msg.get("method").is_some()
        && json_msg["method"] == "subscribe"
        && json_msg.get("params").is_some()
    {
        let subscribe = Level3Subscribe::from_json(&json_msg)?;
        Ok(Response::Level3Subscribe(subscribe))
    } else if json_msg.get("method").is_some()
        && json_msg["method"] == "subscribe"
        && json_msg.get("result").is_some()
    {
        let subscribe_ack = Level3SubscribeAcknowledgement::from_json(&json_msg)?;
        Ok(Response::Level3SubscribeAck(subscribe_ack))
    } else if json_msg.get("method").is_some()
        && json_msg["method"] == "unsubscribe"
        && json_msg.get("params").is_some()
    {
        let unsubscribe = Level3Unsubscribe::from_json(&json_msg)?;
        Ok(Response::Level3Unsubscribe(unsubscribe))
    } else if json_msg.get("method").is_some()
        && json_msg["method"] == "unsubscribe"
        && json_msg.get("result").is_some()
    {
        let unsubscribe_ack = Level3UnsubscribeAcknowledgement::from_json(&json_msg)?;
        Ok(Response::Level3UnsubscribeAck(unsubscribe_ack))
    } else if json_msg.get("channel").is_some()
        && json_msg["channel"] == "level3"
        && json_msg.get("type").is_some()
        && json_msg["type"] == "snapshot"
    {
        let snapshot = Level3Snapshot::from_json(&json_msg)?;
        Ok(Response::Level3Snapshot(snapshot))
    } else if json_msg.get("channel").is_some()
        && json_msg["channel"] == "level3"
        && json_msg.get("type").is_some()
        && json_msg["type"] == "update"
    {
        let update = Level3Update::from_json(&json_msg)?;
        Ok(Response::Level3Update(update))
    } else if json_msg.get("channel").is_some()
        && json_msg["channel"] == "trade"
        && json_msg.get("type").is_some()
        && json_msg["type"] == "snapshot"
    {
        let trade = TradeSnapshot::from_json(&json_msg)?;
        Ok(Response::TradeSnapshot(trade))
    } else if json_msg.get("channel").is_some()
        && json_msg["channel"] == "trade"
        && json_msg.get("type").is_some()
        && json_msg["type"] == "update"
    {
        let update = TradeUpdate::from_json(&json_msg)?;
        Ok(Response::TradeUpdate(update))
    } else {
        Err(anyhow!("Failed to parse message: {:?}", json_msg))
    }
}