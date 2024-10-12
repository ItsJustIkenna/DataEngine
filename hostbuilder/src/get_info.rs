use std::sync::Arc;

use databaseschema::{
    models::{Exchange, NewExchange, NewOrderBook, NewSecurity, OrderBook, Security},
    ops::{
        exchanges_ops::{create_exchange, exchange_exists, get_exchanges_by_name},
        order_book_ops::{create_orderbook, get_orderbook_by_exchange_id, orderbook_exists},
        securities_ops::{create_security, get_security_by_symbol, security_exists},
    },
    CustomAsyncPgConnectionManager,
};
use deadpool::managed::Pool;
use deadpool_redis::{Connection, Manager};
use redis::cmd;
use redis_utils::create_redis_connection;

pub async fn get_exchange(
    redis_pool: Arc<Pool<Manager, Connection>>,
    postgres_pool: Arc<Pool<CustomAsyncPgConnectionManager>>,
    exchange: String,
) -> Exchange {
    let mut connection = create_redis_connection(&redis_pool)
        .await
        .expect("Failed to create redis connection");

    if exchange_exists(postgres_pool.clone(), &exchange).await {
        println!("Exchange exists");
        let exchange = get_exchanges_by_name(postgres_pool.clone(), &exchange).await;

        match cmd("HSET")
            .arg(format!("exchange:{}", &exchange.exchange_id))
            .arg("created_at")
            .arg(&exchange.created_at.to_string())
            .arg("exchange_id")
            .arg(&exchange.exchange_id.to_string())
            .arg("exchange")
            .arg(&exchange.exchange)
            .query_async::<_, ()>(&mut connection)
            .await
        {
            Ok(_) => {
                println!("Successfully saved exchange to Redis with key exchange:{}", exchange.exchange_id);
                exchange
            }
            Err(e) => {
                println!("Failed to save exchange to redis: {}", e);
                exchange
            }
        }
    } else {
        println!("Exchange does not exist");
        let new_exchange = NewExchange::new(&exchange);
        let exchange = create_exchange(postgres_pool.clone(), new_exchange).await;

        match cmd("HSET")
            .arg(format!("exchange:{}", &exchange.exchange_id))
            .arg("created_at")
            .arg(&exchange.created_at.to_string())
            .arg("exchange_id")
            .arg(&exchange.exchange_id.to_string())
            .arg("exchange")
            .arg(&exchange.exchange)
            .query_async::<_, ()>(&mut connection)
            .await
        {
            Ok(_) => {
                println!("Successfully saved exchange to Redis with key exchange:{}", exchange.exchange_id);
                exchange
            }
            Err(e) => {
                println!("Failed to save exchange to redis: {}", e);
                exchange
            }
        }
    }
}

pub async fn get_securities(
    redis_pool: Arc<Pool<Manager, Connection>>,
    postgres_pool: Arc<Pool<CustomAsyncPgConnectionManager>>,
    symbols: &Vec<String>,
) -> Vec<Security> {
    let mut connection = create_redis_connection(&redis_pool)
        .await
        .expect("Failed to create redis connection");

    let mut pipe = redis::pipe();

    let mut securities = vec![];
    for symbol in symbols.iter() {
        if security_exists(postgres_pool.clone(), symbol).await {
            println!("Security exists");
            let security = get_security_by_symbol(postgres_pool.clone(), symbol).await;
            pipe.atomic()
                .cmd("HSET")
                .arg(format!("security:{}", &security.security_id))
                .arg("created_at")
                .arg(&security.created_at.to_string())
                .arg("security_id")
                .arg(&security.security_id.to_string())
                .arg("symbol")
                .arg(&security.symbol);
            securities.push(security);
        } else {
            println!("Security does not exist");
            let new_security = NewSecurity::new(symbol);
            let security = create_security(postgres_pool.clone(), new_security).await;

            pipe.atomic()
                .cmd("HSET")
                .arg(format!("security:{}", &security.security_id))
                .arg("created_at")
                .arg(&security.created_at.to_string())
                .arg("security_id")
                .arg(&security.security_id.to_string())
                .arg("symbol")
                .arg(&security.symbol);

            securities.push(security);
        }
    }

    match pipe.query_async::<_, ()>(&mut connection).await {
        Ok(_) => {
            println!("Securities saved to redis");
            securities
        }
        Err(e) => {
            println!("Failed to save securities to redis: {}", e);
            securities
        }
    }
}

pub async fn get_orderbooks(
    redis_pool: Arc<Pool<Manager, Connection>>,
    postgres_pool: Arc<Pool<CustomAsyncPgConnectionManager>>,
    securities: Vec<Security>,
    exchange: &Exchange,
) -> Vec<OrderBook> {
    let mut connection = create_redis_connection(&redis_pool)
        .await
        .expect("Failed to create redis connection");

    let mut pipe = redis::pipe();

    let mut orderbooks = vec![];
    for security in securities.iter() {
        if orderbook_exists(postgres_pool.clone(), &exchange.exchange_id).await {
            println!("Orderbook exists");
            let order_book =
                get_orderbook_by_exchange_id(postgres_pool.clone(), &exchange.exchange_id).await;

            pipe.atomic()
                .cmd("HSET")
                .arg(format!("order_book:{}", &order_book.order_book_id))
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
                .arg("order_book_id")
                .arg(&order_book.order_book_id.to_string())
                .arg("buy_order_book_id")
                .arg(&order_book.buy_order_book_id.to_string())
                .arg("sell_order_book_id")
                .arg(&order_book.sell_order_book_id.to_string())
                .arg("total_volume")
                .arg(&order_book.total_volume.to_string());

            orderbooks.push(order_book);
        } else {
            println!("Orderbook does not exist");
            let new_orderbook = NewOrderBook::new(
                &security.symbol,
                &exchange.exchange,
                security.security_id,
                exchange.exchange_id,
            );
            let order_book = create_orderbook(postgres_pool.clone(), new_orderbook).await;

            pipe.atomic()
                .cmd("HSET")
                .arg(format!("order_book:{}", &order_book.order_book_id))
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
                .arg("order_book_id")
                .arg(&order_book.order_book_id.to_string())
                .arg("buy_order_book_id")
                .arg(&order_book.buy_order_book_id.to_string())
                .arg("sell_order_book_id")
                .arg(&order_book.sell_order_book_id.to_string())
                .arg("total_volume")
                .arg(&order_book.total_volume.to_string());

            orderbooks.push(order_book);
        }
    }

    match pipe.query_async::<_, ()>(&mut connection).await {
        Ok(_) => {
            println!("Orderbooks saved to redis");
            orderbooks
        }
        Err(e) => {
            println!("Failed to save orderbooks to redis: {}", e);
            orderbooks
        }
    }
}
