create table if not exists crypto_prices  (
    id serial primary key,
    btc_price DOUBLE PRECISION,
    eth_price DOUBLE PRECISION,
    price_diff DOUBLE PRECISION,
    event_time TIMESTAMP 
);