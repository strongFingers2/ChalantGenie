CREATE TABLE IF NOT EXISTS processed_orders (
    order_id VARCHAR(64),
    product_id VARCHAR(64) NOT NULL,
    PRIMARY KEY (order_id, product_id)
    );
CREATE TABLE IF NOT EXISTS inventory (
    product_id VARCHAR(64) PRIMARY KEY,
    stock INT NOT NULL CHECK (stock >= 0)
    );
CREATE TABLE IF NOT EXISTS outbox (
    sequence_id BIGSERIAL PRIMARY KEY,
    order_id VARCHAR(64),
    product_id VARCHAR(64),
    reserved BOOLEAN NOT NULL,
    sent_marker TIMESTAMP
    );