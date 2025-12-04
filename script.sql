CREATE DATABASE IF NOT EXISTS olist_ecommerce;

USE olist_ecommerce;

SET GLOBAL local_infile = 1;

SET FOREIGN_KEY_CHECKS = 0;

DROP TABLE IF EXISTS olist_order_reviews_dataset;

DROP TABLE IF EXISTS olist_order_payments_dataset;

DROP TABLE IF EXISTS olist_order_items_dataset;

DROP TABLE IF EXISTS olist_orders_dataset;

DROP TABLE IF EXISTS product_category_name_translation;

DROP TABLE IF EXISTS olist_products_dataset;

DROP TABLE IF EXISTS olist_sellers_dataset;

DROP TABLE IF EXISTS olist_customers_dataset;

DROP TABLE IF EXISTS olist_geolocation_dataset;

CREATE TABLE olist_geolocation_dataset (
    geolocation_zip_code_prefix VARCHAR(5) NOT NULL,
    geolocation_lat DECIMAL(15, 10),
    geolocation_lng DECIMAL(15, 10),
    geolocation_city VARCHAR(100),
    geolocation_state CHAR(2),
    PRIMARY KEY (
        geolocation_zip_code_prefix,
        geolocation_lat,
        geolocation_lng
    )
);

CREATE TABLE olist_customers_dataset (
    customer_id VARCHAR(32) PRIMARY KEY,
    customer_unique_id VARCHAR(32) UNIQUE NOT NULL,
    customer_zip_code_prefix VARCHAR(5) NOT NULL,
    customer_city VARCHAR(100) NOT NULL,
    customer_state CHAR(2) NOT NULL,
    INDEX idx_customer_zip (customer_zip_code_prefix)
);

CREATE TABLE olist_sellers_dataset (
    seller_id VARCHAR(32) PRIMARY KEY,
    seller_zip_code_prefix VARCHAR(5) NOT NULL,
    seller_city VARCHAR(100) NOT NULL,
    seller_state CHAR(2) NOT NULL,
    INDEX idx_seller_zip (seller_zip_code_prefix)
);

CREATE TABLE olist_products_dataset (
    product_id VARCHAR(32) PRIMARY KEY,
    product_category_name VARCHAR(100),
    product_name_length INT,
    product_description_length INT,
    product_photos_qty INT,
    product_weight_g DECIMAL(10, 2),
    product_length_cm DECIMAL(10, 2),
    product_height_cm DECIMAL(10, 2),
    product_width_cm DECIMAL(10, 2)
);

CREATE TABLE product_category_name_translation (
    product_category_name VARCHAR(100) PRIMARY KEY,
    product_category_name_english VARCHAR(100)
);

CREATE TABLE olist_orders_dataset (
    order_id VARCHAR(32) PRIMARY KEY,
    customer_id VARCHAR(32) NOT NULL,
    order_status VARCHAR(20),
    order_purchase_timestamp DATETIME,
    order_approved_at DATETIME,
    order_delivered_carrier_date DATETIME,
    order_delivered_customer_date DATETIME,
    order_estimated_delivery_date DATETIME,
    INDEX idx_customer_id (customer_id)
);

CREATE TABLE olist_order_items_dataset (
    order_id VARCHAR(32) NOT NULL,
    order_item_id INT NOT NULL,
    product_id VARCHAR(32) NOT NULL,
    seller_id VARCHAR(32) NOT NULL,
    shipping_limit_date DATETIME,
    price DECIMAL(10, 2),
    freight_value DECIMAL(10, 2),
    PRIMARY KEY (order_id, order_item_id),
    INDEX idx_product_id (product_id),
    INDEX idx_seller_id (seller_id)
);

CREATE TABLE olist_order_payments_dataset (
    order_id VARCHAR(32) NOT NULL,
    payment_sequential INT NOT NULL,
    payment_type VARCHAR(20),
    payment_installments INT,
    payment_value DECIMAL(10, 2),
    PRIMARY KEY (order_id, payment_sequential)
);

CREATE TABLE olist_order_reviews_dataset (
    review_id VARCHAR(32) PRIMARY KEY,
    order_id VARCHAR(32) NOT NULL,
    review_score INT,
    review_comment_title VARCHAR(255),
    review_comment_message TEXT,
    review_creation_date DATETIME,
    review_answer_timestamp DATETIME,
    INDEX idx_order_id (order_id)
);

SET FOREIGN_KEY_CHECKS = 1;

ALTER TABLE olist_orders_dataset
ADD CONSTRAINT fk_orders_customer FOREIGN KEY (customer_id) REFERENCES olist_customers_dataset (customer_id) ON UPDATE RESTRICT ON DELETE RESTRICT;

ALTER TABLE olist_order_items_dataset
ADD CONSTRAINT fk_items_order FOREIGN KEY (order_id) REFERENCES olist_orders_dataset (order_id) ON UPDATE RESTRICT ON DELETE RESTRICT;

ALTER TABLE olist_order_items_dataset
ADD CONSTRAINT fk_items_product FOREIGN KEY (product_id) REFERENCES olist_products_dataset (product_id) ON UPDATE RESTRICT ON DELETE RESTRICT;

ALTER TABLE olist_order_items_dataset
ADD CONSTRAINT fk_items_seller FOREIGN KEY (seller_id) REFERENCES olist_sellers_dataset (seller_id) ON UPDATE RESTRICT ON DELETE RESTRICT;

ALTER TABLE olist_order_reviews_dataset
ADD CONSTRAINT fk_reviews_order FOREIGN KEY (order_id) REFERENCES olist_orders_dataset (order_id) ON UPDATE RESTRICT ON DELETE RESTRICT;

ALTER TABLE olist_order_payments_dataset
ADD CONSTRAINT fk_payments_order FOREIGN KEY (order_id) REFERENCES olist_orders_dataset (order_id) ON UPDATE RESTRICT ON DELETE RESTRICT;

ALTER TABLE olist_products_dataset
ADD CONSTRAINT fk_product_category_translation FOREIGN KEY (product_category_name) REFERENCES product_category_name_translation (product_category_name) ON UPDATE RESTRICT ON DELETE RESTRICT;

ALTER TABLE olist_products_dataset
ADD INDEX idx_product_category_name (product_category_name);

SET @path = '/home/oagarian/bcdd/';

LOAD DATA LOCAL INFILE '/home/oagarian/bcdd/olist_geolocation_dataset.csv' INTO
TABLE olist_geolocation_dataset CHARACTER SET utf8mb4 FIELDS TERMINATED BY ',' ENCLOSED BY '"' LINES TERMINATED BY '\n' IGNORE 1 ROWS (
    geolocation_zip_code_prefix,
    geolocation_lat,
    geolocation_lng,
    geolocation_city,
    geolocation_state
);

LOAD DATA LOCAL INFILE '/home/oagarian/bcdd/olist_customers_dataset.csv' INTO
TABLE olist_customers_dataset CHARACTER SET utf8mb4 FIELDS TERMINATED BY ',' ENCLOSED BY '"' LINES TERMINATED BY '\n' IGNORE 1 ROWS (
    customer_id,
    customer_unique_id,
    customer_zip_code_prefix,
    customer_city,
    customer_state
);

LOAD DATA LOCAL INFILE '/home/oagarian/bcdd/olist_sellers_dataset.csv' INTO
TABLE olist_sellers_dataset CHARACTER SET utf8mb4 FIELDS TERMINATED BY ',' ENCLOSED BY '"' LINES TERMINATED BY '\n' IGNORE 1 ROWS (
    seller_id,
    seller_zip_code_prefix,
    seller_city,
    seller_state
);

LOAD DATA LOCAL INFILE '/home/oagarian/bcdd/product_category_name_translation.csv' INTO
TABLE product_category_name_translation CHARACTER SET utf8mb4 FIELDS TERMINATED BY ',' ENCLOSED BY '"' LINES TERMINATED BY '\n' IGNORE 1 ROWS (
    product_category_name,
    product_category_name_english
);

LOAD DATA LOCAL INFILE '/home/oagarian/bcdd/olist_products_dataset.csv' INTO
TABLE olist_products_dataset CHARACTER SET utf8mb4 FIELDS TERMINATED BY ',' ENCLOSED BY '"' LINES TERMINATED BY '\n' IGNORE 1 ROWS (
    product_id,
    product_category_name,
    product_name_length,
    product_description_length,
    product_photos_qty,
    product_weight_g,
    product_length_cm,
    product_height_cm,
    product_width_cm
);

LOAD DATA LOCAL INFILE '/home/oagarian/bcdd/olist_orders_dataset.csv' INTO
TABLE olist_orders_dataset CHARACTER SET utf8mb4 FIELDS TERMINATED BY ',' ENCLOSED BY '"' LINES TERMINATED BY '\n' IGNORE 1 ROWS (
    order_id,
    customer_id,
    order_status,
    order_purchase_timestamp,
    order_approved_at,
    order_delivered_carrier_date,
    order_delivered_customer_date,
    order_estimated_delivery_date
);

LOAD DATA LOCAL INFILE '/home/oagarian/bcdd/olist_order_items_dataset.csv' INTO
TABLE olist_order_items_dataset CHARACTER SET utf8mb4 FIELDS TERMINATED BY ',' ENCLOSED BY '"' LINES TERMINATED BY '\n' IGNORE 1 ROWS (
    order_id,
    order_item_id,
    product_id,
    seller_id,
    shipping_limit_date,
    price,
    freight_value
);

LOAD DATA LOCAL INFILE '/home/oagarian/bcdd/olist_order_payments_dataset.csv' INTO
TABLE olist_order_payments_dataset CHARACTER SET utf8mb4 FIELDS TERMINATED BY ',' ENCLOSED BY '"' LINES TERMINATED BY '\n' IGNORE 1 ROWS (
    order_id,
    payment_sequential,
    payment_type,
    payment_installments,
    payment_value
);

LOAD DATA LOCAL INFILE '/home/oagarian/bcdd/olist_order_reviews_dataset.csv' INTO
TABLE olist_order_reviews_dataset CHARACTER SET utf8mb4 FIELDS TERMINATED BY ',' ENCLOSED BY '"' LINES TERMINATED BY '\n' IGNORE 1 ROWS (
    review_id,
    order_id,
    review_score,
    review_comment_title,
    review_comment_message,
    review_creation_date,
    review_answer_timestamp
);

-- CRIAÇÃO DO USUÁRIO DE BI
DROP USER IF EXISTS 'bi_user' @'%';

CREATE USER 'bi_user' @'%' IDENTIFIED BY 'SenhaForte@123';

GRANT SELECT ON olist_ecommerce.* TO 'bi_user' @'%';

FLUSH PRIVILEGES;

-- 5.1

SELECT s.seller_id, s.seller_city, s.seller_state, SUM(oi.price + oi.freight_value) AS total_faturado
FROM
    olist_order_items_dataset AS oi
    JOIN olist_sellers_dataset AS s ON s.seller_id = oi.seller_id
GROUP BY
    s.seller_id,
    s.seller_city,
    s.seller_state
ORDER BY total_faturado DESC;

-- 5.2

SELECT
    c.customer_unique_id,
    COUNT(DISTINCT o.order_id) AS total_pedidos,
    SUM(p.payment_value) AS total_gasto
FROM
    olist_orders_dataset AS o
    JOIN olist_customers_dataset AS c ON c.customer_id = o.customer_id
    JOIN olist_order_payments_dataset AS p ON p.order_id = o.order_id
WHERE
    o.order_purchase_timestamp BETWEEN '2017-01-01' AND '2017-12-31'
GROUP BY
    c.customer_unique_id
ORDER BY total_gasto DESC, total_pedidos DESC
LIMIT 10;

-- 5.3

SELECT
    s.seller_id,
    AVG(r.review_score) AS media_avaliacao,
    COUNT(DISTINCT r.review_id) AS quantidade_avaliacoes
FROM
    olist_order_reviews_dataset AS r
    JOIN olist_orders_dataset AS o ON o.order_id = r.order_id
    JOIN olist_order_items_dataset AS oi ON oi.order_id = o.order_id
    JOIN olist_sellers_dataset AS s ON s.seller_id = oi.seller_id
GROUP BY
    s.seller_id
ORDER BY media_avaliacao DESC;

-- 5.4
SELECT o.order_id, o.order_status, o.order_purchase_timestamp, c.customer_unique_id, c.customer_city, c.customer_state, SUM(p.payment_value) AS total_pago
FROM
    olist_orders_dataset AS o
    JOIN olist_customers_dataset AS c ON c.customer_id = o.customer_id
    LEFT JOIN olist_order_payments_dataset AS p ON p.order_id = o.order_id
WHERE
    o.order_purchase_timestamp BETWEEN '2017-01-01' AND '2017-01-31'
GROUP BY
    o.order_id,
    o.order_status,
    o.order_purchase_timestamp,
    c.customer_unique_id,
    c.customer_city,
    c.customer_state
ORDER BY o.order_purchase_timestamp;

-- 5.5

SELECT p.product_id, p.product_category_name, COUNT(*) AS quantidade_vendida
FROM
    olist_order_items_dataset AS oi
    JOIN olist_orders_dataset AS o ON o.order_id = oi.order_id
    JOIN olist_products_dataset AS p ON p.product_id = oi.product_id
WHERE
    o.order_purchase_timestamp BETWEEN '2017-01-01' AND '2017-12-31'
GROUP BY
    p.product_id,
    p.product_category_name
ORDER BY quantidade_vendida DESC
LIMIT 5;

-- 5.6

SELECT o.order_id, c.customer_unique_id, c.customer_city, c.customer_state, o.order_estimated_delivery_date, o.order_delivered_customer_date, DATEDIFF(
        o.order_delivered_customer_date, o.order_estimated_delivery_date
    ) AS dias_atraso
FROM
    olist_orders_dataset AS o
    JOIN olist_customers_dataset AS c ON c.customer_id = o.customer_id
WHERE
    o.order_delivered_customer_date IS NOT NULL
    AND o.order_estimated_delivery_date IS NOT NULL
    AND o.order_delivered_customer_date BETWEEN '2017-01-01' AND '2017-12-31'
    AND DATEDIFF(
        o.order_delivered_customer_date,
        o.order_estimated_delivery_date
    ) > 0
ORDER BY dias_atraso DESC
LIMIT 10;

-- 5.7
SELECT c.customer_unique_id, SUM(p.payment_value) AS total_gasto
FROM
    olist_orders_dataset AS o
    JOIN olist_customers_dataset AS c ON c.customer_id = o.customer_id
    JOIN olist_order_payments_dataset AS p ON p.order_id = o.order_id
GROUP BY
    c.customer_unique_id
ORDER BY total_gasto DESC
LIMIT 10;

-- 5.8
SELECT
    c.customer_state,
    AVG(
        DATEDIFF(
            o.order_delivered_customer_date,
            o.order_delivered_carrier_date
        )
    ) AS tempo_medio_entrega_dias,
    COUNT(*) AS quantidade_pedidos
FROM
    olist_orders_dataset AS o
    JOIN olist_customers_dataset AS c ON c.customer_id = o.customer_id
WHERE
    o.order_delivered_customer_date IS NOT NULL
    AND o.order_delivered_carrier_date IS NOT NULL
GROUP BY
    c.customer_state
ORDER BY tempo_medio_entrega_dias DESC;

-- Pro desafio eu pensei em usar uma fórmula chamada "Haversine", mas não sei como implementar em SQL de maneira efetiva

-- OTIMIZAÇÃO DE QUERYS

CREATE INDEX idx_orders_purchase_timestamp ON olist_orders_dataset (order_purchase_timestamp);

CREATE INDEX idx_orders_delivered_customer_date ON olist_orders_dataset (
    order_delivered_customer_date,
    order_estimated_delivery_date
);

CREATE INDEX idx_order_items_order_seller ON olist_order_items_dataset (order_id, seller_id);

CREATE INDEX idx_customers_state ON olist_customers_dataset (customer_state);

-- 6.1 Versão otimizada da query 5.1
SELECT s.seller_id, s.seller_city, s.seller_state, SUM(oi.price + oi.freight_value) AS total_faturado
FROM
    olist_order_items_dataset AS oi
    JOIN olist_sellers_dataset AS s ON s.seller_id = oi.seller_id
GROUP BY
    s.seller_id,
    s.seller_city,
    s.seller_state
ORDER BY total_faturado DESC;

-- 6.2 Versão otimizada da query 5.2
WITH
    resumo_clientes AS (
        SELECT
            o.customer_id,
            COUNT(DISTINCT o.order_id) AS total_pedidos,
            SUM(p.payment_value) AS total_gasto
        FROM
            olist_orders_dataset AS o
            JOIN olist_order_payments_dataset AS p ON p.order_id = o.order_id
        WHERE
            o.order_purchase_timestamp BETWEEN '2017-01-01' AND '2017-12-31'
        GROUP BY
            o.customer_id
    )
SELECT c.customer_unique_id, r.total_pedidos, r.total_gasto
FROM
    resumo_clientes AS r
    JOIN olist_customers_dataset AS c ON c.customer_id = r.customer_id
ORDER BY r.total_gasto DESC, r.total_pedidos DESC
LIMIT 10;

-- 6.3 Versão otimizada da query 5.3
SELECT
    s.seller_id,
    AVG(r.review_score) AS media_avaliacao,
    COUNT(DISTINCT r.review_id) AS quantidade_avaliacoes
FROM
    olist_order_reviews_dataset AS r
    JOIN olist_orders_dataset AS o ON o.order_id = r.order_id
    JOIN olist_order_items_dataset AS oi ON oi.order_id = o.order_id
    JOIN olist_sellers_dataset AS s ON s.seller_id = oi.seller_id
GROUP BY
    s.seller_id
ORDER BY media_avaliacao DESC;

-- 6.4 Versão otimizada ds query 5.4
SELECT o.order_id, o.order_status, o.order_purchase_timestamp, c.customer_unique_id, c.customer_city, c.customer_state, SUM(p.payment_value) AS total_pago
FROM
    olist_orders_dataset AS o
    JOIN olist_customers_dataset AS c ON c.customer_id = o.customer_id
    LEFT JOIN olist_order_payments_dataset AS p ON p.order_id = o.order_id
WHERE
    o.order_purchase_timestamp BETWEEN '2017-01-01' AND '2017-01-31'
GROUP BY
    o.order_id,
    o.order_status,
    o.order_purchase_timestamp,
    c.customer_unique_id,
    c.customer_city,
    c.customer_state
ORDER BY o.order_purchase_timestamp;

-- 6.5 Versão otimizada da query 5.5
SELECT p.product_id, p.product_category_name, COUNT(*) AS quantidade_vendida
FROM
    olist_orders_dataset AS o
    JOIN olist_order_items_dataset AS oi ON oi.order_id = o.order_id
    JOIN olist_products_dataset AS p ON p.product_id = oi.product_id
WHERE
    o.order_purchase_timestamp BETWEEN '2017-01-01' AND '2017-12-31'
GROUP BY
    p.product_id,
    p.product_category_name
ORDER BY quantidade_vendida DESC
LIMIT 5;

-- 6.6 Versão otimizada da queryy 5.6
SELECT o.order_id, c.customer_unique_id, c.customer_city, c.customer_state, o.order_estimated_delivery_date, o.order_delivered_customer_date, DATEDIFF(
        o.order_delivered_customer_date, o.order_estimated_delivery_date
    ) AS dias_atraso
FROM
    olist_orders_dataset AS o
    JOIN olist_customers_dataset AS c ON c.customer_id = o.customer_id
WHERE
    o.order_delivered_customer_date IS NOT NULL
    AND o.order_estimated_delivery_date IS NOT NULL
    AND o.order_delivered_customer_date BETWEEN '2017-01-01' AND '2017-12-31'
    AND DATEDIFF(
        o.order_delivered_customer_date,
        o.order_estimated_delivery_date
    ) > 0
ORDER BY dias_atraso DESC
LIMIT 10;

-- 6.7 Versão otimizada da query 5.7
SELECT c.customer_unique_id, SUM(p.payment_value) AS total_gasto
FROM
    olist_orders_dataset AS o
    JOIN olist_customers_dataset AS c ON c.customer_id = o.customer_id
    JOIN olist_order_payments_dataset AS p ON p.order_id = o.order_id
GROUP BY
    c.customer_unique_id
ORDER BY total_gasto DESC
LIMIT 10;

-- 6.8 Versão otimizada da query 5.8
SELECT
    c.customer_state,
    AVG(
        DATEDIFF(
            o.order_delivered_customer_date,
            o.order_delivered_carrier_date
        )
    ) AS tempo_medio_entrega_dias,
    COUNT(*) AS quantidade_pedidos
FROM
    olist_orders_dataset AS o
    JOIN olist_customers_dataset AS c ON c.customer_id = o.customer_id
WHERE
    o.order_delivered_customer_date IS NOT NULL
    AND o.order_delivered_carrier_date IS NOT NULL
GROUP BY
    c.customer_state
ORDER BY tempo_medio_entrega_dias DESC;