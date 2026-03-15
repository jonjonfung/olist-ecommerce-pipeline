-- ============================================
-- OLIST E-COMMERCE PIPELINE - ATHENA QUERIES
-- ============================================

-- CREATE SILVER DATABASE
CREATE DATABASE IF NOT EXISTS olist_silver_db;

-- ============================================
-- SILVER LAYER TABLES
-- ============================================

-- Orders
CREATE EXTERNAL TABLE IF NOT EXISTS olist_silver_db.orders (
  order_id string,
  customer_id string,
  order_status string,
  order_purchase_timestamp string,
  order_approved_at string,
  order_delivered_carrier_date string,
  order_delivered_customer_date string,
  order_estimated_delivery_date string
)
STORED AS PARQUET
LOCATION 's3://olist-ecommerce-pipeline-john/silver/orders/';

-- Customers
CREATE EXTERNAL TABLE IF NOT EXISTS olist_silver_db.customers (
  customer_id string,
  customer_unique_id string,
  customer_zip_code_prefix string,
  customer_city string,
  customer_state string
)
STORED AS PARQUET
LOCATION 's3://olist-ecommerce-pipeline-john/silver/customers/';

-- Order Items
CREATE EXTERNAL TABLE IF NOT EXISTS olist_silver_db.order_items (
  order_id string,
  order_item_id string,
  product_id string,
  seller_id string,
  shipping_limit_date string,
  price double,
  freight_value double
)
STORED AS PARQUET
LOCATION 's3://olist-ecommerce-pipeline-john/silver/order_items/';

-- Order Payments
CREATE EXTERNAL TABLE IF NOT EXISTS olist_silver_db.order_payments (
  order_id string,
  payment_sequential string,
  payment_type string,
  payment_installments int,
  payment_value double
)
STORED AS PARQUET
LOCATION 's3://olist-ecommerce-pipeline-john/silver/order_payments/';

-- Order Reviews
CREATE EXTERNAL TABLE IF NOT EXISTS olist_silver_db.order_reviews (
  review_id string,
  order_id string,
  review_score int,
  review_comment_title string,
  review_comment_message string,
  review_creation_date string,
  review_answer_timestamp string
)
STORED AS PARQUET
LOCATION 's3://olist-ecommerce-pipeline-john/silver/order_reviews/';

-- Products
CREATE EXTERNAL TABLE IF NOT EXISTS olist_silver_db.products (
  product_id string,
  product_category_name string,
  product_name_lenght string,
  product_description_lenght string,
  product_photos_qty string,
  product_weight_g double,
  product_length_cm double,
  product_height_cm double,
  product_width_cm double
)
STORED AS PARQUET
LOCATION 's3://olist-ecommerce-pipeline-john/silver/products/';

-- Sellers
CREATE EXTERNAL TABLE IF NOT EXISTS olist_silver_db.sellers (
  seller_id string,
  seller_zip_code_prefix string,
  seller_city string,
  seller_state string
)
STORED AS PARQUET
LOCATION 's3://olist-ecommerce-pipeline-john/silver/sellers/';

-- Geolocation
CREATE EXTERNAL TABLE IF NOT EXISTS olist_silver_db.geolocation (
  geolocation_zip_code_prefix string,
  geolocation_lat double,
  geolocation_lng double,
  geolocation_city string,
  geolocation_state string
)
STORED AS PARQUET
LOCATION 's3://olist-ecommerce-pipeline-john/silver/geolocation/';

-- Product Category Translation
CREATE EXTERNAL TABLE IF NOT EXISTS olist_silver_db.product_category_translation (
  product_category_name string,
  product_category_name_english string
)
STORED AS PARQUET
LOCATION 's3://olist-ecommerce-pipeline-john/silver/product_category_translation/';

-- ============================================
-- GOLD LAYER TABLES
-- ============================================

-- Revenue by Category
CREATE TABLE olist_silver_db.revenue_by_category
WITH (
    format = 'PARQUET',
    external_location = 's3://olist-ecommerce-pipeline-john/gold/revenue_by_category/'
)
AS
SELECT 
    p.product_category_name,
    COUNT(o.order_id) as total_orders,
    ROUND(SUM(oi.price), 2) as total_revenue
FROM olist_silver_db.orders o
JOIN olist_silver_db.order_items oi ON o.order_id = oi.order_id
JOIN olist_silver_db.products p ON oi.product_id = p.product_id
GROUP BY p.product_category_name
ORDER BY total_revenue DESC;

-- Monthly Revenue
CREATE TABLE olist_silver_db.monthly_revenue
WITH (
    format = 'PARQUET',
    external_location = 's3://olist-ecommerce-pipeline-john/gold/monthly_revenue/'
)
AS
SELECT 
    SUBSTR(o.order_purchase_timestamp, 1, 7) as year_month,
    COUNT(o.order_id) as total_orders,
    ROUND(SUM(oi.price), 2) as total_revenue
FROM olist_silver_db.orders o
JOIN olist_silver_db.order_items oi ON o.order_id = oi.order_id
WHERE o.order_status = 'delivered'
GROUP BY SUBSTR(o.order_purchase_timestamp, 1, 7)
ORDER BY year_month ASC;

-- Average Delivery Time by State
CREATE TABLE olist_silver_db.avg_delivery_time_by_state
WITH (
    format = 'PARQUET',
    external_location = 's3://olist-ecommerce-pipeline-john/gold/avg_delivery_time_by_state/'
)
AS
SELECT 
    c.customer_state,
    COUNT(o.order_id) as total_orders,
    ROUND(AVG(
        DATE_DIFF('day', 
            DATE_PARSE(o.order_purchase_timestamp, '%Y-%m-%d %H:%i:%s'),
            DATE_PARSE(o.order_delivered_customer_date, '%Y-%m-%d %H:%i:%s')
        )
    ), 1) as avg_delivery_days
FROM olist_silver_db.orders o
JOIN olist_silver_db.customers c ON o.customer_id = c.customer_id
WHERE o.order_status = 'delivered'
AND o.order_delivered_customer_date != ''
AND o.order_purchase_timestamp != ''
GROUP BY c.customer_state
ORDER BY avg_delivery_days ASC;

-- Payment Method Breakdown
CREATE TABLE olist_silver_db.payment_method_breakdown
WITH (
    format = 'PARQUET',
    external_location = 's3://olist-ecommerce-pipeline-john/gold/payment_method_breakdown/'
)
AS
SELECT 
    payment_type,
    COUNT(order_id) as total_orders,
    ROUND(SUM(payment_value), 2) as total_value,
    ROUND(AVG(CAST(payment_installments AS double)), 1) as avg_installments
FROM olist_silver_db.order_payments
GROUP BY payment_type
ORDER BY total_orders DESC;

-- Top Sellers by Revenue
CREATE TABLE olist_silver_db.top_sellers_by_revenue
WITH (
    format = 'PARQUET',
    external_location = 's3://olist-ecommerce-pipeline-john/gold/top_sellers_by_revenue/'
)
AS
SELECT 
    s.seller_id,
    s.seller_city,
    s.seller_state,
    COUNT(oi.order_id) as total_orders,
    ROUND(SUM(oi.price), 2) as total_revenue
FROM olist_silver_db.order_items oi
JOIN olist_silver_db.sellers s ON oi.seller_id = s.seller_id
GROUP BY s.seller_id, s.seller_city, s.seller_state
ORDER BY total_revenue DESC;
