-- Databricks notebook source
-- MAGIC %run ../Includes/Classroom-Setup-05.1.3

-- COMMAND ----------

CREATE OR REFRESH STREAMING LIVE TABLE sales_orders_raw
COMMENT "The raw sales orders, ingested from retail-org/sales_orders."
AS
SELECT * FROM cloud_files("${source}/retail-org/sales_orders/", "json", map("cloudFiles.inferColumnTypes", "true"))

-- COMMAND ----------

CREATE OR REFRESH STREAMING LIVE TABLE customers
COMMENT "The customers buying finished products, ingested from retail-org/customers."
AS SELECT * FROM cloud_files("${source}/retail-org/customers/", "csv");

-- COMMAND ----------

CREATE OR REFRESH STREAMING LIVE TABLE sales_orders_cleaned(
  CONSTRAINT valid_order_number EXPECT (order_number IS NOT NULL) ON VIOLATION DROP ROW
)
COMMENT "The cleaned sales orders with valid order_number(s) and partitioned by order_datetime."
AS
SELECT f.customer_id, f.customer_name, f.number_of_line_items, 
  TIMESTAMP(from_unixtime((cast(f.order_datetime as long)))) as order_datetime, 
  DATE(from_unixtime((cast(f.order_datetime as long)))) as order_date, 
  f.order_number, f.ordered_products, c.state, c.city, c.lon, c.lat, c.units_purchased, c.loyalty_segment
  FROM STREAM(LIVE.sales_orders_raw) f
  LEFT JOIN LIVE.customers c
      ON c.customer_id = f.customer_id
     AND c.customer_name = f.customer_name

-- COMMAND ----------

CREATE OR REFRESH LIVE TABLE sales_order_in_la
COMMENT "Sales orders in LA."
AS
SELECT city, order_date, customer_id, customer_name, ordered_products_explode.curr, SUM(ordered_products_explode.price) as sales, SUM(ordered_products_explode.qty) as quantity, COUNT(ordered_products_explode.id) as product_count
FROM (
  SELECT city, order_date, customer_id, customer_name, EXPLODE(ordered_products) as ordered_products_explode
  FROM LIVE.sales_orders_cleaned 
  WHERE city = 'Los Angeles'
  )
GROUP BY order_date, city, customer_id, customer_name, ordered_products_explode.curr

-- COMMAND ----------

CREATE OR REFRESH LIVE TABLE sales_order_in_chicago
COMMENT "Sales orders in Chicago."
AS
SELECT city, order_date, customer_id, customer_name, ordered_products_explode.curr, SUM(ordered_products_explode.price) as sales, SUM(ordered_products_explode.qty) as quantity, COUNT(ordered_products_explode.id) as product_count
FROM (
  SELECT city, order_date, customer_id, customer_name, EXPLODE(ordered_products) as ordered_products_explode
  FROM LIVE.sales_orders_cleaned 
  WHERE city = 'Chicago'
  )
GROUP BY order_date, city, customer_id, customer_name, ordered_products_explode.curr
