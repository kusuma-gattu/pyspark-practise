CREATE TABLE orders (
     order_id  STRING,
     customer_id STRING,
     order_purchase_timestamp TIMESTAMP,
     order_approved_at TIMESTAMP,
     order_delivered_carrier_date TIMESTAMP,
     order_delivered_customer_date TIMESTAMP,
     order_estimated_delivery_date TIMESTAMP
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
TBLPROPERTIES ("skip.header.line.count"="1");

load data inpath '/spark_input_data/orders_dataset.csv' into table orders;

CREATE TABLE customers (
     customer_id STRING,
     customer_unique_id STRING,
     customer_zip_code_prefix STRING,
     customer_city STRING,
     customer_state STRING
  )
  ROW FORMAT DELIMITED
  FIELDS TERMINATED BY ','
  TBLPROPERTIES ("skip.header.line.count"="1");

LOAD DATA INPATH '/spark_input_data/customers_dataset.csv' INTO TABLE customers;
