from pyspark.sql import SparkSession
from pyspark.sql.functions import regexp_replace

# Create a SparkSession
spark = SparkSession.builder \
    .appName("Orders-Customers-Data-Analysis") \
    .enableHiveSupport() \
    .getOrCreate()
    
# 1. Read both tables from Hive and store them in different dataframes
df_orders = spark.table("spark_db.orders")
df_customers = spark.table("spark_db.customers")
print("Read orders and customers data successfully")

# 2. Remove double quotes from order_id and customer_id in orders dataframe
df_orders = df_orders.withColumn("order_id", regexp_replace("order_id", "\"", "")) \
                    .withColumn("customer_id", regexp_replace("customre_id", "\"", ""))

# 3. Remove double quotes from customer_id and customer_unique_id in customers dataframe
df_customers = df_customers.withColumn("customer_id", regexp_replace("customer_id", "\"", "")) \
    .withColumn("customer_unique_id", regexp_replace("customer_unique_id", "\"", ""))
    
# Cache the dataframes to optimize reading operations
df_orders.cache()
df_customers.cache()

# 4. filter records where order_status='delivered' in orders dataframe
df_orders_delivered = df_orders.filter(df_orders.order_status == 'delivered')

# 5. Perform groupby operation on customer_id column to calculate number of orders
df_orders_grouped = df_orders_delivered.groupBy("customer_id").count()

# 6. Do a left join of customers dataframe with df_orders_grouped on customer_id column
df_joined = df_customers.join(df_orders_grouped, on="customer_id", how="left")

print("Join complete") 

# 7. show some records (First Action - triggers first job with multiple stages)
df_joined.show(10)

# Trigger an action to materialize the cached dataframes (Second action - triggers)
df_orders.count()
df_customers.count()

# Stop the SparkSession
spark.stop()                   