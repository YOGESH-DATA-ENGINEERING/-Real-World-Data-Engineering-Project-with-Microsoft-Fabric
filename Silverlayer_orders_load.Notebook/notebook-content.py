# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "bdca05df-3e55-4c4a-9bf6-3978822f1ac4",
# META       "default_lakehouse_name": "SilverLayer",
# META       "default_lakehouse_workspace_id": "809a8f19-69af-4189-81cc-f336c733f267",
# META       "known_lakehouses": [
# META         {
# META           "id": "bdca05df-3e55-4c4a-9bf6-3978822f1ac4"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

# Welcome to your new notebook
# Type here in the cell editor to add code!
spark.sql("""
CREATE TABLE IF NOT EXISTS silver_orders (
    order_id STRING,
    customer_id STRING,
    product_id STRING,
    quantity INT,
    total_amount DOUBLE,
    transaction_date DATE,
    order_status STRING,
    last_updated TIMESTAMP
)
USING DELTA
""")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Identify last processed timestamp.
last_processed_df = spark.sql("SELECT MAX(last_updated) as last_processed FROM silver_orders")
last_processed_timestamp = last_processed_df.collect()[0]['last_processed']

if last_processed_timestamp is None:
    last_processed_timestamp = "1900-01-01T00:00:00.000+00:00"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Create a temporary view of incremental bronze data
bronze_df = spark.table("dbo_1.Orders") \
    .where(f"ingestion_timestamp > '{last_processed_timestamp}'")

bronze_df.createOrReplaceTempView("bronze_incremental_orders")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Transform the order data.
spark.sql("""
CREATE OR REPLACE TEMPORARY VIEW silver_incremental_orders AS
SELECT
    transaction_id as order_id,
    customer_id,
    product_id,
    CASE
        WHEN quantity < 0 THEN 0
        ELSE quantity
    END AS quantity,
    CASE
        WHEN total_amount < 0 THEN 0
        ELSE total_amount
    END AS total_amount,
    CAST(transaction_date AS DATE) AS transaction_date,
    CASE
        WHEN quantity = 0 AND total_amount = 0 THEN 'Cancelled'
        WHEN quantity > 0 AND total_amount > 0 THEN 'Completed'
        ELSE 'In Progress'
    END AS order_status,
    CURRENT_TIMESTAMP() AS last_updated
FROM bronze_incremental_orders
WHERE transaction_date IS NOT NULL 
  AND customer_id IS NOT NULL 
  AND product_id IS NOT NULL
""")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Merge data into the Silver layer
spark.sql("""
MERGE INTO silver_orders target
USING silver_incremental_orders source
ON target.order_id = source.order_id
WHEN MATCHED THEN
    UPDATE SET *
WHEN NOT MATCHED THEN
    INSERT *
""")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Verify the data in the Silver layer.
spark.sql("SELECT * FROM silver_orders LIMIT 10").show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
