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
CREATE TABLE IF NOT EXISTS silver_products (
    product_id STRING,
    name STRING,
    category STRING,
    brand STRING,
    price DOUBLE,
    stock_quantity INT,
    rating DOUBLE,
    is_active BOOLEAN,
    price_category STRING,
    stock_status STRING,
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
last_processed_df = spark.sql("SELECT MAX(last_updated) as last_processed FROM silver_products")
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
bronze_df = spark.table("dbo_1.Product") \
    .where(f"ingestion_timestamp > '{last_processed_timestamp}'")

bronze_df.createOrReplaceTempView("bronze_incremental_products")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Transform the product data.
spark.sql("""
CREATE OR REPLACE TEMPORARY VIEW silver_incremental_products AS
SELECT
    product_id,
    name,
    category,
    brand,
    CASE
        WHEN price < 0 THEN 0
        ELSE price
    END AS price,
    CASE
        WHEN stock_quantity < 0 THEN 0
        ELSE stock_quantity
    END AS stock_quantity,
    CASE
        WHEN rating < 0 THEN 0
        WHEN rating > 5 THEN 5
        ELSE rating
    END AS rating,
    is_active,
    CASE
        WHEN price > 1000 THEN 'Premium'
        WHEN price > 100 THEN 'Standard'
        ELSE 'Budget'
    END AS price_category,
    CASE
        WHEN stock_quantity = 0 THEN 'Out of Stock'
        WHEN stock_quantity < 10 THEN 'Low Stock'
        WHEN stock_quantity < 50 THEN 'Moderate Stock'
        ELSE 'Sufficient Stock'
    END AS stock_status,
    CURRENT_TIMESTAMP() AS last_updated
FROM bronze_incremental_products
WHERE name IS NOT NULL AND category IS NOT NULL
""")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Merge data into the Silver layer.
spark.sql("""
MERGE INTO silver_products target
USING silver_incremental_products source
ON target.product_id = source.product_id
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
spark.sql("SELECT * FROM silver_products LIMIT 10").show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
