# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "0804c6dd-f0e2-4829-abd1-adab62581e0c",
# META       "default_lakehouse_name": "GoldLayer",
# META       "default_lakehouse_workspace_id": "809a8f19-69af-4189-81cc-f336c733f267",
# META       "known_lakehouses": [
# META         {
# META           "id": "0804c6dd-f0e2-4829-abd1-adab62581e0c"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

# Welcome to your new notebook
# Type here in the cell editor to add code!
spark.sql("""
CREATE OR REPLACE TABLE gold_daily_sales AS
SELECT 
    transaction_date,
    SUM(total_amount) AS daily_total_sales
FROM 
    dbo_1.silver_orders
GROUP BY 
    transaction_date
""")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Verify the Gold table
spark.sql("SELECT * FROM gold_daily_sales LIMIT 10").show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
