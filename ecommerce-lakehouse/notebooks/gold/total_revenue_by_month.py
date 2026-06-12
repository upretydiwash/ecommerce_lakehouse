# Databricks notebook source
#importing bronze tables to calculate the revenues

order =spark.sql(
"""
SELECT 
    order_id,
    date_format(order_approved_at,'yyyy-MM') as order_approved_date
FROM 
    dev.silver.silver_orders """)
order_payments =spark.sql(
"""
SELECT 
    order_id,
    payment_value
FROM 
    dev.silver.silver_order_payments """)

# COMMAND ----------

# DBTITLE 1,Cell 2
from pyspark.sql import functions as F

# Calculate monthly revenue and write to gold table
monthly_revenue = (
    order.join(order_payments, on="order_id")
    .groupBy("order_approved_date")
    .agg(
        F.sum("payment_value").alias("revenue")
    )
)

# Write to gold table
monthly_revenue.write.mode("overwrite").saveAsTable("dev.gold.gold_monthly_revenue")



# COMMAND ----------

