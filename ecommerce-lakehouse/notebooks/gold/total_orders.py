# Databricks notebook source
total_orders = spark.sql(
"""
SELECT 
	count(distinct order_id) as total_orders
FROM dev.silver.silver_orders
WHERE order_approved_at is not null
"""
)

# COMMAND ----------

total_orders.write.mode('overwrite').saveAsTable('dev.gold.gold_total_orders')

# COMMAND ----------

