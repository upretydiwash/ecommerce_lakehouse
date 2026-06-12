# Databricks notebook source
#get customers from table orders

orders = spark.sql("""
SELECT customer_id FROM dev.silver.silver_orders
""")

# COMMAND ----------

from pyspark.sql import functions as F

repeat_customers = orders.groupBy('customer_id')\
                         .agg(F.count('customer_id').alias('count'))\
					.filter(F.count('customer_id') > 1)

# COMMAND ----------

repeat_customers.write.mode('overwrite').saveAsTable('dev.gold.gold_repeat_customers')

# COMMAND ----------

