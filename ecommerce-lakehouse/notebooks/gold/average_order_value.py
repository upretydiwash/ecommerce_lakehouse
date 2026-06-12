# Databricks notebook source
#import table order payments

order_payments = spark.sql("""
SELECT order_id, payment_value 
FROM dev.silver.silver_order_payments""")

# COMMAND ----------

# DBTITLE 1,Cell 2
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# Calculate total payment per order, then get the average across all orders
average_order_value = order_payments.groupBy('order_id')\
    .agg(F.sum('payment_value').alias('total_payment_value'))\
    .agg(F.avg('total_payment_value').alias('avg_order_value'))

# COMMAND ----------

#write to gold table 

average_order_value.write.mode('overwrite').saveAsTable('dev.gold.gold_average_order_value')

# COMMAND ----------

