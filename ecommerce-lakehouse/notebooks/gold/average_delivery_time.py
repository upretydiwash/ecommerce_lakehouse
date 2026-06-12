# Databricks notebook source
#get data from oders table

orders = spark.sql(
    """
    SELECT 
        date_format(order_approved_at, 'yyyy-MM-dd') as order_approved_date,
        date_format(order_delivered_customer_date, 'yyyy-MM-dd') as order_delivered_date
    FROM 
        dev.silver.silver_orders

    WHERE 
        order_status = 'delivered'
    """
)

# COMMAND ----------

import pyspark.sql.functions as F
from pyspark.sql.functions import datediff as date_diff,cast

avg_delivery_time = orders.agg(F.avg(date_diff(F.col('order_delivered_date'),F.col('order_approved_date'))).cast('int').alias('avg_delivery_time'))

# COMMAND ----------

avg_delivery_time.write.mode('overwrite').saveAsTable('dev.gold.gold_avg_delivery_time')

# COMMAND ----------

