# Databricks notebook source
from pyspark.sql.functions import date_format , cast
orders = spark.read.table('dev.silver.silver_orders')\
			.select('order_id','customer_id','order_delivered_carrier_date','order_delivered_customer_date','order_estimated_delivery_date')\
                .filter('order_status = "delivered"')\
				    .withColumn('order_delivered_customer_date', date_format('order_delivered_customer_date','yyyy-MM-dd').cast('date'))\
					    .withColumn('order_estimated_delivery_date', date_format('order_estimated_delivery_date','yyyy-MM-dd').cast('date'))

# COMMAND ----------

from pyspark.sql.functions import  date_diff

delayed_orders = orders.filter(date_diff('order_delivered_customer_date','order_estimated_delivery_date') > 0)\
					.withColumn('delayed_days', date_diff('order_delivered_customer_date','order_estimated_delivery_date'))

# COMMAND ----------

#write to gold table

delayed_orders.write.mode('overwrite').saveAsTable('dev.gold.gold_delayed_orders')

# COMMAND ----------

