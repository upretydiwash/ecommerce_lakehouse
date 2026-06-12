# Databricks notebook source
from pyspark.sql.window import Window
from pyspark.sql.functions import desc, dense_rank,count
import pyspark.sql.functions as F
#get order details

WindowSpec = Window.orderBy(F.col('order_count').desc())
orders = spark.read.table('dev.silver.silver_orders')\
			.groupBy('customer_id')\
			.agg(count('customer_id')\
			.alias('order_count'))\
			.withColumn('rnk', dense_rank().over(WindowSpec))


# COMMAND ----------

orders.display()

# COMMAND ----------

#get customer details
customers = spark.read.table('dev.silver.silver_customers')\
                .select('customer_id','customer_city')

            

# COMMAND ----------

#top cities

top_cities = orders\
				.join(customers, orders.customer_id == customers.customer_id, 'left')\
				.select(orders.customer_id, customers.customer_city,orders.order_count,orders.rnk)

# COMMAND ----------

top_cities.display()

# COMMAND ----------

