# Databricks notebook source
from pyspark.sql.window import Window
from pyspark.sql.functions import dense_rank, count, desc
from pyspark.sql import functions as F
#top sellers

WindowSpec = Window.orderBy(F.col('products_sold').desc())
order_sellers = spark.read.table('dev.silver.silver_order_items')\
					.groupBy('seller_id')\
					.agg(count('seller_id')\
					.alias('products_sold'))\
					.withColumn('Top_sellers_rank', dense_rank().over(WindowSpec))

# COMMAND ----------

#write to a table
order_sellers.write.mode('overwrite').saveAsTable('dev.gold.gold_top_sellers')

# COMMAND ----------

