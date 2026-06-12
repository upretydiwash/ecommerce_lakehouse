# Databricks notebook source
from pyspark.sql.window import Window 
from pyspark.sql.functions import dense_rank,col, count,desc
#select records from orders

order_items = spark.read.table('dev.silver.silver_order_items')\
                    .groupBy('product_id')\
                    .agg(count('product_id').alias('product_count'))\
                    .withColumn('Rank', dense_rank().over(Window.orderBy(col('product_count').desc())))\
                    .filter("Rank <= 50")

# COMMAND ----------

#write to table 

order_items.write.mode("overwrite").saveAsTable("dev.gold.gold_top_products")


# COMMAND ----------

