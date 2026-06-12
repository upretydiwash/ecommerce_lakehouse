# Databricks notebook source
#import table for total revenue

order_payments = spark.sql("""
                           SELECT sum(payment_value) as total_revenue from dev.silver.silver_order_payments
                           """)

# COMMAND ----------

#write to the gold total revenue table

order_payments.write.mode("overwrite").saveAsTable("dev.gold.gold_total_revenue")

# COMMAND ----------

