# Databricks notebook source
from pyspark.sql.functions import current_timestamp, lit


# COMMAND ----------

def bronze_ingetion(input_file_path, output_file_path):
  df = spark.read.format("csv").option("header", "true").option("quote",'"').option("escape", '"').option("ignoreLeadingWhiteSpace", "true").option("ignoreTrailingWhiteSpace", "true").option("multiLine", "true").load(input_file_path)
  df = df.withColumn("ingestion_time", current_timestamp()).withColumn("source", lit(input_file_path))
  df.write.mode("overwrite").format("delta").saveAsTable(output_file_path)  
  return

# COMMAND ----------


input_file_path = dbutils.widgets.get("input_file_path")
target_catalog= dbutils.widgets.get("target_catalog")
target_schema= dbutils.widgets.get("target_schema")
target_table= dbutils.widgets.get("target_table")
output_table_path = f"{target_catalog}.{target_schema}.{target_table}"
bronze_ingetion(input_file_path, output_table_path)

# COMMAND ----------

