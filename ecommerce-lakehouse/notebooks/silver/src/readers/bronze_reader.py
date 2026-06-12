from pyspark.sql import SparkSession
class BronzeReader:

  @staticmethod
  def read_bronze(table_name):
    spark = SparkSession.builder.getOrCreate()
    return spark.read.table(table_name)
