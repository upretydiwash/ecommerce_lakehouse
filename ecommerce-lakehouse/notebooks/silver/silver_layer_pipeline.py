# Databricks notebook source
#remove duplicates
def drop_duplicates(df,primary_column):
    df = df.dropDuplicates([primary_column])
    return df

# COMMAND ----------

#remove null data
def drop_null(df):
    df = df.dropna()
    return df
    

# COMMAND ----------

