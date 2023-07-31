# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql import functions as func
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType

# COMMAND ----------

spark = SparkSession.builder.appName("CustomerAmountSpent").getOrCreate()

# COMMAND ----------

schema = StructType(\
        [ \
            StructField("CustomerID", IntegerType(), True), \
            StructField("ProductID", IntegerType(), True), \
            StructField("Amount", FloatType(), True)
        ]
    )

# COMMAND ----------

df = spark.read.schema(schema).csv("dbfs:/FileStore/shared_uploads/hassan.ashas@systemsltd.com/6__customers_dataset.csv")

# COMMAND ----------

df.printSchema()

# COMMAND ----------

df.groupBy("CustomerID").agg(func.round(func.sum("Amount"), 2).alias("Total Amounnt Spent")).sort("CustomerID").show()

# COMMAND ----------


