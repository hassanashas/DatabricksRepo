# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql import functions as func 
from pyspark.sql.types import StructType, StructField, IntegerType, FloatType, StringType, LongType

# COMMAND ----------

spark = SparkSession.builder.appName("PopularMovie").getOrCreate()

# COMMAND ----------

schema = StructType([ 
                    StructField("userID", IntegerType(), True), 
                    StructField("movieID", IntegerType(), True),  
                    StructField("rating", IntegerType(), True),  
                    StructField("timestamp", LongType(), True) 
                    ])

# COMMAND ----------

moviesDF = spark.read.option("sep", "\t").schema(schema).csv("dbfs:/FileStore/shared_uploads/hassan.ashas@systemsltd.com/u.data")

# COMMAND ----------

topMoviesDF = moviesDF.groupby("movieID").count().orderBy(func.desc("count"))
topMoviesDF.show()

# COMMAND ----------

lines = spark.sparkContext.textFile("dbfs:/FileStore/shared_uploads/hassan.ashas@systemsltd.com/u.data")

