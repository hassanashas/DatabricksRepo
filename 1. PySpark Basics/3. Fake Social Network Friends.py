# Databricks notebook source
from pyspark.sql import SparkSession
import collections

# Create a Spark session
spark = SparkSession.builder \
    .appName("RatingsHistogram") \
    .getOrCreate()


# COMMAND ----------

lines = spark.sparkContext.textFile('dbfs:/FileStore/shared_uploads/hassan.ashas@systemsltd.com/fakefriends.csv')

# COMMAND ----------

def parseLine(line):
    fields = line.split(',')
    age = int(fields[2])
    numFriends = int(fields[3])
    return (age, numFriends)

# COMMAND ----------

rdd = lines.map(parseLine)
totalsByAge = rdd.mapValues(lambda x: (x, 1)).reduceByKey(lambda x, y: (x[0] + y[0], x[1] + y[1]))
averagesByAge = totalsByAge.mapValues(lambda x: x[0] / x[1])
results = averagesByAge.collect()

# COMMAND ----------

sorted_list = sorted(results, key=lambda x: x[1], reverse=True)

# COMMAND ----------

for result in sorted_list:
    print(result)

# COMMAND ----------


