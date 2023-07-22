# Databricks notebook source
from pyspark.sql import SparkSession
import collections

# Create a Spark session
spark = SparkSession.builder \
    .appName("RatingsHistogram") \
    .getOrCreate()


# COMMAND ----------

lines = spark.sparkContext.textFile("dbfs:/FileStore/shared_uploads/hassan.ashas@systemsltd.com/6__customers_dataset.csv")

# COMMAND ----------

def parseLine(line):
    fields = line.split(",")
    customer_id = int(fields[0])
    amount_spent = float(fields[2])
    return (customer_id, amount_spent)

# COMMAND ----------

customersData = lines.map(parseLine)

# COMMAND ----------

totalAmounts = customersData.reduceByKey(lambda x, y: x + y)

# COMMAND ----------

totalAmountsSorted = totalAmounts.map(lambda xy: (xy[1], xy[0])).sortByKey(ascending=False)
results = totalAmountsSorted.collect()

# COMMAND ----------

for result in results: 
    print("Customer: ", result[1], "\tSales: ", result[0])

# COMMAND ----------


