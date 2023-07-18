# Databricks notebook source
print("Damn man, already starting this!")

# COMMAND ----------

# MAGIC %run ./SecondFunctionNotebook

# COMMAND ----------

print(full_name)

# COMMAND ----------

# MAGIC %md # This is my First Header
# MAGIC
# MAGIC - First bullet
# MAGIC - You see how created a damn function up there??

# COMMAND ----------

from pyspark import SparkConf, SparkContext

# COMMAND ----------

from pyspark.sql import SparkSession
import collections

# Create a Spark session
spark = SparkSession.builder \
    .appName("RatingsHistogram") \
    .getOrCreate()

# Read the input file
lines = spark.sparkContext.textFile("dbfs:/FileStore/shared_uploads/hassan.ashas@systemsltd.com/u.data")

# Extract the ratings
ratings = lines.map(lambda x: x.split()[2])

# Count the occurrences of each rating
result = ratings.countByValue()

# Sort the results
sortedResults = collections.OrderedDict(sorted(result.items()))

# Print the sorted results
for key, value in sortedResults.items():
    print("%s %i" % (key, value))

# Stop the Spark session
# spark.stop()



# COMMAND ----------


