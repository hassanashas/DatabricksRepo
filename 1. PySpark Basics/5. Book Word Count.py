# Databricks notebook source
from pyspark.sql import SparkSession
import collections
import re 

# COMMAND ----------

spark = SparkSession.builder.appName('BookCountApp').getOrCreate()

# COMMAND ----------

lines = spark.sparkContext.textFile('dbfs:/FileStore/shared_uploads/hassan.ashas@systemsltd.com/5__Book-1')

# COMMAND ----------

def normalizeWords(text):
    return re.compile(r'\W+', re.UNICODE).split(text.lower())

# COMMAND ----------

words = lines.flatMap(normalizeWords)

# COMMAND ----------

wordsCount = words.map(lambda x: (x, 1)).reduceByKey(lambda x, y: x+y)

# COMMAND ----------

wordsCountSorted = wordsCount.map(lambda xy: (xy[1], xy[0])).sortByKey(ascending=False)
results = wordsCountSorted.collect()

# COMMAND ----------

    for result in results:
        print(result[1], "\t\t:", result[0])

# COMMAND ----------


