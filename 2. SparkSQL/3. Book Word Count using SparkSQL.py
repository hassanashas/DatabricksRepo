# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql import functions as func

spark = SparkSession.builder.appName("BookWordCount").getOrCreate()

# COMMAND ----------

inputDF = spark.read.text('dbfs:/FileStore/shared_uploads/hassan.ashas@systemsltd.com/5__Book-1')

# COMMAND ----------

inputDF.show()

# COMMAND ----------

# Split using a regular expression that extracts words
words = inputDF.select(func.explode(func.split(inputDF.value, "\\W+")).alias("word"))
wordsWithoutEmptyString = words.filter(words.word != "")

# Normalize everything to lowercase
lowercaseWords = wordsWithoutEmptyString.select(func.lower(wordsWithoutEmptyString.word).alias("word"))

# Count up the occurrences of each word
wordCounts = lowercaseWords.groupBy("word").count()

# Sort by counts
wordCountsSorted = wordCounts.sort(func.desc("count"))

# Show the results.
wordCountsSorted.show(wordCountsSorted.count())

# COMMAND ----------


