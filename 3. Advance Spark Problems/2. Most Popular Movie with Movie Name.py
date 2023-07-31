# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql import functions as func 
from pyspark.sql.types import StructType, StructField, IntegerType, FloatType, StringType, LongType
import codecs
from pyspark.dbutils import DBUtils

# COMMAND ----------

dbutils = DBUtils(spark)

# COMMAND ----------

def loadMovieNames():
    movieNames = {} 
    with codecs.open("/dbfs/FileStore/shared_uploads/hassan.ashas@systemsltd.com/u.item", "r", 
                     encoding = "ISO-8859-1", errors = "ignore") as f:
        for line in f:
            fields = line.split("|")
            movieNames[int(fields[0])] = fields[1]
    return movieNames

# COMMAND ----------

nameDict = spark.sparkContext.broadcast(loadMovieNames())

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

def lookupName(movieID):
    return nameDict.value[movieID]

# COMMAND ----------

lookupNameUDF = func.udf(lookupName)

# COMMAND ----------

moviesWithNames = topMoviesDF.withColumn("movieTitle", lookupNameUDF(func.col("movieID")))
moviesWithNames.show(10, False)

# COMMAND ----------


