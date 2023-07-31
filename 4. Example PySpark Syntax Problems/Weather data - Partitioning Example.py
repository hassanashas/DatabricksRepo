# Databricks notebook source
dbutils.fs.ls('/FileStore/practice')

# COMMAND ----------

rd = sc.textFile('/FileStore/practice/weather.csv')
# rd.getNumPartitions()
# rd.glom().collect()
rd.count()

# COMMAND ----------

r2 = sc.parallelize(spark.range(9000000).collect())
r2.getNumPartitions()

# COMMAND ----------

def num_records_partition(index, partition):
    count = 0
    for i in partition:
        count += 1 
    return index, count

r2.mapPartitionsWithIndex(num_records_partition).collect()

# COMMAND ----------

r3 = r2.repartition(16)
r3.getNumPartitions()

# COMMAND ----------

r3.mapPartitionsWithIndex(num_records_partition).collect()

# COMMAND ----------

r4 = sc.parallelize(spark.range(1000000).collect())
r5 = r4.coalesce(3)
r5.getNumPartitions()

# COMMAND ----------

r5.mapPartitionsWithIndex(num_records_partition).collect()

# COMMAND ----------

df = spark.read.text('dbfs:/FileStore/practice/weather.csv' )

# COMMAND ----------

def input_data(line):
    fields = line.split(",")
    date = fields

# COMMAND ----------

rddd_data = df.rdd.map(lambda line: (int(line.split(",")[0].split("-")[0]), int(line.split(",")[2])))

# COMMAND ----------

df = spark.read.format("csv").option("header", True).option("inferSchema", True).load('dbfs:/FileStore/practice/weather.csv')
df.rdd.getNumPartitions()

# COMMAND ----------


# Read the CSV file into a DataFrame
df = spark.read.text('dbfs:/FileStore/practice/weather.csv')

df = df.withColumn("date", df["value"].substr(0, 10))
df = df.withColumn("numeric_value", df["value"].substr(12, 6).cast("int"))
df = df.withColumn("temperature", df["value"].substr(19, 3).cast("int"))

df = df.withColumn("year", df["date"].substr(0, 4).cast("int"))

df = df.select("year", "temperature")

df.show()


# COMMAND ----------

rdd_data = df.rdd

# COMMAND ----------

max_temperature_rdd = rdd_data.reduceByKey(lambda x, y: x if x > y else y)

# COMMAND ----------

print("Max Temperature: ", max_temperature_rdd.collect())

# COMMAND ----------

print("Partitioners for max_temperature_rdd is {}".format(max_temperature_rdd.partitioner.partitionFunc))

# COMMAND ----------

sorted_rdd = max_temperature_rdd.sortByKey()

# COMMAND ----------

print("Sorted RDD: {}".format(sorted_rdd.collect()))

# COMMAND ----------

print("Partitioner for second sorted RDD: {}".format(sorted_rdd.partitioner.partitionFunc))

# COMMAND ----------

# MAGIC %fs 
# MAGIC rm -r /FileStore/practice/output/max_temperature

# COMMAND ----------

# MAGIC %fs 
# MAGIC rm -r /FileStore/practice/output/sorted_max_temperature

# COMMAND ----------

max_temperature_rdd.saveAsTextFile("/FileStore/practice/output/max_temperature")

# COMMAND ----------

sorted_rdd.saveAsTextFile("/FileStore/practice/output/sorted_max_temperature")

# COMMAND ----------

# MAGIC %fs 
# MAGIC ls /FileStore/practice/output/max_temperature

# COMMAND ----------

# MAGIC %fs 
# MAGIC ls /FileStore/practice/output/sorted_max_temperature

# COMMAND ----------


