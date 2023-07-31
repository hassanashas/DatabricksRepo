# Databricks notebook source
import pandas as pd
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number, col

# COMMAND ----------

file_path = "/dbfs/FileStore/practice/Billing_Sheet"
sheet_name = "Resource Allocation"

# COMMAND ----------

def num_records_partition(index, partition):
    count = 0
    for i in partition:
        count += 1 
    return index, count

# COMMAND ----------

pandas_df = pd.read_excel(file_path, sheet_name=sheet_name)
pandas_df = pandas_df.applymap(str)

df = spark.createDataFrame(pandas_df)

# COMMAND ----------

# r2.mapPartitionsWithIndex(num_records_partition).collect()
w = Window.partitionBy(df.columns).orderBy("Emp_ID")
df1 = df.select("Emp_ID", row_number().over(w).alias("rn"))
df_duplicates = df1.filter(col("rn") > 1).drop("rn")
df_unique = df1.filter(col("rn") == 1).drop("rn")


# COMMAND ----------

# df_unique.count()
df_duplicates.count()

# COMMAND ----------

df = spark.read.format("csv").option("header", True).option("inferSchema", True).load("dbfs:/FileStore/practice/Billing_Sheet")

# COMMAND ----------

df.show()

# COMMAND ----------


