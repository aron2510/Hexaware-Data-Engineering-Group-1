# Databricks notebook source
# MAGIC %md
# MAGIC ### Silver Data Transformation

# COMMAND ----------

spark.conf.set("spark.databricks.monitoring.enabled", "true")

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import *

# COMMAND ----------

silver_df = spark.read.format("delta") \
    .load("abfss://bronze@projectetlandml.dfs.core.windows.net/ushome_sales")

# COMMAND ----------

silver_df = silver_df \
    .withColumn("Price", col("Price").cast("double")) \
    .withColumn("Bedrooms", col("Bedrooms").cast("int")) \
    .withColumn("Bathrooms", col("Bathrooms").cast("int")) \
    .withColumn("Area_Sqft", col("Area_Sqft").cast("int")) \
    .withColumn("Lot_Size", col("Lot_Size").cast("int")) \
    .withColumn("Year_Built", col("Year_Built").cast("int")) \
    .withColumn("Days_on_Market", col("Days_on_Market").cast("int"))

# COMMAND ----------

ranked_df = silver_df.withColumn("Price_ranking", dense_rank().over(Window.orderBy(col("Price").desc())))

# COMMAND ----------

ranked_df.write.format("delta") \
    .mode("overwrite") \
    .option("path", "abfss://gold@projectetlandml.dfs.core.windows.net/gold_sales_ranked") \
    .save()

# COMMAND ----------

ranked_df.display()