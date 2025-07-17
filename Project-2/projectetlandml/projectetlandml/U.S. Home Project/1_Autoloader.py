# Databricks notebook source
# MAGIC %md
# MAGIC # Incremental Data Loading Using AutoLoader

# COMMAND ----------

spark.conf.set("spark.databricks.monitoring.enabled", "true")

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE SCHEMA IF NOT EXISTS ushome_catalog.net_schema;

# COMMAND ----------

from pyspark.sql.functions import regexp_replace, regexp_extract, col

# COMMAND ----------

checkpoint_location="abfss://bronze@projectetlandml.dfs.core.windows.net/checkpoint"

# COMMAND ----------

raw_df = spark.readStream \
  .format("cloudFiles") \
  .option("cloudFiles.format", "csv") \
  .option("cloudFiles.schemaLocation", checkpoint_location) \
  .load("abfss://raw@projectetlandml.dfs.core.windows.net")


# COMMAND ----------

display(raw_df)

# COMMAND ----------

clean_df = raw_df \
    .withColumn("Price", regexp_replace("Price", "[$,]", "").cast("double")) \
    .withColumn("Bedrooms", regexp_extract("Bedrooms", "(\\d+)", 1).cast("int")) \
    .withColumn("Bathrooms", regexp_extract("Bathrooms", "(\\d+)", 1).cast("int")) \
    .withColumn("Area_Sqft", regexp_extract("`Area (Sqft)`", "(\\d+)", 1).cast("int")) \
    .withColumn("Lot_Size", regexp_extract("`Lot Size`", "(\\d+)", 1).cast("int")) \
    .withColumn("Year_Built", col("Year Built").cast("int")) \
    .withColumn("Days_on_Market", col("Days on Market").cast("int")) \
    .withColumnRenamed("MLS ID", "MLS_ID") \
    .withColumnRenamed("Listing Agent", "Listing_Agent") \
    .withColumnRenamed("Property Type", "Property_Type") \
    .withColumnRenamed("Listing URL", "Listing_URL") \
    .withColumnRenamed("Area (Sqft)", "drop_1") \
    .withColumnRenamed("Lot Size", "drop_2") \
    .withColumnRenamed("Year Built", "drop_3") \
    .withColumnRenamed("Days on Market", "drop_4") \
    .drop("drop_1", "drop_2", "drop_3", "drop_4")


# COMMAND ----------

clean_df = clean_df.drop("\x00 validating storage credential...")


# COMMAND ----------

clean_df.writeStream \
  .format("delta") \
  .option("checkpointLocation", checkpoint_location) \
  .trigger(processingTime='10 seconds') \
  .start("abfss://bronze@projectetlandml.dfs.core.windows.net/ushome_sales")