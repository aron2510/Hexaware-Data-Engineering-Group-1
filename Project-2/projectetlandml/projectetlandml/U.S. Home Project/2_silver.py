# Databricks notebook source
# MAGIC %md
# MAGIC # Silver Notebook Lookup Tables

# COMMAND ----------

# MAGIC %md
# MAGIC ### Parameters
# MAGIC

# COMMAND ----------

spark.conf.set("spark.databricks.monitoring.enabled", "true")

# COMMAND ----------

dbutils.widgets.text("sourcefolder","us_house_Sales_data")
dbutils.widgets.text("targetfolder","us_house_Sales_data")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Variables
# MAGIC

# COMMAND ----------

var_src_folder=dbutils.widgets.get("sourcefolder")
var_tgt_folder=dbutils.widgets.get("targetfolder")

# COMMAND ----------

print(var_src_folder)

# COMMAND ----------

bronze_df = spark.read.format("csv")\
    .option("header", "true")\
    .option("inferSchema", "true")\
    .load(f"abfss://bronze@projectetlandml.dfs.core.windows.net/{var_src_folder}")

# COMMAND ----------

bronze_df.display()

# COMMAND ----------

silver_df = bronze_df \
    .withColumnRenamed("Lot Size", "Lot_Size") \
    .withColumnRenamed("Area (Sqft)", "Area_Sqft") \
    .withColumnRenamed("Year Built", "Year_Built") \
    .withColumnRenamed("Days on Market", "Days_on_Market") \
    .withColumnRenamed("Listing Agent", "Listing_Agent") \
    .withColumnRenamed("Property Type", "Property_Type") \
    .withColumnRenamed("Listing URL", "Listing_URL") \
    .withColumnRenamed("MLS ID", "MLS_ID")


# COMMAND ----------

silver_df.write.format("delta") \
    .mode("append") \
    .option("path", f"abfss://silver@projectetlandml.dfs.core.windows.net/{var_tgt_folder}") \
    .save()