# Databricks notebook source
# MAGIC %md
# MAGIC ### DLT Notebook- GOLD LAYER
# MAGIC

# COMMAND ----------

import dlt

# COMMAND ----------

looktables_rules = {
    "price_nonzero": "Price > 0",
    "valid_bedrooms": "Bedrooms >= 0 AND Bedrooms <= 10",
    "valid_bathrooms": "Bathrooms >= 0 AND Bathrooms <= 10",
    "area_reasonable": "Area_Sqft > 100 AND Area_Sqft < 10000",
    "year_valid": "Year_Built >= 1900 AND Year_Built <= 2025",
    "lot_size_positive": "Lot_Size > 0",
    "days_on_market_valid": "Days_on_Market >= 0 AND Days_on_Market <= 1000",
    "status_known": "Status IN ('For Sale', 'Sold', 'Pending')"
}


# COMMAND ----------

@dlt.table(
  name="gold_ushouseprice"
) 

@dlt.expect_all_or_drop(looktables_rules)
def myfunc():
  df = spark.readStream.format("delta").load("abfss://silver@projectetlandml.dfs.core.windows.net/us_house_Sales_data")
  return df

# COMMAND ----------

@dlt.table

def gold_stg_ushouse():

    df = spark.readStream.format("delta").load("abfss://silver@projectetlandml.dfs.core.windows.net/us_house_price")
    return df

# COMMAND ----------

    from pyspark.sql.functions import *

# COMMAND ----------

@dlt.view
def gold_trns_ushouse():
    df = spark.readStream.table("LIVE.gold_stg_ushouse")
    df = df.withColumn("newflag", lit(1))
    return df

# COMMAND ----------

masterdata_rules={
    "rule1":"newflag is NOT NULL",
}

# COMMAND ----------

@dlt.table

@dlt.expect_all_or_drop(masterdata_rules)
def gold_ushouse():
    df = spark.readStream.table("LIVE.gold_trns_ushouse")
    return df
