# Databricks notebook source
# MAGIC %md
# MAGIC ### Array Parameter

# COMMAND ----------

files=[
  {
    "sourcefolder":"us_house_Sales_data",
    "targetfolder":"us_house_Sales_data"
  }
]

# COMMAND ----------

# MAGIC %md
# MAGIC ### Job Utility to return the Array
# MAGIC

# COMMAND ----------

dbutils.jobs.taskValues.set(key="my_array" , value=files)