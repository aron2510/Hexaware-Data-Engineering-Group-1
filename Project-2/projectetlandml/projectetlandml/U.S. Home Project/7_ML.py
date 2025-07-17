# Databricks notebook source
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import LinearRegression
from pyspark.ml.regression import RandomForestRegressor
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml import Pipeline
from pyspark.sql.functions import col,log1p,expm1

# COMMAND ----------

df = spark.read.format("delta").load("abfss://gold@projectetlandml.dfs.core.windows.net/gold_sales_ranked")

# COMMAND ----------

feature_cols = ["Bedrooms", "Bathrooms", "Area_Sqft", "Lot_Size", "Year_Built", "Days_on_Market"]
df = df.select(col("Price").alias("label"), *feature_cols)

# COMMAND ----------

df = df.na.drop()

# COMMAND ----------

train_df, test_df = df.randomSplit([0.8, 0.2], seed=42)

# COMMAND ----------

assembler = VectorAssembler(inputCols=feature_cols, outputCol="features")

# COMMAND ----------

lr = LinearRegression(featuresCol="features", labelCol="label")

# COMMAND ----------

pipeline = Pipeline(stages=[assembler, lr])

# COMMAND ----------

model = pipeline.fit(train_df)

# COMMAND ----------

predictions = model.transform(test_df)

# COMMAND ----------

evaluator = RegressionEvaluator(labelCol="label", predictionCol="prediction", metricName="rmse")
rmse = evaluator.evaluate(predictions)
print(f"Root Mean Squared Error (RMSE) on test data = {rmse}")

# COMMAND ----------

mean_price = test_df.selectExpr("avg(label)").first()[0]
rrmse = (rmse / mean_price) * 100
print(f"Relative RMSE = {rrmse:.2f}%")

# COMMAND ----------

model_path = "dbfs:/models/house_price_model"
model.write().overwrite().save(model_path)

# COMMAND ----------

predictions.write.format("delta").mode("overwrite").save("abfss://gold@projectetlandml.dfs.core.windows.net/predicted_sales")

# COMMAND ----------

feature_df = df.select(*feature_cols)

# COMMAND ----------

assembler_anom = VectorAssembler(inputCols=feature_cols, outputCol="features")

feature_df_vector = assembler_anom.transform(feature_df)

# COMMAND ----------

from sklearn.ensemble import IsolationForest
import pandas as pd

# COMMAND ----------

pandas_df = feature_df.toPandas()

# COMMAND ----------

clf = IsolationForest(contamination=0.01)
clf.fit(pandas_df)

# COMMAND ----------

pandas_df['anomaly'] = clf.predict(pandas_df)

# COMMAND ----------

anomalies_pd = pandas_df[pandas_df['anomaly'] == -1]

# COMMAND ----------

anom_spark_df = spark.createDataFrame(anomalies_pd)

# COMMAND ----------

display(anom_spark_df)

# COMMAND ----------



# COMMAND ----------

anom_only_df = anom_spark_df.filter(anom_spark_df.anomaly == -1)

# COMMAND ----------

anom_only_df.write.format("delta").mode("overwrite").save("abfss://gold@projectetlandml.dfs.core.windows.net/anomalies")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Random Forest
# MAGIC

# COMMAND ----------

df = df.withColumn("log_label", log1p("label"))

# COMMAND ----------

train_df, test_df = df.randomSplit([0.8, 0.2], seed=42)

# COMMAND ----------

assembler = VectorAssembler(inputCols=feature_cols, outputCol="features")

# COMMAND ----------

rf = RandomForestRegressor(featuresCol="features", labelCol="log_label", numTrees=100, maxDepth=10)

# COMMAND ----------

pipeline = Pipeline(stages=[assembler, rf])

# COMMAND ----------

model = pipeline.fit(train_df)

# COMMAND ----------

predictions = model.transform(test_df)

# COMMAND ----------

predictions = predictions.withColumn("prediction_original", expm1("prediction"))

# COMMAND ----------

evaluator = RegressionEvaluator(labelCol="label", predictionCol="prediction_original", metricName="rmse")
rmse = evaluator.evaluate(predictions)

# COMMAND ----------

mean_price = predictions.selectExpr("avg(label)").first()[0]
rel_rmse = (rmse / mean_price) * 100

print(f"RMSE: {rmse:.2f}")
print(f"Relative RMSE: {rel_rmse:.2f}%")

# COMMAND ----------

model_path = "dbfs:/models/house_price_rf"
model.write().overwrite().save(model_path)

# COMMAND ----------

predictions.write.format("delta").mode("overwrite").save("abfss://gold@projectetlandml.dfs.core.windows.net/predicted_sales_rf")