-- Databricks notebook source
-- MAGIC %python
-- MAGIC display(dbutils.fs.mounts())

-- COMMAND ----------

-- MAGIC %python
-- MAGIC df = spark.read.format('csv').load('dbfs:/formula1pylearn/raw/harvard_reviews.csv')

-- COMMAND ----------


