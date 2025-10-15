# Databricks notebook source
# MAGIC %md
# MAGIC # Simple Data Pipeline
# MAGIC This notebook demonstrates a basic ETL pipeline

# COMMAND ----------

# Get environment from job parameters (defaults to 'dev')
dbutils.widgets.text("bundle.target", "dev", "Environment")
env = dbutils.widgets.get("bundle.target")
print(f"Running in environment: {env}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Load Sample Data

# COMMAND ----------

from pyspark.sql import functions as F
from datetime import datetime

# Create sample data
data = [
    (1, "Product A", 100, "2024-01-01"),
    (2, "Product B", 200, "2024-01-01"),
    (3, "Product A", 150, "2024-01-02"),
    (4, "Product C", 300, "2024-01-02"),
]

df = spark.createDataFrame(data, ["id", "product", "amount", "date"])
print(f"Loaded {df.count()} rows")
display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Transform Data

# COMMAND ----------

# Calculate daily totals by product
daily_totals = (
    df.groupBy("product", "date")
    .agg(
        F.sum("amount").alias("total_amount"),
        F.count("id").alias("transaction_count")
    )
    .orderBy("date", "product")
)

print("Daily totals by product:")
display(daily_totals)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Save Results

# COMMAND ----------

# Save to environment-specific catalog
# Dev uses dev-catalog, Prod uses main  
catalog = "`dev-catalog`" if env == "dev" else "main"
output_table = f"{catalog}.default.daily_product_totals"

daily_totals.write.mode("overwrite").saveAsTable(output_table)
print(f"✅ Results saved to {output_table} (environment: {env})")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Pipeline Complete!

