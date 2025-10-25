# Databricks notebook source
# MAGIC %md
# MAGIC # Generate Bad Healthcare Data for Quality Testing
# MAGIC 
# MAGIC Creates files with intentional quality issues (NULLs, invalid formats) to test data quality monitoring

# COMMAND ----------

import pandas as pd
from datetime import datetime, timedelta
import random
import time

# Use timestamp as seed for unique records each run
random.seed(int(time.time()))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Helper Functions

# COMMAND ----------

def _single_file_write(spark_df, target_file_path: str, fmt: str, options: dict = None):
    """Write a Spark DataFrame as a single file"""
    tmp_dir = target_file_path + "_tmp_write"
    writer = spark_df.coalesce(1).write.mode("overwrite")
    if options:
        for k, v in options.items():
            writer = writer.option(k, v)
    if fmt == "csv":
        writer.csv(tmp_dir)
    elif fmt == "json":
        writer.json(tmp_dir)
    else:
        raise ValueError(f"Unsupported format: {fmt}")
    
    part_files = [f.path for f in dbutils.fs.ls(tmp_dir) if f.name.startswith("part-")]
    if not part_files:
        raise RuntimeError(f"No part files written in {tmp_dir}")
    dbutils.fs.cp(part_files[0], target_file_path)
    dbutils.fs.rm(tmp_dir, True)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Bad Data Generator

# COMMAND ----------

def create_bad_claims_837_data(n_records: int = 100):
    """
    Create claims with intentional quality issues:
    - 20% have NULL claim_id
    - 30% have NULL member_id
    - 10% have NULL service_date
    - 10% have negative billed_amount
    """
    # Use timestamp for unique claim IDs each run
    ts_suffix = int(time.time())
    data = []
    for i in range(n_records):
        data.append({
            'claim_id': None if i < n_records * 0.2 else f'CLM_{ts_suffix}_{i+1:06d}',
            'member_id': None if 0.2 <= i/n_records < 0.5 else f'MEM{random.randint(100000, 999999)}',
            'provider_id': 'provider_123',
            'service_date': None if i >= n_records * 0.9 else (datetime.now() - timedelta(days=random.randint(1, 30))).strftime('%Y-%m-%d'),
            'billed_amount': -100.0 if 0.5 <= i/n_records < 0.6 else round(random.uniform(50.0, 2000.0), 2),
            'paid_amount': round(random.uniform(40.0, 1800.0), 2),
            'subscriber_name': f'Bad_Patient_{i+1}',
            'date_of_birth': (datetime.now() - timedelta(days=random.randint(18*365, 80*365))).strftime('%Y-%m-%d'),
            'diagnosis_code': f'ICD10_{random.randint(100, 999)}',
            'procedure_code': f'CPT_{random.randint(10000, 99999)}'
        })
    
    print(f"📊 Created {n_records} records with INTENTIONAL quality issues:")
    print(f"   - {int(n_records * 0.2)} records (20%) with NULL claim_id")
    print(f"   - {int(n_records * 0.3)} records (30%) with NULL member_id")
    print(f"   - {int(n_records * 0.1)} records (10%) with NULL service_date")
    print(f"   - {int(n_records * 0.1)} records (10%) with negative billed_amount")
    print(f"\n⚠️ Expected to be DROPPED by Silver: ~{int(n_records * 0.5)} records")
    
    return pd.DataFrame(data)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Generate and Write Bad Data File

# COMMAND ----------

# Configuration
account = "ucdatabricksstorage"  # Change if different
base = f"abfss://rawdata@{account}.dfs.core.windows.net/healthcare"
ts = datetime.now().strftime('%Y%m%d_%H%M%S')

print("🚨 Creating BAD claims file for quality testing...")
print("=" * 80)

bad_claims_df = create_bad_claims_837_data(100)
bad_claims_path = f"{base}/payer/claims/claims_837_{ts}.csv"

_single_file_write(
    spark.createDataFrame(bad_claims_df),
    bad_claims_path,
    fmt="csv",
    options={"header": "true", "delimiter": "|"}
)

print(f"\n✅ Created: {bad_claims_path}")
print("\n" + "=" * 80)
print("Next steps:")
print("1. Start the DLT pipeline")
print("2. Check silver_claims_enriched in DLT UI Data Quality tab for expectation metrics")
print("3. Query monitoring_data_quality table to see dropped records:")
print("   SELECT * FROM healthcare.default.monitoring_data_quality WHERE severity IN ('WARNING', 'CRITICAL')")
print("4. Compare Bronze vs Silver counts:")
print("   SELECT COUNT(*) FROM healthcare.default.bronze_claims_837;")
print("   SELECT COUNT(*) FROM healthcare.default.silver_claims_837;")

