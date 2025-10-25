# Databricks notebook source
# MAGIC %md
# MAGIC # Healthcare Test Data Generator
# MAGIC 
# MAGIC **Creates timestamped test files in abfss external locations**
# MAGIC 
# MAGIC Run this notebook anytime to generate new files that Auto Loader will ingest.

# COMMAND ----------

import pandas as pd
from datetime import datetime, timedelta
import random

# Set random seed for reproducible data
random.seed(42)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Helper Functions

# COMMAND ----------

def _single_file_write(spark_df, target_file_path: str, fmt: str, options: dict = None):
    """Write a Spark DataFrame as a single file to target_file_path."""
    tmp_dir = target_file_path + "_tmp_write"
    writer = spark_df.coalesce(1).write.mode("overwrite")
    if options:
        for k, v in options.items():
            writer = writer.option(k, v)
    if fmt == "csv":
        writer.csv(tmp_dir)
    elif fmt == "json":
        writer.json(tmp_dir)
    elif fmt == "parquet":
        writer.parquet(tmp_dir)
    else:
        raise ValueError(f"Unsupported format: {fmt}")

    part_files = [f.path for f in dbutils.fs.ls(tmp_dir) if f.name.startswith("part-")]
    if not part_files:
        raise RuntimeError(f"No part files written in {tmp_dir}")
    dbutils.fs.cp(part_files[0], target_file_path)
    dbutils.fs.rm(tmp_dir, True)

def _abfss_base(account_name: str = None) -> str:
    """Get the base abfss path for healthcare data."""
    account = account_name or "ucdatabricksstorage"
    return f"abfss://rawdata@{account}.dfs.core.windows.net/healthcare"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Data Generation Functions

# COMMAND ----------

def create_claims_837_data(n_records: int = 1000):
    """
    Create sample claims 837 data (EDI format)
    
    IMPORTANT: Creates SKEWED provider distribution for cost optimization demo
    - 80% of claims go to provider_123 (INTENTIONAL SKEW)
    - 20% of claims distributed across 5 other specific providers for join consistency
    """
    data = []
    skewed_provider = "provider_123"
    other_providers = ["PROV201", "PROV202", "PROV203", "PROV204", "PROV205"]
    
    for i in range(n_records):
        # 80% of claims go to one provider (SKEW!)
        if i < n_records * 0.8:
            provider_id = skewed_provider
        else:
            # Remaining 20% distributed across 5 specific providers (deterministic)
            provider_id = other_providers[(i - int(n_records * 0.8)) % len(other_providers)]
        
        data.append({
            'claim_id': f'CLM837_{i+1:06d}',
            'member_id': f'MEM{random.randint(100000, 999999)}',
            'provider_id': provider_id,
            'service_date': (datetime.now() - timedelta(days=random.randint(1, 30))).strftime('%Y-%m-%d'),
            'billed_amount': round(random.uniform(50.0, 2000.0), 2),
            'paid_amount': round(random.uniform(40.0, 1800.0), 2),
            'subscriber_name': f'Patient_{i+1}',
            'date_of_birth': (datetime.now() - timedelta(days=random.randint(18*365, 80*365))).strftime('%Y-%m-%d'),
            'diagnosis_code': f'ICD10_{random.randint(100, 999)}',
            'procedure_code': f'CPT_{random.randint(10000, 99999)}'
        })
    
    print(f"📊 Created {n_records} claims with SKEWED distribution:")
    print(f"   - {int(n_records * 0.8)} claims (80%) → {skewed_provider}")
    print(f"   - {int(n_records * 0.2)} claims (20%) → 5 other providers")
    print(f"   This skew will demonstrate cost optimization benefits!")
    
    return pd.DataFrame(data)

def create_claims_835_data(n_records: int = 800):
    """
    Create sample claims 835 payment data
    
    IMPORTANT: Matches skewed claims for join demo
    - Creates payments for 80% of claims (800 out of 1000)
    - Uses same provider distribution as claims_837 for consistent joins
    """
    data = []
    skewed_provider = "provider_123"
    other_providers = ["PROV201", "PROV202", "PROV203", "PROV204", "PROV205"]
    
    for i in range(n_records):
        # Match claim_id pattern from claims_837 (payments for first 800 claims)
        claim_id = f'CLM837_{i+1:06d}'
        
        # Match provider distribution from claims_837
        if i < n_records * 0.8:
            provider_id = skewed_provider
        else:
            provider_id = other_providers[(i - int(n_records * 0.8)) % len(other_providers)]
        
        data.append({
            'payment_id': f'PAY835_{i+1:06d}',
            'claim_id': claim_id,
            'payment_amount': round(random.uniform(30.0, 1500.0), 2),
            'payment_date': (datetime.now() - timedelta(days=random.randint(1, 15))).strftime('%Y-%m-%d'),
            'adjustment_reason': random.choice(['COPAY', 'DEDUCTIBLE', 'COINSURANCE', 'DENIAL']),
            'member_id': f'MEM{random.randint(100000, 999999)}',
            'provider_id': provider_id
        })
    
    print(f"📊 Created {n_records} payments with SKEWED distribution:")
    print(f"   - {int(n_records * 0.8)} payments (80%) → {skewed_provider}")
    print(f"   - {int(n_records * 0.2)} payments (20%) → 5 other providers")
    print(f"   - Joinable with claims_837 on claim_id")
    
    return pd.DataFrame(data)

def create_hl7_messages_data(n_records: int = 75):
    """Create sample HL7 clinical messages"""
    data = []
    for i in range(n_records):
        data.append({
            'message_id': f'HL7_{i+1:06d}',
            'patient_id': f'PAT{random.randint(100000, 999999)}',
            'message_type': random.choice(['ADT', 'ORU', 'MDM', 'DFT']),
            'message_date': (datetime.now() - timedelta(days=random.randint(1, 20))).strftime('%Y-%m-%d'),
            'facility_id': f'FAC{random.randint(1000, 9999)}',
            'provider_npi': f'{random.randint(1000000000, 9999999999)}',
            'patient_name': f'Patient_{i+1}',
            'dob': (datetime.now() - timedelta(days=random.randint(18*365, 80*365))).strftime('%Y-%m-%d'),
            'message_content': f'HL7 message content for patient {i+1}',
            'status': random.choice(['ACTIVE', 'COMPLETED', 'PENDING'])
        })
    return pd.DataFrame(data)

def create_eligibility_data(n_records: int = 60):
    """Create sample eligibility verification data"""
    data = []
    for i in range(n_records):
        data.append({
            'verification_id': f'ELIG_{i+1:06d}',
            'member_id': f'MEM{random.randint(100000, 999999)}',
            'subscriber_id': f'SUB{random.randint(100000, 999999)}',
            'verification_status': random.choice(['ACTIVE', 'INACTIVE', 'PENDING', 'EXPIRED']),
            'verification_date': (datetime.now() - timedelta(days=random.randint(1, 25))).strftime('%Y-%m-%d'),
            'coverage_type': random.choice(['HMO', 'PPO', 'EPO', 'POS']),
            'member_name': f'Member_{i+1}',
            'effective_date': (datetime.now() - timedelta(days=random.randint(30, 365))).strftime('%Y-%m-%d'),
            'termination_date': (datetime.now() + timedelta(days=random.randint(30, 365))).strftime('%Y-%m-%d')
        })
    return pd.DataFrame(data)

def create_lab_results_data(n_records: int = 80):
    """Create sample lab results data"""
    data = []
    lab_tests = ['CBC', 'CMP', 'Lipid Panel', 'HbA1c', 'TSH', 'PSA', 'Glucose', 'Cholesterol']
    for i in range(n_records):
        data.append({
            'lab_id': f'LAB_{i+1:06d}',
            'patient_id': f'PAT{random.randint(100000, 999999)}',
            'test_code': f'LAB{random.randint(1000, 9999)}',
            'test_date': (datetime.now() - timedelta(days=random.randint(1, 28))).strftime('%Y-%m-%d'),
            'result_value': round(random.uniform(1.0, 200.0), 2),
            'normal_range': f'{round(random.uniform(1.0, 50.0), 1)}-{round(random.uniform(51.0, 100.0), 1)}',
            'patient_name': f'Patient_{i+1}',
            'dob': (datetime.now() - timedelta(days=random.randint(18*365, 80*365))).strftime('%Y-%m-%d'),
            'test_name': random.choice(lab_tests),
            'units': random.choice(['mg/dL', 'g/dL', 'U/L', '%', 'ng/mL']),
            'status': random.choice(['NORMAL', 'ABNORMAL', 'CRITICAL', 'PENDING'])
        })
    return pd.DataFrame(data)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Generate All Test Files

# COMMAND ----------

# Configuration
account_name = "ucdatabricksstorage"  # Change if needed
base = _abfss_base(account_name)
ts = datetime.now().strftime('%Y%m%d_%H%M%S')

print(f"Creating test healthcare files in: {base}")
print("=" * 80)

# COMMAND ----------

# 1) Claims 837 - CSV with | delimiter
print("\n1️⃣ Creating claims_837...")
claims_837_df = create_claims_837_data()
claims_837_path = f"{base}/payer/claims/claims_837_{ts}.csv"
_single_file_write(
    spark.createDataFrame(claims_837_df),
    claims_837_path,
    fmt="csv",
    options={"header": "true", "delimiter": "|"}
)
print(f"✅ Created: {claims_837_path}")

# COMMAND ----------

# 2) Claims 835 - CSV with | delimiter
print("\n2️⃣ Creating claims_835...")
claims_835_df = create_claims_835_data()
claims_835_path = f"{base}/payer/payments/claims_835_{ts}.csv"
_single_file_write(
    spark.createDataFrame(claims_835_df),
    claims_835_path,
    fmt="csv",
    options={"header": "true", "delimiter": "|"}
)
print(f"✅ Created: {claims_835_path}")

# COMMAND ----------

# 3) HL7 messages - JSON lines
print("\n3️⃣ Creating hl7_messages...")
hl7_df = create_hl7_messages_data()
hl7_path = f"{base}/clinical/hl7/hl7_{ts}.json"
_single_file_write(
    spark.createDataFrame(hl7_df),
    hl7_path,
    fmt="json",
    options=None
)
print(f"✅ Created: {hl7_path}")

# COMMAND ----------

# 4) Eligibility - CSV with , delimiter
print("\n4️⃣ Creating eligibility...")
eligibility_df = create_eligibility_data()
eligibility_path = f"{base}/eligibility/verification/eligibility_{ts}.csv"
_single_file_write(
    spark.createDataFrame(eligibility_df),
    eligibility_path,
    fmt="csv",
    options={"header": "true", "delimiter": ","}
)
print(f"✅ Created: {eligibility_path}")

# COMMAND ----------

# 5) Lab Results - Parquet
print("\n5️⃣ Creating lab_results...")
lab_df = create_lab_results_data()
lab_path = f"{base}/clinical/lab/lab_{ts}.parquet"
_single_file_write(
    spark.createDataFrame(lab_df),
    lab_path,
    fmt="parquet",
    options=None
)
print(f"✅ Created: {lab_path}")

# COMMAND ----------

print("\n" + "=" * 80)
print("✅ All test files created successfully!")
print("\n🎯 Ready for your metadata-driven DLT demo!")
print("Files match the metadata configuration in healthcare.metadata.file_metadata")
print("\nNext: Start your DLT pipeline to ingest these files")

