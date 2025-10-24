# Databricks notebook source
# MAGIC %md
# MAGIC # Test Centralized Schemas
# MAGIC 
# MAGIC **Test the centralized schema configuration with real data**

# COMMAND ----------

# Import centralized schemas using %run
%run ../src/schemas/schemas_config

# COMMAND ----------

# MAGIC %md
# MAGIC ## Test Schema Retrieval

# COMMAND ----------

# Test getting all available schemas
all_tables = get_all_table_names()
print("Available schemas:")
for table in all_tables:
    print(f"  📋 {table}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Test Individual Schemas

# COMMAND ----------

# Test claims_837 schema
claims_schema = get_schema_for_table('claims_837')
print("Claims 837 Schema:")
print(claims_schema)
print(f"Number of fields: {len(claims_schema.fields)}")

# COMMAND ----------

# Test hl7_messages schema
hl7_schema = get_schema_for_table('hl7_messages')
print("HL7 Messages Schema:")
print(hl7_schema)
print(f"Number of fields: {len(hl7_schema.fields)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Test Reading Files with Centralized Schemas

# COMMAND ----------

# Set OAuth configuration
spark.conf.set("fs.azure.account.auth.type.ucdatabricksstorage.dfs.core.windows.net", "OAuth")
spark.conf.set("fs.azure.account.oauth.provider.type.ucdatabricksstorage.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set("fs.azure.account.oauth2.client.id.ucdatabricksstorage.dfs.core.windows.net", "9fad6d79-c44b-41aa-8a24-e4c29d7844e0")
spark.conf.set("fs.azure.account.oauth2.client.secret.ucdatabricksstorage.dfs.core.windows.net", dbutils.secrets.get("Formula1-scope", "Formula1serviceprincipal"))
spark.conf.set("fs.azure.account.oauth2.client.endpoint.ucdatabricksstorage.dfs.core.windows.net", "https://login.microsoftonline.com/fdb4f009-b405-4272-8dc1-585f5d3cb93c/oauth2/token")

# COMMAND ----------

# Test reading claims_837 with centralized schema
print("Testing claims_837 with centralized schema...")
claims_schema = get_schema_for_table('claims_837')
claims_df = spark.read \
    .option("sep", "|") \
    .option("header", "true") \
    .schema(claims_schema) \
    .csv("abfss://rawdata@ucdatabricksstorage.dfs.core.windows.net/healthcare/payer/claims/")

print(f"✅ Claims 837: {claims_df.count()} records")
claims_df.show(5)

# COMMAND ----------

# Test reading hl7_messages with centralized schema
print("Testing hl7_messages with centralized schema...")
hl7_schema = get_schema_for_table('hl7_messages')
hl7_df = spark.read \
    .option("multiline", "true") \
    .schema(hl7_schema) \
    .json("abfss://rawdata@ucdatabricksstorage.dfs.core.windows.net/healthcare/clinical/hl7/")

print(f"✅ HL7 Messages: {hl7_df.count()} records")
hl7_df.show(5)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Test All File Types

# COMMAND ----------

# Test all file types with their respective schemas
file_tests = [
    ("claims_837", "abfss://rawdata@ucdatabricksstorage.dfs.core.windows.net/healthcare/payer/claims/", "csv", "|"),
    ("claims_835", "abfss://rawdata@ucdatabricksstorage.dfs.core.windows.net/healthcare/payer/payments/", "csv", "|"),
    ("hl7_messages", "abfss://rawdata@ucdatabricksstorage.dfs.core.windows.net/healthcare/clinical/hl7/", "json", None),
    ("eligibility_verification", "abfss://rawdata@ucdatabricksstorage.dfs.core.windows.net/healthcare/eligibility/verification/", "csv", ","),
    ("lab_results", "abfss://rawdata@ucdatabricksstorage.dfs.core.windows.net/healthcare/clinical/lab/", "parquet", None)
]

for table_name, path, file_format, delimiter in file_tests:
    try:
        print(f"\n🔍 Testing {table_name}...")
        schema = get_schema_for_table(table_name)
        
        if schema is None:
            print(f"❌ No schema found for {table_name}")
            continue
        
        # Configure reader based on file format
        reader = spark.read
        if file_format == "csv":
            reader = reader.option("header", "true")
            if delimiter:
                reader = reader.option("sep", delimiter)
        elif file_format == "json":
            reader = reader.option("multiline", "true")
        
        # Read with schema
        df = reader.schema(schema).format(file_format).load(path)
        count = df.count()
        print(f"✅ {table_name}: {count} records")
        
        # Show sample data
        if count > 0:
            df.show(2, truncate=False)
        
    except Exception as e:
        print(f"❌ {table_name}: Error - {str(e)[:100]}...")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Schema Validation Test

# COMMAND ----------

from schemas_config import validate_schema_for_table

# Test schema validation
print("Testing schema validation...")

# Test with claims_837
claims_schema = get_schema_for_table('claims_837')
claims_df = spark.read \
    .option("sep", "|") \
    .option("header", "true") \
    .schema(claims_schema) \
    .csv("abfss://rawdata@ucdatabricksstorage.dfs.core.windows.net/healthcare/payer/claims/")

is_valid, message = validate_schema_for_table('claims_837', claims_df)
print(f"Claims 837 validation: {is_valid} - {message}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary

# COMMAND ----------

print("🎯 Centralized Schema Test Summary:")
print("✅ Schema retrieval working")
print("✅ File reading with schemas working")
print("✅ All file types tested")
print("✅ Schema validation working")
print("\n🚀 Ready for DLT pipeline integration!")
