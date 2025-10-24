# Databricks notebook source
# MAGIC %md
# MAGIC # Healthcare File Schemas - Centralized Configuration
# MAGIC 
# MAGIC **Centralized schema definitions for all healthcare file types**
# MAGIC 
# MAGIC ### Key Features:
# MAGIC - **Centralized schemas** - all file types in one place
# MAGIC - **Easy maintenance** - add new file types easily
# MAGIC - **Type safety** - consistent schemas across pipeline
# MAGIC - **Scalable** - works for 500+ file types
# MAGIC - **Production ready** - proper schema management
# MAGIC 
# MAGIC ### Usage:
# MAGIC ```python
# MAGIC from schemas_config import get_schema_for_table
# MAGIC schema = get_schema_for_table('claims_837')
# MAGIC ```

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType, IntegerType, BooleanType

# COMMAND ----------

# MAGIC %md
# MAGIC ## Schema Definitions

# COMMAND ----------

def get_file_schemas():
    """
    Returns dictionary of schemas for all healthcare file types
    Key: table_name, Value: StructType schema
    """
    schemas = {
        # Claims 837 (EDI Format) - CSV with | delimiter
        'claims_837': StructType([
            StructField("claim_id", StringType(), True),
            StructField("member_id", StringType(), True),
            StructField("provider_id", StringType(), True),
            StructField("service_date", StringType(), True),
            StructField("billed_amount", StringType(), True),
            StructField("paid_amount", StringType(), True),
            StructField("subscriber_name", StringType(), True),
            StructField("date_of_birth", StringType(), True),
            StructField("diagnosis_code", StringType(), True),
            StructField("procedure_code", StringType(), True)
        ]),
        
        # Claims 835 (Payment Format) - CSV with | delimiter
        'claims_835': StructType([
            StructField("payment_id", StringType(), True),
            StructField("claim_id", StringType(), True),
            StructField("payment_amount", StringType(), True),
            StructField("payment_date", StringType(), True),
            StructField("adjustment_reason", StringType(), True),
            StructField("member_id", StringType(), True),
            StructField("provider_id", StringType(), True)
        ]),
        
        # HL7 Messages (Clinical Data) - JSON format
        'hl7_messages': StructType([
            StructField("message_id", StringType(), True),
            StructField("patient_id", StringType(), True),
            StructField("message_type", StringType(), True),
            StructField("message_date", StringType(), True),
            StructField("facility_id", StringType(), True),
            StructField("provider_npi", StringType(), True),
            StructField("patient_name", StringType(), True),
            StructField("dob", StringType(), True),
            StructField("message_content", StringType(), True),
            StructField("status", StringType(), True)
        ]),
        
        # Eligibility Verification - CSV with , delimiter
        'eligibility_verification': StructType([
            StructField("verification_id", StringType(), True),
            StructField("member_id", StringType(), True),
            StructField("subscriber_id", StringType(), True),
            StructField("verification_status", StringType(), True),
            StructField("verification_date", StringType(), True),
            StructField("coverage_type", StringType(), True),
            StructField("member_name", StringType(), True),
            StructField("effective_date", StringType(), True),
            StructField("termination_date", StringType(), True)
        ]),
        
        # Lab Results (Clinical Data) - Parquet format
        'lab_results': StructType([
            StructField("lab_id", StringType(), True),
            StructField("patient_id", StringType(), True),
            StructField("test_code", StringType(), True),
            StructField("test_date", StringType(), True),
            StructField("result_value", StringType(), True),
            StructField("normal_range", StringType(), True),
            StructField("patient_name", StringType(), True),
            StructField("dob", StringType(), True),
            StructField("test_name", StringType(), True),
            StructField("units", StringType(), True),
            StructField("status", StringType(), True)
        ])
    }
    
    return schemas

def get_schema_for_table(table_name):
    """
    Get schema for a specific table
    """
    schemas = get_file_schemas()
    return schemas.get(table_name, None)

def get_all_table_names():
    """
    Get list of all available table names
    """
    schemas = get_file_schemas()
    return list(schemas.keys())

# COMMAND ----------

# MAGIC %md
# MAGIC ## Test Schemas

# COMMAND ----------

# Test the schemas
if __name__ == "__main__":
    schemas = get_file_schemas()
    print("Available schemas:")
    for table_name, schema in schemas.items():
        print(f"  📋 {table_name}: {len(schema.fields)} fields")
        for field in schema.fields:
            print(f"    - {field.name}: {field.dataType}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Example Usage

# COMMAND ----------

# Example: Get schema for claims_837
claims_schema = get_schema_for_table('claims_837')
print("Claims 837 Schema:")
print(claims_schema)

# Example: Get all available tables
all_tables = get_all_table_names()
print(f"\nAll available tables: {all_tables}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Schema Validation

# COMMAND ----------

def validate_schema_for_table(table_name, df):
    """
    Validate that a DataFrame matches the expected schema for a table
    """
    expected_schema = get_schema_for_table(table_name)
    if expected_schema is None:
        return False, f"No schema found for table: {table_name}"
    
    # Check if all expected fields are present
    expected_fields = {field.name for field in expected_schema.fields}
    actual_fields = set(df.columns)
    
    missing_fields = expected_fields - actual_fields
    extra_fields = actual_fields - expected_fields
    
    if missing_fields:
        return False, f"Missing fields: {missing_fields}"
    if extra_fields:
        return False, f"Extra fields: {extra_fields}"
    
    return True, "Schema validation passed"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Add New Schema

# COMMAND ----------

def add_new_schema(table_name, schema):
    """
    Add a new schema to the configuration
    This would typically be used for dynamic schema addition
    """
    # In a real implementation, this would update a persistent store
    # For now, we'll just add to the in-memory dictionary
    schemas = get_file_schemas()
    schemas[table_name] = schema
    print(f"Added schema for {table_name} with {len(schema.fields)} fields")
    return schemas

# COMMAND ----------

# MAGIC %md
# MAGIC ## Schema Documentation

# COMMAND ----------

def get_schema_documentation():
    """
    Generate documentation for all schemas
    """
    schemas = get_file_schemas()
    doc = "# Healthcare File Schemas Documentation\n\n"
    
    for table_name, schema in schemas.items():
        doc += f"## {table_name}\n\n"
        doc += f"**Fields:** {len(schema.fields)}\n\n"
        doc += "| Field Name | Data Type | Nullable |\n"
        doc += "|------------|-----------|----------|\n"
        
        for field in schema.fields:
            doc += f"| {field.name} | {field.dataType} | {field.nullable} |\n"
        
        doc += "\n"
    
    return doc

# Generate and display documentation
print(get_schema_documentation())
