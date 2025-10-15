"""
Run any SQL query on Databricks - just like the web UI!
Edit the SQL query below and run: python run_sql.py
"""
import os
from databricks.connect import DatabricksSession

# Load environment variables
env_file = ".databricks/.databricks.env"
if os.path.exists(env_file):
    with open(env_file) as f:
        for line in f:
            if '=' in line and not line.startswith('#'):
                key, value = line.strip().split('=', 1)
                os.environ[key] = value

# Initialize Spark
spark = DatabricksSession.builder.getOrCreate()

# ========================================
# EDIT YOUR SQL QUERY HERE:
# ========================================

query = """
SHOW CATALOGS
"""

# Run the query
result = spark.sql(query)
result.show()

# ========================================
# More examples you can try:
# ========================================

# Show schemas in a catalog:
# spark.sql("SHOW SCHEMAS IN main").show()

# Show tables in a schema:
# spark.sql("SHOW TABLES IN main.default").show()

# Query data from a table:
# spark.sql("SELECT * FROM samples.nyctaxi.trips LIMIT 10").show()

# Describe a table:
# spark.sql("DESCRIBE samples.nyctaxi.trips").show()

