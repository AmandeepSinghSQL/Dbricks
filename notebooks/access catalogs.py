from databricks.connect import DatabricksSession

spark = DatabricksSession.builder.getOrCreate()

# Run SQL query
spark.sql("SHOW CATALOGS").show()
