from databricks.connect import DatabricksSession

spark = DatabricksSession.builder.getOrCreate()

spark.sql("SELECT * FROM `dev-catalog`.default.daily_product_totals").show()

