# Databricks notebook source
catalog_name = "primeinsurance"
schema_name = "data_source"
volume_name = "raw_data_volume"

# COMMAND ----------

spark.sql(f"""
CREATE CATALOG IF NOT EXISTS {catalog_name}
""")

# COMMAND ----------

spark.sql(f"""
CREATE SCHEMA IF NOT EXISTS {catalog_name}.{schema_name}
""")

# COMMAND ----------

spark.sql(f"""
CREATE VOLUME IF NOT EXISTS {catalog_name}.{schema_name}.{volume_name}
""")

# COMMAND ----------

volume_path = f"/Volumes/{catalog_name}/{schema_name}/{volume_name}"
print("Volume Path:", volume_path)

# COMMAND ----------

bronze_schema_name = "bronze"
spark.sql(f"""CREATE SCHEMA IF NOT EXISTS {catalog_name}.{bronze_schema_name}""")