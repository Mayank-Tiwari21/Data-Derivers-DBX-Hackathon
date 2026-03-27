from pyspark import pipelines as dp
from pyspark.sql.functions import col, current_timestamp

base_path = "/Volumes/primeinsurance/data_source/raw_data"

# Bronze layer: Raw data ingestion from Volume using Auto Loader, with schema inference, path globbing and schema evolution to support schema changes

@dp.table(
    name="primeinsurance.bronze_layer.bronze_sales",
    comment="Bronze table for sales data from CSV files"
)
def bronze_sales():
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("header", "true")
        .option("cloudFiles.inferColumnTypes", "true")
        .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
        .option("recursiveFileLookup", "true")
        .option("pathGlobFilter", "sales*.csv")
        .load(base_path)
        .withColumn("source_file", col("_metadata.file_name"))
        .withColumn("ingestion_timestamp", current_timestamp())
    )


@dp.table(
    name="primeinsurance.bronze_layer.bronze_cars",
    comment="Bronze table for car data from CSV files"
)
def bronze_cars():
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("header", "true")
        .option("cloudFiles.inferColumnTypes", "true")
        .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
        .option("recursiveFileLookup", "true")
        .option("pathGlobFilter", "cars*.csv")
        .load(base_path)
        .withColumn("source_file", col("_metadata.file_name"))
        .withColumn("ingestion_timestamp", current_timestamp())
    )


@dp.table(
    name="primeinsurance.bronze_layer.bronze_customers",
    comment="Bronze table for customer data from CSV files"
)
def customers_raw():
    return (
        spark.readStream
            .format("cloudFiles")
            .option("cloudFiles.format", "csv")
            .option("header", "true")
            .option("cloudFiles.inferColumnTypes", "true")
            .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
            .option("recursiveFileLookup", "true")
            .option("pathGlobFilter", "customers_*.csv")
            .load(base_path)
            .withColumn("source_file", col("_metadata.file_name"))
            .withColumn("ingestion_timestamp", current_timestamp())
    )


@dp.table(
    name="primeinsurance.bronze_layer.bronze_claims",
    comment="Bronze table for claims data from JSON files"
)
def bronze_claims():
    return (
        spark.readStream
            .format("cloudFiles")
            .option("cloudFiles.format", "json")
            .option("cloudFiles.inferColumnTypes", "true")
            .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
            .option("recursiveFileLookup", "true")
            .option("pathGlobFilter", "claims_*.json")
            .option("multiLine", "true")
            .load(base_path)
            .withColumn("source_file", col("_metadata.file_name"))
            .withColumn("ingestion_timestamp", current_timestamp())
    )


@dp.table(
    name="primeinsurance.bronze_layer.bronze_policy",
    comment="Bronze table for policy data extracted from claims"
)
def bronze_policy():
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("header", "true")
        .option("cloudFiles.inferColumnTypes", "true")
        .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
        .option("recursiveFileLookup", "true")
        .option("pathGlobFilter", "policy*.csv")
        .load(base_path)
        .withColumn("source_file", col("_metadata.file_name"))
        .withColumn("ingestion_timestamp", current_timestamp())
    )
