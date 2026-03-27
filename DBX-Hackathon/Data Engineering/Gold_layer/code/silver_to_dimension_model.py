import dlt as dp
from pyspark.sql.functions import col,current_timestamp

# DIMENSION TABLES

# 1. dim_customer
@dp.table(
    name="primeinsurance.gold_layer.dim_customer",
    comment="Customer dimension table with unique customer_id and region derived from silver layer"
)
def dim_customer():
    return (
        dp.read_stream("primeinsurance.silver_layer.silver_customers")
        .select("customer_id",
                "region",
                "state",
                "city",
                "default_flag",
                "balance",
                "hh_insurance",
                "car_loan",
                "job",
                "ingestion_timestamp")
        .dropDuplicates(["customer_id"])
        .withColumn("md_created_datetime",current_timestamp())
    )

# 2. dim_car
@dp.table(
    name="primeinsurance.gold_layer.dim_car",
    comment="Car dimension table containing unique car_id with model and name attributes"
)
def dim_car():
    return (
        dp.read_stream("primeinsurance.silver_layer.silver_cars")
        .select("car_id", "model", "name","km_driven","fuel","transmission","mileage_value","mileage_unit","engine_cc","max_power_bhp","torque","seats","ingestion_timestamp")
        .dropDuplicates(["car_id"])
        .withColumn("md_created_datetime",current_timestamp())
    )


# 3. dim_policy
@dp.table(
    name="primeinsurance.gold_layer.dim_policy",
    comment="Policy dimension table with policy_number and policy_csl attributes"
)
def dim_policy():
    return (
        dp.read_stream("primeinsurance.silver_layer.silver_policy")
        .select("policy_number","policy_bind_date","policy_state", "policy_csl","policy_deductable","policy_annual_premium","umbrella_limit","car_id","customer_id","ingestion_timestamp")
        .dropDuplicates(["policy_number"])
        .withColumn("md_created_datetime",current_timestamp())
    )


# FACT TABLES

# 4. fact_claims
@dp.table(
    name="primeinsurance.gold_layer.fact_claims",
    comment="Fact table at claim-level grain containing claim details joined with customer_id via policy"
)
def fact_claims():
    claims = dp.read_stream("primeinsurance.silver_layer.silver_claims")
    # we can't have two streams in a join, so we need to read the policy table as a static table
    policy = dp.read("primeinsurance.silver_layer.silver_policy")

    return (
        claims.alias("c")
        .join(
            policy.alias("p"),
            col("c.policy_id") == col("p.policy_number"),
            "left"
        )
        .select(
            col("c.claim_id"),
            col("p.customer_id"),
            col("c.policy_id"),

            col("c.claim_rejected"),
            col("c.incident_city"),

            col("c.authorities_contacted"),
            col("c.bodily_injuries"),
            col("c.incident_location"),
            col("c.incident_severity"),
            col("c.incident_state"),
            col("c.incident_type"),
            col("c.injury"),
            col("c.number_of_vehicles_involved"),
            col("c.police_report_available"),
            col("c.property"),
            col("property_damage"),
            col("c.vehicle"),
            col("c.witnesses"),
            col("c.ingestion_timestamp")
            # col("c.claim_logged_on"),
            # col("c.claim_processed_on")
        )
        .withColumn("md_created_datetime",current_timestamp())
    )


# 5. fact_sales
@dp.table(
    name="primeinsurance.gold_layer.fact_sales",
    comment="Fact table at sales/listing level grain containing vehicle sales and listing information"
)
def fact_sales():
    return (
        dp.read_stream("primeinsurance.silver_layer.silver_sales")
        .select(
            "sales_id",
            "ad_placed_on",
            "car_id",
            "owner",
            "sold_on",
            "original_selling_price",
            "region",
            "state",
            "city",
            "seller_type",
            "ingestion_timestamp"

        )
        .withColumn("md_created_datetime",current_timestamp())
    )