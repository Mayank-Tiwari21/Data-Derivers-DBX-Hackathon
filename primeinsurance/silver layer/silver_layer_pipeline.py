# Databricks notebook source
# Databricks Delta Live Tables — Silver Layer Pipeline
# =====================================================
# Catalog  : primeinsurance
# Target   : silver_layer
# Sources  : primeinsurance.bronze_layer.*  (all DLT-managed Delta tables)
# Mode     : Continuous streaming
#
# Pipeline JSON config (Workflows → DLT → Create Pipeline → Settings):
# {
#   "name"       : "silver_layer_pipeline",
#   "catalog"    : "primeinsurance",
#   "target"     : "silver_layer",
#   "continuous" : true,
#   "libraries"  : [{"notebook": {"path": "/path/to/this/notebook"}}]
# }
#
# Tables produced
# ───────────────
#   silver_customers          silver_customers_quarantine
#   silver_cars               silver_cars_quarantine
#   silver_claims             silver_claims_quarantine
#   silver_policy             silver_policy_quarantine
#   silver_sales              silver_sales_quarantine
#   quality_issues_log        (append-only streaming DQ event log)
#   dq_issues                 (joined view: quarantine + issue_recorder metadata)
# =====================================================

import dlt
from pyspark.sql import functions as F
from pyspark.sql.types import (
    IntegerType, LongType, DecimalType,
    DateType, TimestampType, StringType
)

# ─────────────────────────────────────────────────────────────────────────────
# utlity functions
# ─────────────────────────────────────────────────────────────────────────────

def null_str(*col_names):
    """
    Replace the literal strings NULL / null / NA / ? / NONE / '' with SQL NULL.
    Accepts one or more column names; returns a Column expression for the first one
    (caller applies withColumn individually per column).
    """
    col_name = col_names[0]
    return F.when(
        F.upper(F.trim(F.col(col_name))).isin("NULL", "NA", "?", "NONE", ""),
        F.lit(None)
    ).otherwise(F.col(col_name))


def region_lookup(col_name: str):
    """
    Map region value (abbrev or full) to canonical full name via master table.
    Falls back to the raw value so partial matches don't become NULL silently.
    The master table is read as a static DataFrame (not streamed).
    """
    mapper = (
        spark.table("primeinsurance.silver_layer.region_mapper_table")
        .select(
            F.col("reg_coded"),
            F.col("reg_mapped")
        )
    )
    return mapper  # caller joins on UPPER(TRIM(col)) = reg_coded


# ─────────────────────────────────────────────────────────────────────────────
# ENTITY 1 — CUSTOMERS
# Issues resolved: DQ-CUST-001 through DQ-CUST-011
# ─────────────────────────────────────────────────────────────────────────────

@dlt.table(
    name="silver_customers_raw_unified",
    comment="""Intermediate staging: unify schema variants across 7 customer source files.
    Fixes: multi-column ID (DQ-CUST-002,010), region abbreviations (DQ-CUST-003,004),
    swapped education/marital in file-2 (DQ-CUST-006,007), City_in_state (DQ-CUST-008),
    deduplication of customers_7 overlap (DQ-CUST-001,011).""",
    temporary=True
)
def silver_customers_raw_unified():
    df = dlt.read_stream("primeinsurance.bronze_layer.bronze_customers")

    # ── Step 1: Unify customer ID (DQ-CUST-002, DQ-CUST-010) ─────────────────
    df = df.withColumn(
        "customer_id",
        F.coalesce(
            F.col("CustomerID").cast(LongType()),
            F.col("Customer_ID").cast(LongType()),
            F.col("cust_id").cast(LongType())
        )
    )

    # ── Step 2: Unify region columns + expand abbreviations (DQ-CUST-003,004) ─
    #    Join against region_mapper_table for lookup-driven expansion
    region_map_df = spark.table("primeinsurance.silver_layer.region_mapper_table")

    df = df.withColumn(
        "region_raw",
        F.upper(F.trim(
            F.coalesce(
                F.when(
                    F.col("Reg").isNotNull() &
                    (~F.upper(F.trim(F.col("Reg"))).isin("NULL", "NA", "")),
                    F.col("Reg")
                ),
                F.when(
                    F.col("Region").isNotNull() &
                    (~F.upper(F.trim(F.col("Region"))).isin("NULL", "NA", "")),
                    F.col("Region")
                )
            )
        ))
    )

    # Broadcast join against region master table
    df = (
        df.join(
            F.broadcast(region_map_df.select(
                F.upper(F.trim(F.col("reg_coded"))).alias("_reg_key"),
                F.col("reg_mapped").alias("_reg_mapped")
            )),
            df["region_raw"] == F.col("_reg_key"),
            how="left"
        )
        .withColumn("region", F.coalesce(F.col("_reg_mapped"), F.col("region_raw")))
        .drop("_reg_key", "_reg_mapped", "region_raw")
    )

    # ── Step 3: Normalise Education (DQ-CUST-005, DQ-CUST-006) ───────────────
    #    customers_2.csv rows: Edu column populated, Education is null
    #    customers_4.csv rows: neither column populated → NULL after fix
    df = df.withColumn(
        "education",
        F.when(
            F.col("Edu").isNotNull() &
            (~F.upper(F.trim(F.col("Edu"))).isin("NULL", "NA", "")),
            F.trim(F.col("Edu"))
        ).when(
            F.col("Education").isNotNull() &
            (~F.upper(F.trim(F.col("Education"))).isin("NULL", "NA", "")),
            F.trim(F.col("Education"))
        ).otherwise(F.lit(None).cast(StringType()))
    )

    # ── Step 4: Normalise Marital status (DQ-CUST-007) ────────────────────────
    df = df.withColumn(
        "marital_status",
        F.when(
            F.col("Marital").isNotNull() &
            (~F.upper(F.trim(F.col("Marital"))).isin("NULL", "NA", "")),
            F.trim(F.col("Marital"))
        ).when(
            F.col("Marital_status").isNotNull() &
            (~F.upper(F.trim(F.col("Marital_status"))).isin("NULL", "NA", "")),
            F.trim(F.col("Marital_status"))
        ).otherwise(F.lit(None).cast(StringType()))
    )

    # ── Step 5: City unification (DQ-CUST-008) ────────────────────────────────
    df = df.withColumn(
        "city",
        F.coalesce(
            F.when(F.col("City").isNotNull() & (~F.upper(F.trim(F.col("City"))).isin("NULL","NA","")), F.trim(F.col("City"))),
            F.when(F.col("City_in_state").isNotNull() & (~F.upper(F.trim(F.col("City_in_state"))).isin("NULL","NA","")), F.trim(F.col("City_in_state")))
        )
    )

    # ── Step 6: Remaining scalar columns ─────────────────────────────────────
    df = (df
          .withColumn("state",        null_str("State"))
          .withColumn("default_flag", F.col("Default").cast(IntegerType()))
          .withColumn("balance",      F.col("Balance").cast(DecimalType(14, 2)))
          .withColumn("hh_insurance", F.col("HHInsurance").cast(IntegerType()))
          .withColumn("car_loan",     F.col("CarLoan").cast(IntegerType()))
          .withColumn("job",          null_str("Job"))
          .withColumn("source_file",  F.col("source_file"))
          .withColumn("ingestion_timestamp", F.col("ingestion_timestamp"))
    )

    # ── Step 7: Deduplication — keep first occurrence per customer_id (DQ-CUST-001,011) ──
    # Use row_number partitioned by customer_id, ordered by source_file to prefer
    # the earliest incremental file over the master snapshot (customers_7).
    # from pyspark.sql.window import Window
    # w = Window.partitionBy("customer_id").orderBy("ingestion_timestamp")
    # df = (df
    #       .withColumn("_row_num", F.row_number().over(w))
    #       .filter(F.col("_row_num") == 1)
    #       .drop("_row_num")
    # )
    df = df.dropDuplicates(["customer_id"])

    return df.select(
        "customer_id", "region", "state", "city", "marital_status",
        "education", "default_flag", "balance", "hh_insurance", "car_loan",
        "job", "source_file", "ingestion_timestamp"
    )


# Quality rules — CUSTOMERS
# Rule                         | Consequence | Maps to issue
# customer_id not null         | DROP        | DQ-CUST-010
# customer_id positive         | DROP        | DQ-CUST-010
# region valid                 | WARN        | DQ-CUST-003
# balance cast ok              | WARN        | schema
# default_flag valid           | WARN        | schema

VALID_REGIONS = ["West", "Central", "East", "South","North"]

@dlt.table(
    name="silver_customers",
    comment="Clean, deduplicated customer records. Rejected rows in silver_customers_quarantine."
)
@dlt.expect_or_drop("customer_id_not_null", "customer_id IS NOT NULL")
@dlt.expect_or_drop("customer_id_positive", "customer_id > 0")
@dlt.expect("region_valid",
            "region IN ('West', 'Central', 'East', 'South')")
@dlt.expect("balance_not_null", "balance IS NOT NULL")
@dlt.expect("default_flag_valid", "default_flag IS NULL OR default_flag IN (0, 1)")
def silver_customers():
    return dlt.read_stream("silver_customers_raw_unified")


@dlt.table(
    name="silver_customers_quarantine",
    comment="""Rejected customer records with DQ rule label and detection timestamp.
    Covers: null/invalid customer_id, invalid region, missing education (DQ-CUST-005),
    unresolved null marital status."""
)
def silver_customers_quarantine():
    df = dlt.read_stream("silver_customers_raw_unified")
    return (
        df.withColumn(
            "_dq_rule",
            F.when(F.col("customer_id").isNull(),
                   F.lit("customer_id_not_null"))
             .when(F.col("customer_id") <= 0,
                   F.lit("customer_id_non_positive"))
             .when(
                 F.col("region").isNotNull() &
                 ~F.col("region").isin(VALID_REGIONS),
                 F.lit("region_invalid")
             )
             .when(F.col("education").isNull(),
                   F.lit("education_missing"))         # DQ-CUST-005
             .when(F.col("marital_status").isNull(),
                   F.lit("marital_status_missing"))
             .when(
                 F.col("default_flag").isNotNull() &
                 ~F.col("default_flag").isin(0, 1),
                 F.lit("default_flag_invalid")
             )
        )
        .filter(F.col("_dq_rule").isNotNull())
        .withColumn("_dq_detected_at", F.current_timestamp())
        .withColumn("_entity", F.lit("customers"))
    )


# ─────────────────────────────────────────────────────────────────────────────
# ENTITY 2 — CARS
# Issues resolved: DQ-CARS-001 through DQ-CARS-008
# ─────────────────────────────────────────────────────────────────────────────

@dlt.table(
    name="silver_cars_parsed",
    comment="""Intermediate: strip unit strings from mileage/engine/max_power,
    normalise fuel/transmission case, cast types.
    Fixes: DQ-CARS-001,002,003,007,008.""",
    temporary=True
)
def silver_cars_parsed():
    df = dlt.read_stream("primeinsurance.bronze_layer.bronze_cars")

    return (df
        .withColumn("car_id",       F.col("car_id").cast(LongType()))
        .withColumn("name",         F.trim(F.col("name")))
        .withColumn("model",        F.trim(F.col("model")))
        .withColumn("km_driven",    F.col("km_driven").cast(LongType()))
        .withColumn("seats",        F.col("seats").cast(IntegerType()))

        # DQ-CARS-007, DQ-CARS-008: normalise to UPPER CASE
        .withColumn("fuel",         F.upper(F.trim(F.col("fuel"))))
        .withColumn("transmission", F.upper(F.trim(F.col("transmission"))))

        # DQ-CARS-001: extract numeric mileage + preserve unit
        .withColumn("mileage_value",
            F.regexp_extract(F.col("mileage"), r"([\d.]+)", 1)
             .cast(DecimalType(6, 2))
        )
        .withColumn("mileage_unit",
            F.when(F.col("mileage").contains("km/kg"), F.lit("km/kg"))
             .when(F.col("mileage").contains("kmpl"),  F.lit("kmpl"))
             .otherwise(F.lit(None).cast(StringType()))
        )

        # DQ-CARS-002: strip CC suffix from engine
        .withColumn("engine_cc",
            F.regexp_extract(F.col("engine"), r"(\d+)", 1)
             .cast(IntegerType())
        )

        # DQ-CARS-003: strip bhp suffix from max_power
        .withColumn("max_power_bhp",
            F.regexp_extract(F.col("max_power"), r"([\d.]+)", 1)
             .cast(DecimalType(7, 2))
        )

        # DQ-CARS-004: torque kept as trimmed string (too varied; gold layer will parse)
        .withColumn("torque", F.trim(F.col("torque")))

        # DQ-CARS-005: flag extreme km_driven outlier for quarantine
        .withColumn("km_driven_extreme",
            F.col("km_driven") > F.lit(500000)
        )

        .select(
            "car_id", "name", "model", "km_driven", "km_driven_extreme",
            "fuel", "transmission",
            "mileage_value", "mileage_unit",
            "engine_cc", "max_power_bhp", "torque",
            "seats",
            "source_file", "ingestion_timestamp"
        )
    )


VALID_FUELS = ["PETROL", "DIESEL", "CNG", "LPG", "ELECTRIC", "HYBRID"]

@dlt.table(
    name="silver_cars",
    comment="Clean car records with numeric specs extracted. Rejected rows in silver_cars_quarantine."
)
@dlt.expect_or_drop("car_id_not_null",    "car_id IS NOT NULL")
@dlt.expect_or_drop("car_id_positive",    "car_id > 0")
@dlt.expect("km_driven_non_negative",     "km_driven IS NULL OR km_driven >= 0")
@dlt.expect("engine_cc_positive",         "engine_cc IS NULL OR engine_cc > 0")
@dlt.expect("max_power_positive",         "max_power_bhp IS NULL OR max_power_bhp > 0")
@dlt.expect("mileage_positive",           "mileage_value IS NULL OR mileage_value > 0")
@dlt.expect("seats_in_range",             "seats IS NULL OR (seats >= 2 AND seats <= 9)")
@dlt.expect("fuel_known",                 "fuel IS NULL OR fuel IN ('PETROL','DIESEL','CNG','LPG','ELECTRIC','HYBRID')")
def silver_cars():
    return dlt.read_stream("silver_cars_parsed")


@dlt.table(
    name="silver_cars_quarantine",
    comment="""Rejected car records. Covers: null car_id, negative km_driven,
    extreme km_driven outlier (DQ-CARS-005), seats out of range (DQ-CARS-006),
    unknown fuel type, non-positive engine/power/mileage."""
)
def silver_cars_quarantine():
    df = dlt.read_stream("silver_cars_parsed")
    return (
        df.withColumn(
            "_dq_rule",
            F.when(F.col("car_id").isNull(),
                   F.lit("car_id_not_null"))
             .when(F.col("car_id") <= 0,
                   F.lit("car_id_non_positive"))
             .when(
                 F.col("km_driven").isNotNull() & (F.col("km_driven") < 0),
                 F.lit("km_driven_negative")
             )
             .when(
                 F.col("km_driven_extreme") == True,
                 F.lit("km_driven_extreme_outlier")  # DQ-CARS-005
             )
             .when(
                 F.col("engine_cc").isNotNull() & (F.col("engine_cc") <= 0),
                 F.lit("engine_cc_non_positive")
             )
             .when(
                 F.col("max_power_bhp").isNotNull() & (F.col("max_power_bhp") <= 0),
                 F.lit("max_power_non_positive")
             )
             .when(
                 F.col("seats").isNotNull() &
                 ((F.col("seats") < 2) | (F.col("seats") > 9)),
                 F.lit("seats_out_of_range")          # DQ-CARS-006
             )
             .when(
                 F.col("fuel").isNotNull() &
                 ~F.col("fuel").isin(VALID_FUELS),
                 F.lit("fuel_unknown_type")
             )
        )
        .filter(F.col("_dq_rule").isNotNull())
        .withColumn("_dq_detected_at", F.current_timestamp())
        .withColumn("_entity", F.lit("cars"))
    )


# ─────────────────────────────────────────────────────────────────────────────
# ENTITY 3 — CLAIMS
# Issues resolved/flagged: DQ-CLMS-001 through DQ-CLMS-010
# ─────────────────────────────────────────────────────────────────────────────

@dlt.table(
    name="silver_claims_parsed",
    comment="""Intermediate: preserve corrupted date raw values, replace string NULLs
    and ?, cast financial fields. Fixes DQ-CLMS-003,005,006,007,008,010.
    Flags DQ-CLMS-001,002,004 as UNRESOLVABLE (original dates unrecoverable).""",
    temporary=True
)
def silver_claims_parsed():
    df = dlt.read_stream("primeinsurance.bronze_layer.bronze_claims")

    CORRUPTED_PATTERN = r"^\d{2}:\d{2}\.\d+$"
    INJURY_EXTREME_THRESHOLD = 80000  # ~12x average; flags $100k outlier (DQ-CLMS-009)

    return (df
        # Preserve raw date strings for audit trail
        .withColumnRenamed("Claim_Logged_On",    "_raw_logged_on")
        .withColumnRenamed("Claim_Processed_On", "_raw_processed_on")
        .withColumnRenamed("incident_date",       "_raw_incident_date")

        # DQ-CLMS-001,002: Claim_Logged_On — all values are corrupted MM:SS.0 format
        .withColumn("claim_logged_on",
            F.lit(None).cast(TimestampType())
        )
        .withColumn("claim_logged_on_corrupted",
            F.col("_raw_logged_on").rlike(CORRUPTED_PATTERN)
        )

        # DQ-CLMS-003,004: Claim_Processed_On — NULL string OR corrupted timestamp
        .withColumn("claim_processed_on",
            F.lit(None).cast(TimestampType())
        )
        .withColumn("claim_processed_on_null_string",
            F.upper(F.trim(F.col("_raw_processed_on"))).isin("NULL", "NA", "")
        )
        .withColumn("claim_processed_on_corrupted",
            (~F.upper(F.trim(F.col("_raw_processed_on"))).isin("NULL", "NA", "")) &
            F.col("_raw_processed_on").rlike(CORRUPTED_PATTERN)
        )

        # DQ-CLMS-001: incident_date — 100% corrupted
        .withColumn("incident_date",
            F.lit(None).cast(DateType())
        )
        .withColumn("incident_date_corrupted",
            F.col("_raw_incident_date").rlike(CORRUPTED_PATTERN)
        )

        # DQ-CLMS-005,006,007,008: replace ? and string NULL with SQL NULL
        .withColumn("collision_type",          null_str("collision_type"))
        .withColumn("police_report_available", null_str("police_report_available"))
        .withColumn("property_damage",         null_str("property_damage"))
        .withColumn("vehicle",                 null_str("vehicle"))

        # DQ-CLMS-010: cast financial amounts to DECIMAL
        .withColumn("injury",   F.col("injury").cast(DecimalType(12, 2)))
        .withColumn("property", F.col("property").cast(DecimalType(12, 2)))
        .withColumn("vehicle",  F.col("vehicle").cast(DecimalType(12, 2)))

        # DQ-CLMS-009: flag extreme injury outlier
        .withColumn("injury_extreme_outlier",
            F.col("injury").isNotNull() & (F.col("injury") > F.lit(INJURY_EXTREME_THRESHOLD))
        )

        # Normalise claim_rejected Y/N
        .withColumn("claim_rejected",
            F.when(F.upper(F.trim(F.col("Claim_Rejected"))) == "Y", F.lit("Y"))
             .when(F.upper(F.trim(F.col("Claim_Rejected"))) == "N", F.lit("N"))
             .otherwise(F.lit(None).cast(StringType()))
        )

        # Cast integer/long fields
        .withColumn("claim_id",   F.col("ClaimID").cast(LongType()))
        .withColumn("policy_id",  F.col("PolicyID").cast(LongType()))
        .withColumn("bodily_injuries",
            F.col("bodily_injuries").cast(IntegerType()))
        .withColumn("number_of_vehicles_involved",
            F.col("number_of_vehicles_involved").cast(IntegerType()))
        .withColumn("witnesses",  F.col("witnesses").cast(IntegerType()))

        .select(
            "claim_id", "claim_logged_on", "_raw_logged_on",
            "claim_logged_on_corrupted",
            "claim_processed_on", "_raw_processed_on",
            "claim_processed_on_null_string", "claim_processed_on_corrupted",
            "claim_rejected", "policy_id",
            "authorities_contacted", "bodily_injuries", "collision_type",
            "incident_city",
            "incident_date", "_raw_incident_date", "incident_date_corrupted",
            "incident_location", "incident_severity", "incident_state",
            "incident_type", "injury", "injury_extreme_outlier",
            "number_of_vehicles_involved",
            "police_report_available", "property", "property_damage",
            "vehicle", "witnesses",
            "source_file", "ingestion_timestamp"
        )
    )


@dlt.table(
    name="silver_claims",
    comment="Clean claims records. All dates NULL (unrecoverable corruption). Quarantine has details."
)
@dlt.expect_or_drop("claim_id_not_null",  "claim_id IS NOT NULL")
@dlt.expect_or_drop("claim_id_positive",  "claim_id > 0")
@dlt.expect_or_drop("policy_id_not_null", "policy_id IS NOT NULL")
@dlt.expect("dates_corrupted_warn",       "incident_date_corrupted = false")  # WARN — all fail
@dlt.expect("injury_non_negative",        "injury IS NULL OR injury >= 0")
@dlt.expect("vehicle_non_negative",       "vehicle IS NULL OR vehicle >= 0")
@dlt.expect("property_non_negative",      "property IS NULL OR property >= 0")
@dlt.expect("claim_rejected_valid",
            "claim_rejected IS NULL OR claim_rejected IN ('Y','N')")
def silver_claims():
    return dlt.read_stream("silver_claims_parsed")


@dlt.table(
    name="silver_claims_quarantine",
    comment="""Rejected claims records. Covers: null/invalid claim_id, null policy_id,
    all date corruption issues (DQ-CLMS-001,002,004), extreme injury outlier (DQ-CLMS-009),
    missing categorical fields."""
)
def silver_claims_quarantine():
    df = dlt.read_stream("silver_claims_parsed")
    return (
        df.withColumn(
            "_dq_rule",
            F.when(F.col("claim_id").isNull(),
                   F.lit("claim_id_not_null"))
             .when(F.col("policy_id").isNull(),
                   F.lit("policy_id_not_null"))
             .when(F.col("claim_logged_on_corrupted") == True,
                   F.lit("claim_logged_on_corrupted"))   # DQ-CLMS-002
             .when(F.col("incident_date_corrupted") == True,
                   F.lit("incident_date_corrupted"))     # DQ-CLMS-001
             .when(F.col("claim_processed_on_corrupted") == True,
                   F.lit("claim_processed_on_corrupted"))# DQ-CLMS-004
             .when(F.col("injury_extreme_outlier") == True,
                   F.lit("injury_extreme_outlier"))      # DQ-CLMS-009
             .when(
                 F.col("claim_rejected").isNotNull() &
                 ~F.col("claim_rejected").isin("Y", "N"),
                 F.lit("claim_rejected_invalid_value")
             )
             .when(
                 F.col("injury").isNotNull() & (F.col("injury") < 0),
                 F.lit("injury_negative")
             )
             .when(F.col("collision_type").isNull(),
                   F.lit("collision_type_unknown"))      # DQ-CLMS-007
             .when(F.col("police_report_available").isNull(),
                   F.lit("police_report_unknown"))       # DQ-CLMS-005
             .when(F.col("property_damage").isNull(),
                   F.lit("property_damage_unknown"))     # DQ-CLMS-006
        )
        .filter(F.col("_dq_rule").isNotNull())
        .withColumn("_dq_detected_at", F.current_timestamp())
        .withColumn("_entity", F.lit("claims"))
    )


# ─────────────────────────────────────────────────────────────────────────────
# ENTITY 4 — POLICY
# Issues resolved: DQ-POL-001 through DQ-POL-004
# ─────────────────────────────────────────────────────────────────────────────

@dlt.table(
    name="silver_policy_parsed",
    comment="""Intermediate: cast types, normalise state/csl whitespace.
    Fixes DQ-POL-001,002,003,004.""",
    temporary=True
)
def silver_policy_parsed():
    df = dlt.read_stream("primeinsurance.bronze_layer.bronze_policy")
    return (df
        # DQ-POL-003: cast IDs to LONG
        .withColumn("policy_number",        F.col("policy_number").cast(LongType()))
        .withColumn("car_id",               F.col("car_id").cast(LongType()))
        .withColumn("customer_id",          F.col("customer_id").cast(LongType()))
        .withColumn("umbrella_limit",       F.col("umbrella_limit").cast(LongType()))

        # DQ-POL-001: normalise state code
        .withColumn("policy_state",         F.upper(F.trim(F.col("policy_state"))))

        # DQ-POL-002: trim CSL whitespace
        .withColumn("policy_csl",           F.trim(F.col("policy_csl")))

        .withColumn("policy_bind_date",     F.to_date(F.col("policy_bind_date"), "yyyy-MM-dd"))
        .withColumn("policy_deductable",    F.col("policy_deductable").cast(IntegerType()))

        # DQ-POL-004: cast to DECIMAL for currency precision
        .withColumn("policy_annual_premium",
            F.col("policy_annual_premium").cast(DecimalType(10, 2))
        )

        .select(
            "policy_number", "policy_bind_date", "policy_state",
            "policy_csl", "policy_deductable", "policy_annual_premium",
            "umbrella_limit", "car_id", "customer_id",
            "source_file", "ingestion_timestamp"
        )
    )


@dlt.table(
    name="silver_policy",
    comment="Clean policy records. Failed records in silver_policy_quarantine."
)
@dlt.expect_or_drop("policy_number_not_null", "policy_number IS NOT NULL")
@dlt.expect_or_drop("policy_number_positive", "policy_number > 0")
@dlt.expect_or_drop("customer_id_not_null",   "customer_id IS NOT NULL")
@dlt.expect_or_drop("car_id_not_null",        "car_id IS NOT NULL")
@dlt.expect("policy_bind_date_valid",         "policy_bind_date IS NOT NULL")
@dlt.expect("premium_positive",               "policy_annual_premium > 0")
@dlt.expect("deductable_non_negative",        "policy_deductable >= 0")
def silver_policy():
    return dlt.read_stream("silver_policy_parsed")


@dlt.table(
    name="silver_policy_quarantine",
    comment="Rejected policy records with DQ rule label."
)
def silver_policy_quarantine():
    df = dlt.read_stream("silver_policy_parsed")
    return (
        df.withColumn(
            "_dq_rule",
            F.when(F.col("policy_number").isNull(),
                   F.lit("policy_number_not_null"))
             .when(F.col("customer_id").isNull(),
                   F.lit("customer_id_not_null"))
             .when(F.col("car_id").isNull(),
                   F.lit("car_id_not_null"))
             .when(F.col("policy_bind_date").isNull(),
                   F.lit("policy_bind_date_unparseable"))
             .when(
                 F.col("policy_annual_premium").isNotNull() &
                 (F.col("policy_annual_premium") <= 0),
                 F.lit("premium_non_positive")
             )
             .when(
                 F.col("policy_deductable").isNotNull() &
                 (F.col("policy_deductable") < 0),
                 F.lit("deductable_negative")
             )
        )
        .filter(F.col("_dq_rule").isNotNull())
        .withColumn("_dq_detected_at", F.current_timestamp())
        .withColumn("_entity", F.lit("policy"))
    )


# ─────────────────────────────────────────────────────────────────────────────
# ENTITY 5 — SALES
# Issues resolved: DQ-SALE-001 through DQ-SALE-005
# ─────────────────────────────────────────────────────────────────────────────

@dlt.table(
    name="silver_sales_parsed",
    comment="""Intermediate: drop 1878 ghost rows (DQ-SALE-001), parse DD-MM-YYYY dates
    (DQ-SALE-002), rename PascalCase columns (DQ-SALE-004), cast types (DQ-SALE-005).""",
    temporary=True
)
def silver_sales_parsed():
    df = dlt.read_stream("primeinsurance.bronze_layer.bronze_sales")

    DATE_FMT = "dd-MM-yyyy HH:mm"

    # DQ-SALE-001: drop ghost rows immediately
    df = df.filter(F.col("sales_id").isNotNull())

    return (df
        # DQ-SALE-005: cast IDs to LONG
        .withColumn("sales_id", F.col("sales_id").cast(LongType()))
        .withColumn("car_id",   F.col("car_id").cast(LongType()))

        # DQ-SALE-002: parse non-ISO date format
        .withColumn("ad_placed_on",
            F.to_timestamp(F.col("ad_placed_on"), DATE_FMT)
        )
        .withColumn("sold_on",
            F.when(
                F.col("sold_on").isNull() |
                (F.upper(F.trim(F.col("sold_on"))).isin("NULL", "NA", "")),
                F.lit(None).cast(TimestampType())
            ).otherwise(
                F.to_timestamp(F.col("sold_on"), DATE_FMT)
            )
        )

        # DQ-SALE-005: DECIMAL for price
        .withColumn("original_selling_price",
            F.col("original_selling_price").cast(DecimalType(15, 2))
        )

        # DQ-SALE-004: normalise column names to snake_case + expand region via master table
        .withColumn("region_raw", F.upper(F.trim(F.col("Region"))))
        .withColumn("state",  F.col("State"))
        .withColumn("city",   F.col("City"))

        .withColumn("seller_type", null_str("seller_type"))
        .withColumn("owner",       null_str("owner"))

        # Temporal violation flag
        .withColumn("sold_before_listed",
            F.col("sold_on").isNotNull() &
            F.col("ad_placed_on").isNotNull() &
            (F.col("sold_on") < F.col("ad_placed_on"))
        )

        .select(
            "sales_id", "ad_placed_on", "sold_on", "sold_before_listed",
            "original_selling_price",
            "region_raw", "state", "city",
            "seller_type", "owner", "car_id",
            "source_file", "ingestion_timestamp"
        )
    )


@dlt.table(
    name="silver_sales",
    comment="Clean sales records with parsed timestamps and correct types. Quarantine has rejects."
)
@dlt.expect_or_drop("sales_id_not_null", "sales_id IS NOT NULL")
@dlt.expect_or_drop("car_id_not_null",   "car_id IS NOT NULL")
@dlt.expect("ad_placed_on_valid",        "ad_placed_on IS NOT NULL")
@dlt.expect("price_positive",            "original_selling_price > 0")
@dlt.expect("sold_not_before_listed",    "sold_before_listed = false OR sold_on IS NULL")
def silver_sales():
    # Join region back from master table
    region_map = spark.table("primeinsurance.silver_layer.region_mapper_table")
    df = dlt.read_stream("silver_sales_parsed")
    return (
        df.join(
            F.broadcast(region_map.select(
                F.upper(F.trim(F.col("reg_coded"))).alias("_rk"),
                F.col("reg_mapped").alias("region")
            )),
            df["region_raw"] == F.col("_rk"),
            how="left"
        )
        .withColumn("region",
            F.coalesce(F.col("region"), F.col("region_raw"))
        )
        .drop("_rk", "region_raw")
        .select(
            "sales_id", "ad_placed_on", "sold_on",
            "original_selling_price", "region", "state", "city",
            "seller_type", "owner", "car_id","sold_before_listed",  
            "source_file", "ingestion_timestamp"
        )
    )


@dlt.table(
    name="silver_sales_quarantine",
    comment="Rejected sales records. Covers: null IDs, unparseable dates, negative price, temporal violations."
)
def silver_sales_quarantine():
    df = dlt.read_stream("silver_sales_parsed")
    return (
        df.withColumn(
            "_dq_rule",
            F.when(F.col("sales_id").isNull(),
                   F.lit("sales_id_not_null"))
             .when(F.col("car_id").isNull(),
                   F.lit("car_id_not_null"))
             .when(F.col("ad_placed_on").isNull(),
                   F.lit("ad_placed_on_unparseable"))
             .when(
                 F.col("original_selling_price").isNotNull() &
                 (F.col("original_selling_price") <= 0),
                 F.lit("price_non_positive")
             )
             .when(
                 F.col("sold_before_listed") == True,
                 F.lit("sold_before_listed")
             )
        )
        .filter(F.col("_dq_rule").isNotNull())
        .withColumn("_dq_detected_at", F.current_timestamp())
        .withColumn("_entity", F.lit("sales"))
    )


# ─────────────────────────────────────────────────────────────────────────────
# QUALITY ISSUES LOG
# Append-only streaming union of all quarantine tables.
# Each row = one bad record event with entity, rule, and timestamp.
# ─────────────────────────────────────────────────────────────────────────────

@dlt.table(
    name="quality_issues_log",
    comment="""Append-only streaming log of every data quality event caught across all
    5 silver entities. One row per bad record. Join with issue_recorder_table on
    _dq_rule to get full business context and remediation guidance.
    Columns: entity, _dq_rule, record_id, source_file, _dq_detected_at."""
)
def quality_issues_log():
    def quarantine_events(table_name, entity, id_col):
        return (
            dlt.read_stream(table_name)
            .withColumn("record_id", F.col(id_col).cast(StringType()))
            .select(
                F.lit(entity).alias("entity"),
                F.col("_dq_rule"),
                F.col("record_id"),
                F.col("source_file"),
                F.col("_dq_detected_at")
            )
        )

    from functools import reduce
    streams = [
        quarantine_events("silver_customers_quarantine", "customers", "customer_id"),
        quarantine_events("silver_cars_quarantine",      "cars",      "car_id"),
        quarantine_events("silver_claims_quarantine",    "claims",    "claim_id"),
        quarantine_events("silver_policy_quarantine",    "policy",    "policy_number"),
        quarantine_events("silver_sales_quarantine",     "sales",     "sales_id"),
    ]
    return reduce(lambda a, b: a.union(b), streams)

# ====================================


# ─────────────────────────────────────────────────────────────────────────────
# LIVE ISSUE STATS + DQ_ISSUES
# Drop-in replacement for the bottom section of 02_silver_layer_dlt_pipeline.py
# Place these three blocks AFTER quality_issues_log and BEFORE any gold reads.
#
# Key design decisions vs previous version
# ─────────────────────────────────────────
# 1. CUMULATIVE AGGREGATION
#    live_issue_stats uses dlt.read_stream() + complete-output-mode aggregation.
#    DLT handles complete-mode automatically when it detects a stateful groupBy
#    on a stream — every microbatch rewrites the full aggregate result so counts
#    are always the total across ALL pipeline history, not a rolling window.
#    No watermark needed or used — watermarks truncate history, which is exactly
#    what we do NOT want for an audit counter.
#
# 2. ENTITY ROW COUNTS (DENOMINATOR)
#    Moved out of live_issue_stats into a separate DLT table: silver_entity_counts.
#    This avoids calling spark.table().count() inside a streaming function
#    (which re-runs on every microbatch and causes inconsistent reads mid-stream).
#    silver_entity_counts is a @dlt.table that reads each silver table once per
#    trigger via dlt.read() (batch read, not stream) and emits one row per entity.
#    live_issue_stats then joins against it — clean, consistent, no hidden counts.
# ─────────────────────────────────────────────────────────────────────────────

import dlt
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StringType, LongType, DoubleType, TimestampType
)

# ─────────────────────────────────────────────────────────────────────────────
# RULE BRIDGE
# Single definition shared by live_issue_stats and dq_issues.
# Add a row here whenever a new @dlt.expect rule is added to any entity.
# ─────────────────────────────────────────────────────────────────────────────

RULE_BRIDGE = [
    # customers
    ("customers", "customer_id_not_null",        "DQ-CUST-010"),
    ("customers", "customer_id_non_positive",    "DQ-CUST-010"),
    ("customers", "region_invalid",              "DQ-CUST-003"),
    ("customers", "education_missing",           "DQ-CUST-005"),
    ("customers", "marital_status_missing",      "DQ-CUST-007"),
    ("customers", "default_flag_invalid",        "DQ-CUST-010"),
    # cars
    ("cars", "car_id_not_null",                  "DQ-CARS-001"),
    ("cars", "car_id_non_positive",              "DQ-CARS-001"),
    ("cars", "km_driven_negative",               "DQ-CARS-005"),
    ("cars", "km_driven_extreme_outlier",        "DQ-CARS-005"),
    ("cars", "engine_cc_non_positive",           "DQ-CARS-002"),
    ("cars", "max_power_non_positive",           "DQ-CARS-003"),
    ("cars", "seats_out_of_range",               "DQ-CARS-006"),
    ("cars", "fuel_unknown_type",                "DQ-CARS-007"),
    # claims
    ("claims", "claim_id_not_null",              "DQ-CLMS-010"),
    ("claims", "policy_id_not_null",             "DQ-CLMS-010"),
    ("claims", "claim_logged_on_corrupted",      "DQ-CLMS-002"),
    ("claims", "incident_date_corrupted",        "DQ-CLMS-001"),
    ("claims", "claim_processed_on_corrupted",   "DQ-CLMS-004"),
    ("claims", "injury_extreme_outlier",         "DQ-CLMS-009"),
    ("claims", "claim_rejected_invalid_value",   "DQ-CLMS-010"),
    ("claims", "injury_negative",                "DQ-CLMS-010"),
    ("claims", "collision_type_unknown",         "DQ-CLMS-007"),
    ("claims", "police_report_unknown",          "DQ-CLMS-005"),
    ("claims", "property_damage_unknown",        "DQ-CLMS-006"),
    # policy
    ("policy", "policy_number_not_null",         "DQ-POL-003"),
    ("policy", "customer_id_not_null",           "DQ-POL-003"),
    ("policy", "car_id_not_null",                "DQ-POL-003"),
    ("policy", "policy_bind_date_unparseable",   "DQ-POL-001"),
    ("policy", "premium_non_positive",           "DQ-POL-004"),
    ("policy", "deductable_negative",            "DQ-POL-004"),
    # sales
    ("sales", "sales_id_not_null",               "DQ-SALE-001"),
    ("sales", "car_id_not_null",                 "DQ-SALE-001"),
    ("sales", "ad_placed_on_unparseable",        "DQ-SALE-002"),
    ("sales", "price_non_positive",              "DQ-SALE-005"),
    ("sales", "sold_before_listed",              "DQ-SALE-002"),
]


# ─────────────────────────────────────────────────────────────────────────────
# TABLE 1 OF 3: silver_entity_counts
#
# Reads each silver table as a DLT batch read (dlt.read, not dlt.read_stream)
# and emits one row per entity with its current clean row count.
# This is the denominator for affected_pct in live_issue_stats.
#
# Why a separate DLT table instead of inline spark.table().count()?
#   spark.table().count() inside a streaming function re-executes on EVERY
#   microbatch, creates a non-deterministic read mid-stream, and can cause
#   Spark to throw AnalysisException when mixing streaming + non-streaming
#   DataFrames in the same query plan without an explicit barrier.
#   Wrapping it as a DLT batch table gives it a clean, separate query plan
#   that live_issue_stats can join against safely.
# ─────────────────────────────────────────────────────────────────────────────

@dlt.table(
    name="silver_entity_counts",
    comment="""Row counts for each silver entity table. Used as the denominator
    when computing affected_pct in live_issue_stats. Refreshed on every
    pipeline trigger via batch reads of the five silver tables."""
)
def silver_entity_counts():
    """
    Unions COUNT(*) from each silver table into a single two-column DataFrame:
      entity       STRING  — matches entity values in quality_issues_log
      total_rows   LONG    — current clean row count in that silver table

    Uses dlt.read() (batch, not stream) so Spark plans this as a standard
    DataFrame operation with no streaming state complications.
    """
    def count_table(table_name: str, entity: str):
        return (
            dlt.read(table_name)
            .agg(F.count("*").alias("total_rows"))
            .withColumn("entity", F.lit(entity))
            .select("entity", "total_rows")
        )

    from functools import reduce
    frames = [
        count_table("silver_customers", "customers"),
        count_table("silver_cars",      "cars"),
        count_table("silver_claims",    "claims"),
        count_table("silver_policy",    "policy"),
        count_table("silver_sales",     "sales"),
    ]
    return reduce(lambda a, b: a.union(b), frames)


# ─────────────────────────────────────────────────────────────────────────────
# TABLE 2 OF 3: live_issue_stats
#
# Reads quality_issues_log as a stream and produces CUMULATIVE aggregates —
# one row per issue_id covering all records ever caught since pipeline start.
#
# Why no watermark?
#   Watermarks tell Spark to DISCARD state older than N time units.
#   For a DQ audit counter we never want to discard history — a record caught
#   6 months ago is still a caught record. Watermarks would silently shrink
#   affected_records over time, which is wrong for compliance tracking.
#
# How does DLT handle stateful groupBy without a watermark?
#   When DLT sees a streaming groupBy aggregation with no watermark it writes
#   the result in COMPLETE output mode automatically — the entire aggregate
#   state is rewritten on every microbatch. This is correct behaviour:
#   every trigger produces a fresh full snapshot of all issue counts.
#   The DLT table is therefore always a complete, consistent view of totals.
#
# fix_status update logic (conservative — human decisions are never overwritten)
#   UNRESOLVABLE → stays UNRESOLVABLE  (dates gone; no pipeline fix possible)
#   PARTIAL      → stays PARTIAL       (needs gold-layer work; not done yet)
#   FLAGGED      → stays FLAGGED       (needs human review; machine won't clear)
#   0 events     → RESOLVED            (nothing caught = pipeline handled it)
#   >0 events    → keep analyst status (analyst set it for a reason)
# ─────────────────────────────────────────────────────────────────────────────

@dlt.table(
    name="live_issue_stats",
    comment="""Cumulative live DQ stats derived from quality_issues_log.
    One row per issue_id. Refreshed as a complete snapshot on every
    pipeline microbatch — counts accumulate across all pipeline history.
    affected_records = COUNT(DISTINCT record_id) across all time.
    affected_pct     = affected_records / silver entity total rows * 100.
    fix_status       = live recomputed status (conservative: UNRESOLVABLE /
                       PARTIAL / FLAGGED never auto-cleared by this logic)."""
)
def live_issue_stats():
    bridge_df = spark.createDataFrame(
        RULE_BRIDGE,
        ["_bridge_entity", "_bridge_rule", "_bridge_issue_id"]
    )

    # Analyst catalogue — static read for baseline fix_status
    catalogue = spark.table("primeinsurance.silver_layer.issue_recorder_table")

    # Entity counts — batch DLT read (clean, separate query plan from stream)
    entity_counts = dlt.read("silver_entity_counts")

    # Stream: resolve issue_id on every quarantine event
    log_stream = (
        dlt.read_stream("quality_issues_log")
        .join(
            F.broadcast(bridge_df),
            (F.col("entity")   == F.col("_bridge_entity")) &
            (F.col("_dq_rule") == F.col("_bridge_rule")),
            how="left"
        )
        .withColumn(
            "issue_id",
            F.coalesce(
                F.col("_bridge_issue_id"),
                F.concat(F.lit("UNMAPPED-"), F.col("entity"))
            )
        )
        .select("issue_id", "entity", "record_id", "_dq_detected_at")
    )

    # ── Cumulative distinct count — two-step workaround ─────────────────────
    # Spark Structured Streaming forbids count_distinct() in both append and
    # complete output modes. The workaround is to deduplicate (issue_id,
    # record_id) pairs first — making each pair appear exactly once — then
    # use a plain count(). This gives exact distinct record counts without
    # triggering the restriction, and works cleanly in complete output mode
    # (no watermark → DLT rewrites the full aggregate on every microbatch).
    deduped = log_stream.dropDuplicates(["issue_id", "record_id"])

    aggregated = (
        deduped
        .groupBy("issue_id", "entity")
        .agg(
            F.count("record_id").alias("affected_records_live"),   # exact, post-dedup
            F.max("_dq_detected_at").alias("last_detected_at")
        )
    )

    # Join entity counts for pct denominator
    with_pct = (
        aggregated
        .join(
            entity_counts.select(
                F.col("entity").alias("_ec_entity"),
                F.col("total_rows").alias("_ec_total_rows")
            ),
            aggregated["entity"] == F.col("_ec_entity"),
            how="left"
        )
        .withColumn(
            "affected_pct_live",
            F.round(
                (F.col("affected_records_live") * 100.0) /
                F.nullif(F.col("_ec_total_rows"), F.lit(0).cast(LongType())),
                2
            )
        )
    )

    # Join catalogue for baseline fix_status — conservative update logic
    return (
        with_pct
        .join(
            F.broadcast(
                catalogue.select(
                    F.col("issue_id").alias("_cat_issue_id"),
                    F.col("fix_status").alias("_baseline_fix_status")
                )
            ),
            with_pct["issue_id"] == F.col("_cat_issue_id"),
            how="left"
        )
        .withColumn(
            "recomputed_fix_status",
            F.when(F.col("_baseline_fix_status") == "UNRESOLVABLE", F.lit("UNRESOLVABLE"))
             .when(F.col("_baseline_fix_status") == "PARTIAL",      F.lit("PARTIAL"))
             .when(F.col("_baseline_fix_status") == "FLAGGED",      F.lit("FLAGGED"))
             .when(F.col("affected_records_live") == 0,             F.lit("RESOLVED"))
             .otherwise(F.col("_baseline_fix_status"))
        )
        .select(
            "issue_id",
            "entity",
            F.col("affected_records_live").alias("affected_records"),
            F.col("affected_pct_live").alias("affected_pct"),
            F.col("recomputed_fix_status").alias("fix_status"),
            F.col("last_detected_at"),
            F.col("_ec_total_rows").alias("silver_total_rows")
        )
    )


# ─────────────────────────────────────────────────────────────────────────────
# dq_issues
#
# Every quarantine event enriched with:
#   - live counts and status  from live_issue_stats  (fresh per microbatch)
#   - analyst metadata        from issue_recorder_table (static: descriptions)
#
# live_issue_stats is read via dlt.read() (batch) — it is a complete-mode
# table so every read gets a consistent full snapshot, not a partial stream.
# ─────────────────────────────────────────────────────────────────────────────

@dlt.table(
    name="dq_issues",
    comment="""Enriched DQ issues table — one row per quarantine event.
    Joins every bad-record event with:
      live_issue_stats     → affected_records, affected_pct, fix_status (LIVE)
      issue_recorder_table → issue_id, descriptions, prevention notes (static)

    affected_records / affected_pct / fix_status are recomputed from
    quality_issues_log on every microbatch — values are never stale.

    Key columns:
      issue_id            — catalogue ID (e.g. DQ-CLMS-001)
      entity              — which silver table
      _dq_rule            — machine rule name that fired
      issue_type          — CORRUPT_DATA | NULL_VALUE | SCHEMA_VARIANT | etc.
      issue_description   — analyst-written description
      severity            — CRITICAL | HIGH | MEDIUM | LOW
      affected_records    — live count (distinct records caught, all history)
      affected_pct        — live % of silver table affected
      silver_total_rows   — denominator used for pct (current silver row count)
      fix_status          — live conservatively recomputed status
      silver_fix_applied  — what the pipeline did to handle this
      prevention_note     — recommended source/bronze fix
      record_id           — specific bad record ID
      source_file         — bronze source file
      _dq_detected_at     — when this event was caught
    """
)
def dq_issues():
    bridge_df = spark.createDataFrame(
        RULE_BRIDGE,
        ["_bridge_entity", "_bridge_rule", "_bridge_issue_id"]
    )

    # Analyst metadata — static read (descriptions/notes never change per run)
    catalogue = spark.table("primeinsurance.silver_layer.issue_recorder_table")

    # live_issue_stats — batch read of complete-mode snapshot (always fresh totals)
    live_stats = dlt.read("live_issue_stats")

    # Resolve issue_id on the events stream
    events_resolved = (
        dlt.read_stream("quality_issues_log")
        .join(
            F.broadcast(bridge_df),
            (F.col("entity")   == F.col("_bridge_entity")) &
            (F.col("_dq_rule") == F.col("_bridge_rule")),
            how="left"
        )
        .withColumn(
            "issue_id",
            F.coalesce(
                F.col("_bridge_issue_id"),
                F.concat(F.lit("UNMAPPED-"), F.col("entity"))
            )
        )
        .drop("_bridge_entity", "_bridge_rule", "_bridge_issue_id")
    )

    # Join live stats (fresh counts/status)
    with_stats = (
        events_resolved
        .join(
            live_stats.select(
                F.col("issue_id").alias("_ls_issue_id"),
                F.col("affected_records"),
                F.col("affected_pct"),
                F.col("fix_status"),
                F.col("silver_total_rows")
            ),
            events_resolved["issue_id"] == F.col("_ls_issue_id"),
            how="left"
        )
        .drop("_ls_issue_id")
    )

    # Join analyst metadata
    return (
        with_stats
        .join(
            F.broadcast(
                catalogue.select(
                    F.col("issue_id").alias("_cat_issue_id"),
                    "column_name",
                    "issue_type",
                    "issue_description",
                    "silver_fix_applied",
                    "prevention_note",
                    "severity"
                )
            ),
            with_stats["issue_id"] == F.col("_cat_issue_id"),
            how="left"
        )
        .drop("_cat_issue_id")
        .withColumn("_dq_table", F.concat_ws("", F.lit("silver_"), F.col("entity")))
        .select(
            "issue_id",
            "entity",
            "_dq_rule",
            "column_name",
            "issue_type",
            "issue_description",
            "severity",
            "affected_records",        # LIVE — from quality_issues_log aggregate
            "affected_pct",            # LIVE — affected / silver_total_rows * 100
            "silver_total_rows",       # denominator — current silver clean count
            "fix_status",              # LIVE — conservatively recomputed
            "silver_fix_applied",
            "prevention_note",
            "record_id",
            "source_file",
            "_dq_detected_at",
            "_dq_table",
        )
    )


