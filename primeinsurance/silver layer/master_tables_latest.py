# Databricks notebook source
# MAGIC %sql
# MAGIC -- =============================================================================
# MAGIC -- FILE: 01_master_table_setup.sql
# MAGIC -- PURPOSE: Create and populate all master/lookup tables in silver_layer
# MAGIC --          Run this ONCE before the DLT pipeline is first triggered.
# MAGIC -- =============================================================================
# MAGIC
# MAGIC -- ---------------------------------------------------------------------------
# MAGIC -- 1. REGION MAPPER TABLE
# MAGIC -- ---------------------------------------------------------------------------
# MAGIC CREATE OR REPLACE TABLE primeinsurance.silver_layer.region_mapper_table (
# MAGIC     reg_coded            STRING  NOT NULL,
# MAGIC     reg_mapped           STRING  NOT NULL,
# MAGIC     md_created_datetime  TIMESTAMP
# MAGIC )
# MAGIC TBLPROPERTIES('delta.feature.allowColumnDefaults' = 'supported')
# MAGIC COMMENT "Master lookup: maps region abbreviations and full-names to canonical full names.
# MAGIC          Used by silver_customers and silver_sales transformations.";
# MAGIC
# MAGIC ALTER TABLE primeinsurance.silver_layer.region_mapper_table
# MAGIC     ALTER COLUMN md_created_datetime SET DEFAULT CURRENT_TIMESTAMP();
# MAGIC
# MAGIC INSERT INTO primeinsurance.silver_layer.region_mapper_table (reg_coded, reg_mapped, md_created_datetime)
# MAGIC VALUES
# MAGIC     ('W',       'West',    CURRENT_TIMESTAMP()),
# MAGIC     ('C',       'Central', CURRENT_TIMESTAMP()),
# MAGIC     ('E',       'East',    CURRENT_TIMESTAMP()),
# MAGIC     ('S',       'South',   CURRENT_TIMESTAMP()),
# MAGIC     ('West',    'West',    CURRENT_TIMESTAMP()),
# MAGIC     ('Central', 'Central', CURRENT_TIMESTAMP()),
# MAGIC     ('East',    'East',    CURRENT_TIMESTAMP()),
# MAGIC     ('South',   'South',   CURRENT_TIMESTAMP());
# MAGIC
# MAGIC
# MAGIC -- ---------------------------------------------------------------------------
# MAGIC -- 2. ISSUE RECORDER TABLE
# MAGIC --    Every known data quality issue discovered during Bronze analysis is
# MAGIC --    catalogued here. The DLT pipeline joins against this table to enrich
# MAGIC --    quarantine records with structured issue metadata.
# MAGIC --
# MAGIC --    Severity scale:
# MAGIC --      CRITICAL  – Blocks core analytics; records unidentifiable or dates lost
# MAGIC --      HIGH      – Material impact on reporting accuracy or FK integrity
# MAGIC --      MEDIUM    – Reduces data richness; requires investigation
# MAGIC --      LOW       – Cosmetic/formatting; minimal business impact
# MAGIC -- ---------------------------------------------------------------------------
# MAGIC CREATE OR REPLACE TABLE primeinsurance.silver_layer.issue_recorder_table (
# MAGIC     issue_id            STRING  NOT NULL,
# MAGIC     entity              STRING  NOT NULL,   -- customers | cars | claims | policy | sales
# MAGIC     column_name         STRING,             -- affected column(s); NULL if structural
# MAGIC     issue_type          STRING  NOT NULL,   -- SCHEMA_VARIANT | NULL_VALUE | CORRUPT_DATA |
# MAGIC                                             -- TYPE_MISMATCH | OUTLIER | DUPLICATE |
# MAGIC                                             -- FORMAT_ISSUE | MISSING_DATA | FK_VIOLATION |
# MAGIC                                             -- COVERAGE_GAP | NORMALISATION
# MAGIC     issue_description   STRING  NOT NULL,
# MAGIC     silver_fix_applied  STRING  NOT NULL,   -- What the pipeline does to resolve it
# MAGIC     fix_status          STRING  NOT NULL,   -- RESOLVED | PARTIAL | UNRESOLVABLE | FLAGGED
# MAGIC     severity            STRING  NOT NULL,
# MAGIC     affected_records    BIGINT,             -- Known count from bronze analysis
# MAGIC     affected_pct        DOUBLE,             -- % of entity rows affected
# MAGIC     prevention_note     STRING,             -- Recommended source/bronze fix
# MAGIC     md_created_datetime TIMESTAMP
# MAGIC )
# MAGIC TBLPROPERTIES('delta.feature.allowColumnDefaults' = 'supported')
# MAGIC COMMENT "Catalogue of all known Bronze data quality issues with silver remediation status.
# MAGIC          Join with quality_issues_log or quarantine tables using issue_id for full context.";
# MAGIC
# MAGIC ALTER TABLE primeinsurance.silver_layer.issue_recorder_table
# MAGIC     ALTER COLUMN md_created_datetime SET DEFAULT CURRENT_TIMESTAMP();
# MAGIC
# MAGIC
# MAGIC -- ---------------------------------------------------------------------------
# MAGIC -- 3. POPULATE issue_recorder_table — all 38 issues found across 5 entities
# MAGIC -- ---------------------------------------------------------------------------
# MAGIC
# MAGIC INSERT INTO primeinsurance.silver_layer.issue_recorder_table
# MAGIC (issue_id, entity, column_name, issue_type, issue_description, silver_fix_applied, fix_status, severity, affected_records, affected_pct, prevention_note, md_created_datetime)
# MAGIC VALUES
# MAGIC
# MAGIC -- ── CUSTOMERS (11 issues) ──────────────────────────────────────────────────
# MAGIC ('DQ-CUST-001', 'customers', 'CustomerID',
# MAGIC  'DUPLICATE',
# MAGIC  'Duplicate CustomerID rows caused by re-ingested master file that overlaps with customers_1/3/4/5/6/7.',
# MAGIC  'Deduplication can be applied using customer_id — only the first occurrence per customer_id is retained in silver_customers.',
# MAGIC  'RESOLVED', 'CRITICAL', 1602, 44.4,
# MAGIC  'Re-check for the data deduplication whether necessary or not',
# MAGIC  CURRENT_TIMESTAMP()),
# MAGIC
# MAGIC ('DQ-CUST-002', 'customers', 'CustomerID, Customer_ID, cust_id',
# MAGIC  'SCHEMA_VARIANT',
# MAGIC  'Customer primary key split across three column variants in different source files: CustomerID (files 1,3,4,5,6,7), Customer_ID (file 2), cust_id (files 2). Rows that have null CustomerID resolved only via alternate columns.',
# MAGIC  'Unified to single customer_id column using COALESCE(CustomerID, Customer_ID, cust_id) cast to LONG.',
# MAGIC  'RESOLVED', 'HIGH', 399, 11.1,
# MAGIC  'Standardise primary key column name to customer_id in source system exports.',
# MAGIC  CURRENT_TIMESTAMP()),
# MAGIC
# MAGIC ('DQ-CUST-003', 'customers', 'Region',
# MAGIC  'NORMALISATION',
# MAGIC  'Region column in customers_2.csv uses single-letter abbreviations (W, C, E, S) while all other files use full names (West, Central, East, South).',
# MAGIC  'Expanded via region_mapper_table lookup: W→West, C→Central, E→East, S→South.',
# MAGIC  'RESOLVED', 'MEDIUM', 200, 5.5,
# MAGIC  'Source system should export full region names consistently. Validate against allowed list at export.',
# MAGIC  CURRENT_TIMESTAMP()),
# MAGIC
# MAGIC ('DQ-CUST-004', 'customers', 'Reg, Region',
# MAGIC  'SCHEMA_VARIANT',
# MAGIC  'Region stored in two different column names: Reg (files 1,3,4,5,6,7) and Region (files 2,4,7). Some files populate both, some only one.',
# MAGIC  'Unified via COALESCE(Reg, Region) after NULL-string filtering, then mapped to canonical region.',
# MAGIC  'RESOLVED', 'MEDIUM', 3605, 100.0,
# MAGIC  'Standardise column name to region across all source file exports.',
# MAGIC  CURRENT_TIMESTAMP()),
# MAGIC
# MAGIC ('DQ-CUST-005', 'customers', 'Education, Edu',
# MAGIC  'MISSING_DATA',
# MAGIC  '1005 rows from customers_4.csv have no education data — neither Education nor Edu column is populated. This is 27.9% of the customer base.',
# MAGIC  'Retained as NULL in silver. Flagged in quarantine with rule education_missing_customers4.',
# MAGIC  'FLAGGED', 'HIGH', 1005, 27.9,
# MAGIC  'Investigate customers_4.csv source: education field must be captured at customer onboarding. Add NOT NULL constraint at source.',
# MAGIC  CURRENT_TIMESTAMP()),
# MAGIC
# MAGIC ('DQ-CUST-006', 'customers', 'Education, Marital_status',
# MAGIC  'SCHEMA_VARIANT',
# MAGIC  'In customers_2.csv the Education and Marital_status columns are positionally swapped relative to all other files. Education values are in Edu column; Marital values are in Marital column.',
# MAGIC  'Detected by presence of non-null Edu/Marital columns as file-2 signal. Values remapped to correct unified columns.',
# MAGIC  'RESOLVED', 'HIGH', 200, 5.5,
# MAGIC  'Enforce fixed column order in source CSV export template. Use header validation at ingestion.',
# MAGIC  CURRENT_TIMESTAMP()),
# MAGIC
# MAGIC ('DQ-CUST-007', 'customers', 'Marital_status, Marital',
# MAGIC  'SCHEMA_VARIANT',
# MAGIC  'Marital status stored in two column variants: Marital_status (files 1,3,4,5,6,7) and Marital (files 2,7). 200 rows have null Marital_status resolved only via Marital column.',
# MAGIC  'Unified to marital_status using COALESCE(Marital, Marital_status).',
# MAGIC  'RESOLVED', 'MEDIUM', 200, 5.5,
# MAGIC  'Standardise column name to marital_status in all source exports.',
# MAGIC  CURRENT_TIMESTAMP()),
# MAGIC
# MAGIC ('DQ-CUST-008', 'customers', 'City, City_in_state',
# MAGIC  'SCHEMA_VARIANT',
# MAGIC  '200 rows in customers_2.csv use City_in_state instead of City as the city column name, with different values in 200 cases.',
# MAGIC  'City_in_state coalesced into city column. City_in_state dropped from silver output.',
# MAGIC  'RESOLVED', 'LOW', 200, 5.5,
# MAGIC  'Standardise city column name across all source exports.',
# MAGIC  CURRENT_TIMESTAMP()),
# MAGIC
# MAGIC ('DQ-CUST-009', 'customers', 'region',
# MAGIC  'COVERAGE_GAP',
# MAGIC  'No customers exist for North region. All 3605 rows belong to West, Central, East, or South. This may indicate a business coverage gap or a missing data feed.',
# MAGIC  'No fix applied. Coverage gap documented and visible in silver data.',
# MAGIC  'FLAGGED', 'LOW', 0, 0.0,
# MAGIC  'Confirm with business whether North region is intentionally absent or if a source feed is missing.',
# MAGIC  CURRENT_TIMESTAMP()),
# MAGIC
# MAGIC ('DQ-CUST-010', 'customers', 'CustomerID',
# MAGIC  'NULL_VALUE',
# MAGIC  'CustomerID is NULL in 399 rows. Of these, 200 have Customer_ID populated and 199 have cust_id populated — all are recoverable.',
# MAGIC  'customer_id resolved via COALESCE fallback across all three ID variants. Zero unresolvable null IDs remain after fix.',
# MAGIC  'RESOLVED', 'CRITICAL', 399, 11.1,
# MAGIC  'Primary key column should never be null at source. Enforce NOT NULL at CRM/source system level.',
# MAGIC  CURRENT_TIMESTAMP()),
# MAGIC
# MAGIC ('DQ-CUST-011', 'customers', 'source_file',
# MAGIC  'DUPLICATE',
# MAGIC  'customers_7.csv (1604 rows) appears to be a master snapshot that re-includes all records from customers_1/3/4/5. It overlaps with 1204 IDs from other files, causing the 1602-row duplicate count.',
# MAGIC  'Window-function deduplication retains first occurrence per customer_id ordered by source_file ascending.',
# MAGIC  'RESOLVED', 'CRITICAL', 1602, 44.4,
# MAGIC  'Do not ingest aggregate/master snapshot files alongside incremental files. Use append-only incremental exports.',
# MAGIC  CURRENT_TIMESTAMP()),
# MAGIC
# MAGIC -- ── CARS (8 issues) ────────────────────────────────────────────────────────
# MAGIC ('DQ-CARS-001', 'cars', 'mileage',
# MAGIC  'FORMAT_ISSUE',
# MAGIC  'Mileage field contains unit strings mixed with numeric value: 2476 rows use "kmpl" and 24 rows use "km/kg" (CNG vehicles). Cannot perform numeric operations without parsing.',
# MAGIC  'Numeric value extracted via regexp_extract. Unit preserved separately in mileage_unit column. mileage_value cast to DECIMAL(6,2).',
# MAGIC  'RESOLVED', 'HIGH', 2500, 100.0,
# MAGIC  'Export numeric value and unit as separate columns from source system.',
# MAGIC  CURRENT_TIMESTAMP()),
# MAGIC
# MAGIC ('DQ-CARS-002', 'cars', 'engine',
# MAGIC  'FORMAT_ISSUE',
# MAGIC  'Engine displacement stored as string with "CC" suffix (e.g. "1248 CC") across all 2500 rows. Prevents numeric operations.',
# MAGIC  'CC suffix stripped via regexp_extract. Value cast to INT as engine_cc column.',
# MAGIC  'RESOLVED', 'HIGH', 2500, 100.0,
# MAGIC  'Store engine displacement as integer in source system. Export unit in separate column or column name.',
# MAGIC  CURRENT_TIMESTAMP()),
# MAGIC
# MAGIC ('DQ-CARS-003', 'cars', 'max_power',
# MAGIC  'FORMAT_ISSUE',
# MAGIC  'Max power stored as string with "bhp" suffix (e.g. "74 bhp") across all 2500 rows. Prevents numeric operations.',
# MAGIC  'bhp suffix stripped via regexp_extract. Value cast to DECIMAL(7,2) as max_power_bhp column.',
# MAGIC  'RESOLVED', 'HIGH', 2500, 100.0,
# MAGIC  'Store power as numeric in source system. Separate unit into column name or dedicated unit column.',
# MAGIC  CURRENT_TIMESTAMP()),
# MAGIC
# MAGIC ('DQ-CARS-004', 'cars', 'torque',
# MAGIC  'FORMAT_ISSUE',
# MAGIC  'Torque field has 4 inconsistent format variants across 2500 rows: "190Nm@ 2000rpm" (2399 rows), "135 Nm at 2500 rpm" (67 rows), "11.4 kgm at 4,000 rpm" (28 rows), "14.9 KGM at 3000 RPM" (6 rows). Also mixed units (Nm vs kgm) and mixed case.',
# MAGIC  'Torque retained as trimmed string (too varied to safely parse to single numeric). Flagged for downstream gold-layer parsing with dedicated regex per format variant.',
# MAGIC  'PARTIAL', 'MEDIUM', 2500, 100.0,
# MAGIC  'Standardise torque format at source to single unit (Nm) with consistent "NNNNm@ RPMrpm" notation.',
# MAGIC  CURRENT_TIMESTAMP()),
# MAGIC
# MAGIC ('DQ-CARS-005', 'cars', 'km_driven',
# MAGIC  'OUTLIER',
# MAGIC  'car_id 125287 (Mahindra XUV500 W6 2WD) has km_driven=1,500,000 — 21.7x the average of 69,001 km. Likely a data entry error (extra zero).',
# MAGIC  'Flagged in silver_cars_quarantine with rule km_driven_extreme_outlier. Record retained in silver with warning annotation.',
# MAGIC  'FLAGGED', 'HIGH', 1, 0.04,
# MAGIC  'Implement range check at source: flag km_driven > 500,000 for manual review before export.',
# MAGIC  CURRENT_TIMESTAMP()),
# MAGIC
# MAGIC ('DQ-CARS-006', 'cars', 'seats',
# MAGIC  'OUTLIER',
# MAGIC  '4 vehicles have seats=10, which is above the standard passenger car range of 2-9. May represent minibuses or data entry errors.',
# MAGIC  'Flagged in silver_cars_quarantine with rule seats_out_of_range. Records quarantined pending business review.',
# MAGIC  'FLAGGED', 'LOW', 4, 0.16,
# MAGIC  'Define and enforce seats value range at source. If >9-seat vehicles are valid, update the allowed range in DQ rules.',
# MAGIC  CURRENT_TIMESTAMP()),
# MAGIC
# MAGIC ('DQ-CARS-007', 'cars', 'fuel',
# MAGIC  'NORMALISATION',
# MAGIC  'Fuel type values use mixed case (Petrol, Diesel, CNG, LPG) rather than consistent UPPER CASE.',
# MAGIC  'Normalised to UPPER CASE via F.upper(F.trim()) in silver transformation.',
# MAGIC  'RESOLVED', 'LOW', 2500, 100.0,
# MAGIC  'Source system should store fuel type as controlled vocabulary in UPPER CASE.',
# MAGIC  CURRENT_TIMESTAMP()),
# MAGIC
# MAGIC ('DQ-CARS-008', 'cars', 'transmission',
# MAGIC  'NORMALISATION',
# MAGIC  'Transmission values use mixed case (Manual, Automatic) rather than consistent UPPER CASE.',
# MAGIC  'Normalised to UPPER CASE via F.upper(F.trim()) in silver transformation.',
# MAGIC  'RESOLVED', 'LOW', 2500, 100.0,
# MAGIC  'Source system should store transmission as controlled vocabulary in UPPER CASE.',
# MAGIC  CURRENT_TIMESTAMP()),
# MAGIC
# MAGIC -- ── CLAIMS (10 issues) ─────────────────────────────────────────────────────
# MAGIC ('DQ-CLMS-001', 'claims', 'incident_date',
# MAGIC  'CORRUPT_DATA',
# MAGIC  'ALL 1000 incident_date values are corrupted: stored as "MM:SS.0" format (e.g. "27:00.0") instead of actual dates. Original dates are unrecoverable. Root cause: likely Excel date serial number converted to time fraction during CSV export.',
# MAGIC  'Raw value preserved in _raw_incident_date for audit. incident_date set to NULL. incident_date_corrupted flag set to TRUE for all rows. All 1000 records quarantined.',
# MAGIC  'UNRESOLVABLE', 'CRITICAL', 1000, 100.0,
# MAGIC  'URGENT: Investigate Excel/CSV export process. Dates must be formatted as YYYY-MM-DD before export. Re-export source data with corrected dates.',
# MAGIC  CURRENT_TIMESTAMP()),
# MAGIC
# MAGIC ('DQ-CLMS-002', 'claims', 'Claim_Logged_On',
# MAGIC  'CORRUPT_DATA',
# MAGIC  '989 of 1000 Claim_Logged_On values are corrupted with same MM:SS.0 format. 11 rows have NULL. Zero rows have a parseable date.',
# MAGIC  'Raw value preserved in _raw_logged_on. claim_logged_on set to NULL. claim_logged_on_corrupted flag set. All affected records quarantined.',
# MAGIC  'UNRESOLVABLE', 'CRITICAL', 989, 98.9,
# MAGIC  'URGENT: Same root cause as incident_date. Re-export with proper TIMESTAMP format from source.',
# MAGIC  CURRENT_TIMESTAMP()),
# MAGIC
# MAGIC ('DQ-CLMS-003', 'claims', 'Claim_Processed_On',
# MAGIC  'NULL_VALUE',
# MAGIC  '526 Claim_Processed_On values are the literal string "NULL" instead of proper SQL NULL. Pattern: ALL rejected claims (Claim_Rejected=Y) have "NULL" here — this is intentional but incorrectly encoded.',
# MAGIC  'String "NULL" converted to SQL NULL via null_str() function. claim_processed_on set to NULL for rejected claims. Business meaning preserved.',
# MAGIC  'RESOLVED', 'HIGH', 526, 52.6,
# MAGIC  'Source system should write NULL (or omit the field) for rejected claims, not the literal string NULL.',
# MAGIC  CURRENT_TIMESTAMP()),
# MAGIC
# MAGIC ('DQ-CLMS-004', 'claims', 'Claim_Processed_On',
# MAGIC  'CORRUPT_DATA',
# MAGIC  '474 Claim_Processed_On values are corrupted MM:SS.0 format (the non-NULL approved claim records). Unparseable.',
# MAGIC  'Raw value preserved in _raw_processed_on. claim_processed_on set to NULL. claim_processed_on_corrupted flag set.',
# MAGIC  'UNRESOLVABLE', 'CRITICAL', 474, 47.4,
# MAGIC  'URGENT: Same root cause as other date fields. Fix at source export process.',
# MAGIC  CURRENT_TIMESTAMP()),
# MAGIC
# MAGIC ('DQ-CLMS-005', 'claims', 'police_report_available',
# MAGIC  'MISSING_DATA',
# MAGIC  '343 records (34.3%) have "?" as the value for police_report_available, indicating unknown/not collected data.',
# MAGIC  'Replaced with SQL NULL via null_str() function. Flagged in quarantine.',
# MAGIC  'RESOLVED', 'HIGH', 343, 34.3,
# MAGIC  'Claims adjusters should always record police report availability. Add mandatory field at claims intake.',
# MAGIC  CURRENT_TIMESTAMP()),
# MAGIC
# MAGIC ('DQ-CLMS-006', 'claims', 'property_damage',
# MAGIC  'MISSING_DATA',
# MAGIC  '360 records (36.0%) have "?" as the value for property_damage, indicating unknown/not collected data.',
# MAGIC  'Replaced with SQL NULL via null_str() function. Flagged in quarantine.',
# MAGIC  'RESOLVED', 'HIGH', 360, 36.0,
# MAGIC  'Property damage assessment must be captured at claims intake. Add mandatory field with YES/NO/UNKNOWN options.',
# MAGIC  CURRENT_TIMESTAMP()),
# MAGIC
# MAGIC ('DQ-CLMS-007', 'claims', 'collision_type',
# MAGIC  'MISSING_DATA',
# MAGIC  '178 records (17.8%) have "?" for collision_type. Valid values are Rear Collision, Side Collision, Front Collision.',
# MAGIC  'Replaced with SQL NULL via null_str(). Flagged in quarantine.',
# MAGIC  'RESOLVED', 'MEDIUM', 178, 17.8,
# MAGIC  'Collision type should be mandatory at claims registration. Implement dropdown with controlled vocabulary.',
# MAGIC  CURRENT_TIMESTAMP()),
# MAGIC
# MAGIC ('DQ-CLMS-008', 'claims', 'vehicle',
# MAGIC  'NULL_VALUE',
# MAGIC  '29 vehicle damage amount records contain the literal string "NULL" instead of proper SQL NULL.',
# MAGIC  'String "NULL" converted to SQL NULL via null_str() function.',
# MAGIC  'RESOLVED', 'MEDIUM', 29, 2.9,
# MAGIC  'Source system should write NULL for missing amounts, not the string NULL.',
# MAGIC  CURRENT_TIMESTAMP()),
# MAGIC
# MAGIC ('DQ-CLMS-009', 'claims', 'injury',
# MAGIC  'OUTLIER',
# MAGIC  'ClaimID 17304533 has injury amount of $100,000 — 13.9x the average of $6,652. Next highest is $60,000. Context: Single Vehicle Collision, Major Damage, Rejected claim.',
# MAGIC  'Flagged in silver_claims_quarantine with rule injury_extreme_outlier. Record retained in silver with warning.',
# MAGIC  'FLAGGED', 'MEDIUM', 1, 0.1,
# MAGIC  'Implement upper-bound validation on injury amounts at claims intake. Flag values >3x standard deviation for manual review.',
# MAGIC  CURRENT_TIMESTAMP()),
# MAGIC
# MAGIC ('DQ-CLMS-010', 'claims', 'injury, property, vehicle',
# MAGIC  'TYPE_MISMATCH',
# MAGIC  'All three financial fields stored as STRING in bronze, requiring explicit DECIMAL cast in silver.',
# MAGIC  'Cast to DECIMAL(12,2) in silver_claims_parsed. Null strings handled before cast.',
# MAGIC  'RESOLVED', 'HIGH', 1000, 100.0,
# MAGIC  'Define explicit schema with DECIMAL types for financial fields in bronze Auto Loader configuration.',
# MAGIC  CURRENT_TIMESTAMP()),
# MAGIC
# MAGIC -- ── POLICY (4 issues) ──────────────────────────────────────────────────────
# MAGIC ('DQ-POL-001', 'policy', 'policy_state',
# MAGIC  'NORMALISATION',
# MAGIC  'policy_state values have inconsistent whitespace and mixed case (e.g. " IL ", "il"). Requires normalisation for consistent state code lookup.',
# MAGIC  'Applied UPPER(TRIM()) in silver_policy_parsed. All state codes now consistent 2-char uppercase.',
# MAGIC  'RESOLVED', 'LOW', NULL, NULL,
# MAGIC  'Source system should enforce UPPER CASE and trim whitespace on state code fields.',
# MAGIC  CURRENT_TIMESTAMP()),
# MAGIC
# MAGIC ('DQ-POL-002', 'policy', 'policy_csl',
# MAGIC  'FORMAT_ISSUE',
# MAGIC  'Coverage split limit (policy_csl) field contains leading/trailing whitespace in some rows.',
# MAGIC  'Applied TRIM() in silver_policy_parsed.',
# MAGIC  'RESOLVED', 'LOW', NULL, NULL,
# MAGIC  'Trim whitespace at source export. Validate CSL codes against allowed list.',
# MAGIC  CURRENT_TIMESTAMP()),
# MAGIC
# MAGIC ('DQ-POL-003', 'policy', 'policy_number, car_id, customer_id, umbrella_limit',
# MAGIC  'TYPE_MISMATCH',
# MAGIC  'ID and limit fields inferred as INT by Auto Loader, risking integer overflow for large values. Should be LONG/BIGINT.',
# MAGIC  'Cast to LongType() in silver_policy_parsed.',
# MAGIC  'RESOLVED', 'MEDIUM', 1000, 100.0,
# MAGIC  'Define explicit bronze schema with BIGINT for all ID and large-integer fields.',
# MAGIC  CURRENT_TIMESTAMP()),
# MAGIC
# MAGIC ('DQ-POL-004', 'policy', 'policy_annual_premium',
# MAGIC  'TYPE_MISMATCH',
# MAGIC  'Premium stored as DOUBLE in bronze, losing decimal precision for currency values.',
# MAGIC  'Cast to DECIMAL(10,2) in silver_policy_parsed.',
# MAGIC  'RESOLVED', 'MEDIUM', 1000, 100.0,
# MAGIC  'Define explicit bronze schema with DECIMAL(10,2) for all currency fields.',
# MAGIC  CURRENT_TIMESTAMP()),
# MAGIC
# MAGIC -- ── SALES (5 issues) ───────────────────────────────────────────────────────
# MAGIC ('DQ-SALE-001', 'sales', 'sales_id',
# MAGIC  'CORRUPT_DATA',
# MAGIC  '1878 fully-null ghost rows injected into bronze_sales by CSV parser malfunction in source files. All columns NULL. These are not real records.',
# MAGIC  'Filtered out at start of silver_sales_parsed using filter(sales_id IS NOT NULL).',
# MAGIC  'RESOLVED', 'CRITICAL', 1878, 60.5,
# MAGIC  'Investigate CSV export process that generates ghost rows. Validate row count at ingestion. Implement NOT NULL check on sales_id at bronze layer.',
# MAGIC  CURRENT_TIMESTAMP()),
# MAGIC
# MAGIC ('DQ-SALE-002', 'sales', 'ad_placed_on, sold_on',
# MAGIC  'FORMAT_ISSUE',
# MAGIC  'Date fields use non-ISO DD-MM-YYYY HH:mm format (e.g. "10-02-2017 20:22") instead of standard YYYY-MM-DD HH:mm:ss. Risk of day/month swap parsing errors.',
# MAGIC  'Parsed explicitly using format pattern dd-MM-yyyy HH:mm via to_timestamp() in silver_sales_parsed.',
# MAGIC  'RESOLVED', 'HIGH', 1225, 100.0,
# MAGIC  'Source system should export timestamps in ISO 8601 format. Enforce at ETL/export layer.',
# MAGIC  CURRENT_TIMESTAMP()),
# MAGIC
# MAGIC ('DQ-SALE-003', 'sales', 'sold_on',
# MAGIC  'MISSING_DATA',
# MAGIC  '116 valid sales records have NULL sold_on. This is valid business data — listings that have not yet been sold should have no sale date.',
# MAGIC  'NULL values retained as-is. No quarantine action. Documented as intentional.',
# MAGIC  'RESOLVED', 'LOW', 116, 9.5,
# MAGIC  'No fix needed. NULL sold_on = unsold listing is valid business logic. Ensure downstream queries handle NULL correctly.',
# MAGIC  CURRENT_TIMESTAMP()),
# MAGIC
# MAGIC ('DQ-SALE-004', 'sales', 'Region, State, City',
# MAGIC  'NORMALISATION',
# MAGIC  'Region, State, City columns use PascalCase in sales files, inconsistent with snake_case convention used across other entities.',
# MAGIC  'Renamed to lowercase region, state, city in silver_sales_parsed select clause.',
# MAGIC  'RESOLVED', 'LOW', 1225, 100.0,
# MAGIC  'Enforce lowercase column naming convention in source CSV exports.',
# MAGIC  CURRENT_TIMESTAMP()),
# MAGIC
# MAGIC ('DQ-SALE-005', 'sales', 'sales_id, car_id, original_selling_price',
# MAGIC  'TYPE_MISMATCH',
# MAGIC  'sales_id and car_id inferred as INT by Auto Loader, risking overflow. original_selling_price inferred as INT losing decimal precision.',
# MAGIC  'sales_id and car_id cast to LongType(). original_selling_price cast to DECIMAL(15,2).',
# MAGIC  'RESOLVED', 'MEDIUM', 1225, 100.0,
# MAGIC  'Define explicit bronze schema with BIGINT for IDs and DECIMAL for prices.',
# MAGIC  CURRENT_TIMESTAMP());