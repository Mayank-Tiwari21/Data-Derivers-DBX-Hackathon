# Databricks notebook source
# DBTITLE 1,deduplicated customers
# MAGIC %sql
# MAGIC create materialized view primeinsurance.gold_layer.GOLD_deduplicated_customers as 
# MAGIC SELECT customer_id, COUNT(DISTINCT region) AS region_count
# MAGIC FROM primeinsurance.gold_layer.dim_customer
# MAGIC GROUP BY customer_id
# MAGIC HAVING COUNT(DISTINCT region) > 1;

# COMMAND ----------

# DBTITLE 1,Claim Rejection Rate by Region & Policy Type
# MAGIC %sql
# MAGIC create materialized view primeinsurance.gold_layer.GOLD_claim_rejection_rate as
# MAGIC SELECT
# MAGIC  dc.region,
# MAGIC  fp.policy_csl,
# MAGIC  SUM(fc.claim_rejected) / COUNT(*) AS rejection_rate
# MAGIC FROM primeinsurance.gold_layer.dim_policy fp
# MAGIC Join primeinsurance.gold_layer.fact_claims fc on fc.claim_id = fp.policy_number
# MAGIC Join primeinsurance.gold_layer.dim_customer dc on dc.customer_id = fp.customer_id
# MAGIC GROUP BY dc.region, fp.policy_csl;
# MAGIC
# MAGIC

# COMMAND ----------

# DBTITLE 1,Unsold Cars by Model & Region
# MAGIC %sql
# MAGIC create materialized view primeinsurance.gold_layer.GOLD_vw_unsold_cars_by_model_reg as 
# MAGIC SELECT
# MAGIC  dc.model,
# MAGIC  fs.region,
# MAGIC  COUNT(*) AS unsold_count
# MAGIC FROM primeinsurance.gold_layer.fact_sales fs
# MAGIC JOIN primeinsurance.gold_layer.dim_car dc ON fs.car_id = dc.car_id
# MAGIC WHERE fs.sold_on IS NULL
# MAGIC GROUP BY dc.model, fs.region;
# MAGIC

# COMMAND ----------

# DBTITLE 1,aging bucket
# MAGIC %sql
# MAGIC create materialized view primeinsurance.gold_layer.GOLD_vw_aging_bucket as 
# MAGIC SELECT
# MAGIC  region,
# MAGIC  CASE 
# MAGIC    WHEN (ad_placed_on - sold_on)  <= 30 THEN '0-30'
# MAGIC    WHEN (ad_placed_on - sold_on)   <= 60 THEN '30-60'
# MAGIC    ELSE '60+'
# MAGIC  END AS aging_bucket,
# MAGIC  COUNT(*)
# MAGIC FROM primeinsurance.gold_layer.fact_sales
# MAGIC WHERE sold_on IS NULL
# MAGIC GROUP BY region, aging_bucket;
# MAGIC