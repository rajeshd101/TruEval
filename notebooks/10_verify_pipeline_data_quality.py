# Author: Rajesh Daggupati
# Databricks notebook source
"""
TruEval Validation: Pipeline Data Quality Checks
This notebook performs a comprehensive audit across all layers of the TruEval pipeline
(Bronze, Silver, Gold) to ensure data integrity and structural correctness.

Key Features:
- Cross-Layer Reconciliation: Verifies row counts match between Bronze and Silver.
- Schema Integrity: Checks for null primary keys and composite key uniqueness.
- Range Validation: Ensures success metrics (match_rate) are within [0, 1] bounds.
- Business Logic Audit: Confirms KPI tables are populated and match source data.

Execution Instructions:
1. Run this as the final step of the Databricks Workflow.
2. If any check fails, investigate the corresponding layer before trusting the dashboard.
"""

# ── Step 1: Configuration ──
from pyspark.sql import functions as F

CATALOG = "bootcamp_students"
checks_results = []

# ── Step 2: Bronze Layer Audit ──
print("🕵️ Auditing Bronze Layer...")
bronze_data_df = spark.table(f"{CATALOG}.trueval_bronze.toucan_raw")
total_bronze_rows = bronze_data_df.count()

# Check: No nulls in primary key (uuid)
null_uuid_count = bronze_data_df.filter(F.col("uuid").isNull()).count()
checks_results.append(("bronze_pk_null_check", null_uuid_count == 0, null_uuid_count))

# Check: Unique composite key (uuid + derived model_name)
# In Toucan, the same UUID exists for different models, so we check uniqueness at that level
distinct_composite_keys = bronze_data_df.withColumn("model_name",
    F.when(F.col("source").like("%kimi%"), "Kimi-K2")
     .when(F.col("source").like("%qwen%"), "Qwen3")
     .otherwise("SFT")
).select("uuid", "model_name").distinct().count()
checks_results.append(("bronze_composite_uniqueness", total_bronze_rows == distinct_composite_keys, total_bronze_rows - distinct_composite_keys))

# ── Step 3: Silver Layer Audit ──
print("🕵️ Auditing Silver Layer...")
silver_data_df = spark.table(f"{CATALOG}.trueval_silver.toucan_trace")
total_silver_rows = silver_data_df.count()

# Check: Row count reconciliation (Silver must match Bronze)
checks_results.append(("silver_bronze_reconciliation", total_silver_rows == total_bronze_rows, abs(total_silver_rows - total_bronze_rows)))

# Check: Success metric range validation (match_rate)
invalid_match_rate_count = silver_data_df.filter((F.col("match_rate") < 0) | (F.col("match_rate") > 1)).count()
checks_results.append(("silver_metric_range_check", invalid_match_rate_count == 0, invalid_match_rate_count))

# ── Step 4: Gold Layer Audit ──
print("🕵️ Auditing Gold Layer...")
gold_kpis_df = spark.table(f"{CATALOG}.trueval_gold.trace_kpis")

# Check: KPI count matching (Summary 'traces_ingested' should match total Silver count)
sample_kpi_row = gold_kpis_df.first()
kpi_ingested_count = sample_kpi_row["traces_ingested"] if sample_kpi_row else 0
checks_results.append(("gold_silver_reconciliation", kpi_ingested_count == total_silver_rows, abs(kpi_ingested_count - total_silver_rows)))

# ── Step 5: Final Quality Report ──
print("\n" + "="*70)
print("              TRUEVAL DATA QUALITY REPORT")
print("="*70)

all_checks_passed = True
for check_name, passed, violation_count in checks_results:
    status_icon = "✅ PASS" if passed else "❌ FAIL"
    if not passed: all_checks_passed = False
    print(f" {status_icon} | {check_name:30} | Violations: {violation_count}")

print("="*70)
if all_checks_passed:
    print("🌟 ALL PIPELINE CHECKS PASSED. Data is healthy and reliable.")
else:
    print("🚨 DATA QUALITY ISSUES DETECTED. Review violations before analysis.")
print("="*70)

# ── Step 6: Detailed Debugging View ──
# Group by subset to see exactly where any missing data might be
print("\nSubset Ingestion Status:")
spark.sql(f"""
    SELECT subset_name, COUNT(*) as row_count, COUNT(DISTINCT uuid) as unique_uuids
    FROM {CATALOG}.trueval_bronze.toucan_raw
    GROUP BY 1
""").show()
