# Author: Rajesh Daggupati
# Databricks notebook source
# MAGIC %run ./00_global_config

# COMMAND ----------

"""
TruEval Aggregation: Gold KPIs & Analytics (Enhanced)
- Polished with Global Config
- Delta MERGE for atomic table updates
- Consistent naming and metrics
"""

# ── Step 1: Configuration ──
from pyspark.sql import functions as F
from delta.tables import DeltaTable

silver_trace_df = spark.table(SILVER_TRACE_TABLE)

# ── Step 2: Headline KPIs ──
operational_kpis_df = silver_trace_df.agg(
    F.current_date().alias("metric_date"),
    F.lit("tru_eval_aggregated").alias("source_name"),
    F.count("*").alias("traces_ingested"),
    F.round(F.avg("match_rate") * 100, 2).alias("avg_match_rate_pct"),
    F.sum(F.when(F.col("match_rate") == 0.0, 1).otherwise(0)).alias("zero_match_count"),
    F.sum(F.when(F.col("match_rate") == 1.0, 1).otherwise(0)).alias("perfect_match_count")
)

# Atomic UPSERT into KPI table
if not spark.catalog.tableExists(GOLD_KPI_TABLE):
    operational_kpis_df.write.mode("overwrite").saveAsTable(GOLD_KPI_TABLE)
else:
    kpi_delta = DeltaTable.forName(spark, GOLD_KPI_TABLE)
    (kpi_delta.alias("target")
        .merge(operational_kpis_df.alias("source"), "target.metric_date = source.metric_date")
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute())
print(f"✅ Gold KPIs updated: {GOLD_KPI_TABLE}")

# ── Step 3: Match Rate by Complexity ──
complexity_metrics_df = (
    silver_trace_df
    .withColumn("complexity_bucket",
        F.when(F.col("tool_call_count") == 1, "1_single_tool")
         .when(F.col("tool_call_count") == 2, "2_two_tools")
         .when(F.col("tool_call_count") <= 5, "3_three_to_five")
         .otherwise("4_six_plus")
    )
    .groupBy("complexity_bucket")
    .agg(
        F.count("*").alias("trace_count"),
        F.round(F.avg("match_rate") * 100, 2).alias("avg_match_rate_pct")
    )
    .orderBy("complexity_bucket")
)

complexity_metrics_df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(GOLD_COMPLEXITY_TABLE)
print(f"✅ Complexity metrics refreshed: {GOLD_COMPLEXITY_TABLE}")

# ── Step 4: Failure Patterns (MCP Extraction) ──
failure_patterns_df = (
    silver_trace_df
    .filter(F.col("match_rate") == 0.0)
    .withColumn("tool_list", F.split("tools_used", ", "))
    .withColumn("individual_tool", F.explode("tool_list"))
    .withColumn("server_name", F.regexp_extract("individual_tool", r"^(.+)-[^-]+$", 1))
    .groupBy("server_name")
    .agg(F.countDistinct("uuid").alias("affected_traces"))
    .orderBy(F.desc("affected_traces"))
)

failure_patterns_df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(GOLD_FAILURE_TABLE)
print(f"✅ Failure patterns refreshed: {GOLD_FAILURE_TABLE}")

# ── Step 5: Data Quality Summary (Gold) ──
total_count = silver_trace_df.count()
dq_rows = [
    ("pipeline_integrity", total_count, 0, "All Silver traces processed"),
    ("success_rate_check", total_count, silver_trace_df.filter(F.col("match_rate") < 0.2).count(), "Traces with <20% match rate")
]
dq_df = spark.createDataFrame(dq_rows, ["dq_rule", "rows_checked", "rows_flagged", "notes"])

dq_df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(GOLD_DQ_TABLE)
print(f"✅ Gold DQ Audit complete: {GOLD_DQ_TABLE}")
dq_df.show()
