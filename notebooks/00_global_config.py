# Author: Rajesh Daggupati
# Databricks notebook source
"""
TruEval Global Configuration
This notebook centralizes all project constants, schema names, and shared utility 
functions for the TruEval pipeline.

Usage:
Add '%run ./00_global_config' at the top of other notebooks to import these settings.
"""

# ── Project Constants ──
CATALOG = "bootcamp_students"
BRONZE_SCHEMA = "trueval_bronze"
SILVER_SCHEMA = "trueval_silver"
GOLD_SCHEMA = "trueval_gold"

# ── Table Names ──
BRONZE_TOUCAN_TABLE = f"{CATALOG}.{BRONZE_SCHEMA}.toucan_raw"
BRONZE_TRAIL_TABLE = f"{CATALOG}.{BRONZE_SCHEMA}.trail_raw"

SILVER_TRACE_TABLE = f"{CATALOG}.{SILVER_SCHEMA}.toucan_trace"
SILVER_TRAIL_TABLE = f"{CATALOG}.{SILVER_SCHEMA}.trail_spans"
QUARANTINE_TABLE = f"{CATALOG}.{SILVER_SCHEMA}.quarantine"

GOLD_KPI_TABLE = f"{CATALOG}.{GOLD_SCHEMA}.trace_kpis"
GOLD_COMPLEXITY_TABLE = f"{CATALOG}.{GOLD_SCHEMA}.match_by_complexity"
GOLD_FAILURE_TABLE = f"{CATALOG}.{GOLD_SCHEMA}.failure_patterns"
GOLD_DQ_TABLE = f"{CATALOG}.{GOLD_SCHEMA}.dq_metrics"

# ── Shared Schemas ──
from pyspark.sql.types import StructType, StructField, StringType

TOUCAN_EXPECTED_SCHEMA = StructType([
    StructField("uuid", StringType(), False),
    StructField("subset_name", StringType(), True),
    StructField("question", StringType(), True),
    StructField("target_tools", StringType(), True),
    StructField("tools", StringType(), True),
    StructField("messages", StringType(), True),
])

# ── Helper Functions ──
def get_model_name_expr(source_col):
    """Returns a Spark expression to derive model_name from source string."""
    from pyspark.sql import functions as F
    return (F.when(source_col.like("%kimi%"), "Kimi-K2")
            .when(source_col.like("%qwen%"), "Qwen3")
            .otherwise("SFT"))

print(f"✅ TruEval Global Config Loaded for Catalog: {CATALOG}")
