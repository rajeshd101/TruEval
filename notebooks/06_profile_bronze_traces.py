# Author: Rajesh Daggupati
# Databricks notebook source
"""
TruEval EDA: Explore Bronze Data
This notebook provides a detailed profiling of the raw ingested traces in the Bronze layer.

Key Features:
- JSON Length Profiling: Analyzes the size of message and tool blobs.
- Sample Inspection: Previews raw questions and conversation JSON.
- Null/Duplicate Checks: Identifies data quality gaps before Silver transformation.

Execution Instructions:
1. Ensure the Bronze table is populated.
2. Run to inspect raw data patterns before developing transformation logic.
"""

# ── Step 1: Configuration and Load ──
from pyspark.sql import functions as F

CATALOG = "bootcamp_students"
bronze_table_name = f"{CATALOG}.trueval_bronze.toucan_raw"

print(f"🧐 Profiling data from: {bronze_table_name}")
bronze_toucan_df = spark.table(bronze_table_name)

# ── Step 2: Payload Distribution Analysis ──
# Understand the complexity of traces by analyzing string lengths of JSON blobs
print("\n=== JSON Payload Size Summary (Characters) ===")
bronze_toucan_df.select(
    F.length("messages").alias("msg_json_length"),
    F.length("tools").alias("tools_json_length"),
    F.length("target_tools").alias("target_tools_length")
).summary("min", "mean", "max").show()

# ── Step 3: Raw Record Inspection ──
# Extract a single sample to visualize the raw JSON structure
print("\n=== Sample Trace Inspection ===")
sample_row = bronze_toucan_df.select("uuid", "question", "messages").first()

print(f"UUID: {sample_row['uuid']}")
print(f"QUESTION: {sample_row['question'][:200]}...")
print("\n" + "="*80)
print("RAW MESSAGES JSON PREVIEW (First 2000 chars):")
print(sample_row["messages"][:2000])
print("="*80)

# ── Step 4: Metadata and Categorical Analysis ──
# Preview target tools and available tools format
print("\n=== Target Tools Format Preview ===")
bronze_toucan_df.select("target_tools").show(10, truncate=False)

print("\n=== Available Tools Format Preview ===")
bronze_toucan_df.select(
    F.substring("tools", 1, 300).alias("tools_json_preview")
).show(5, truncate=False)

# ── Step 5: Data Completeness Audit ──
# Count nulls across all columns to detect ingestion issues
print("\n=== Null Count Audit ===")
bronze_toucan_df.select(
    [F.sum(F.when(F.col(c).isNull(), 1).otherwise(0)).alias(c) for c in bronze_toucan_df.columns]
).show(vertical=True)
