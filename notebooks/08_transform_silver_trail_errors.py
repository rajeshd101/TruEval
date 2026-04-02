# Author: Rajesh Daggupati
# Databricks notebook source
"""
TruEval Transformation: Silver TRAIL (Span & Error Parser)
This notebook transforms raw TRAIL benchmark JSON traces into structured error logs.

Key Features:
- Nested JSON Parsing: Extracts trace_id and span details from complex objects.
- Error Explosion: Flattens nested error arrays into a one-row-per-error format.
- Category Analysis: Aggregates errors by category and benchmark source (GAIA/SWE-Bench).

Execution Instructions:
1. Ensure the TRAIL Bronze table is populated.
2. Run to generate the Silver error-span table for detailed benchmark analysis.
"""

# ── Step 1: Configuration and Load ──
from pyspark.sql import functions as F
from pyspark.sql.types import ArrayType, StringType, StructType, StructField

CATALOG = "bootcamp_students"
BRONZE = f"{CATALOG}.trueval_bronze.trail_raw"
SILVER_TRAIL = f"{CATALOG}.trueval_silver.trail_spans"

bronze_trail_df = spark.table(BRONZE)
print(f"✅ Read {bronze_trail_df.count()} rows from Bronze TRAIL")

# ── Step 2: Define JSON Schemas ──
# Schema for the 'trace' field containing span and hierarchy data
trace_data_schema = StructType([
    StructField("trace_id", StringType()),
    StructField("spans", ArrayType(StructType([
        StructField("span_id", StringType()),
        StructField("parent_span_id", StringType()),
        StructField("span_name", StringType()),
        StructField("span_kind", StringType()),
        StructField("service_name", StringType())
    ])))
])

# Schema for the 'labels' field containing expert-annotated error details
labels_data_schema = StructType([
    StructField("trace_id", StringType()),
    StructField("errors", ArrayType(StructType([
        StructField("category", StringType()),
        StructField("location", StringType()),
        StructField("evidence", StringType()),
        StructField("description", StringType())
    ])))
])

# ── Step 3: Parsing and Status Tagging ──
parsed_trail_df = (
    bronze_trail_df
    .withColumn("trace_parsed", F.from_json("trace", trace_data_schema))
    .withColumn("labels_parsed", F.from_json("labels", labels_data_schema))
    .withColumn("trace_id", F.col("trace_parsed.trace_id"))
    .withColumn("span_count", F.size("trace_parsed.spans"))
    .withColumn("error_count", F.size("labels_parsed.errors"))
    .withColumn("parse_status",
        F.when(F.col("trace_parsed").isNull() | F.col("labels_parsed").isNull(), "FAILED")
         .otherwise("OK"))
)

print("Parse Status Breakdown:")
parsed_trail_df.groupBy("parse_status").count().show()

# ── Step 4: Error Flattening (Explosion) ──
# Convert nested error objects into individual rows for granular analysis
exploded_trail_errors_df = (
    parsed_trail_df
    .filter(F.col("parse_status") == "OK")
    .select(
        "trace_id", "span_count", "error_count", F.col("trail_source"),
        F.explode_outer("labels_parsed.errors").alias("error_obj")
    )
    .select(
        "trace_id", "span_count", "error_count", "trail_source",
        F.col("error_obj.category").alias("error_category"),
        F.col("error_obj.location").alias("error_span_id"),
        F.col("error_obj.evidence").alias("error_evidence"),
        F.col("error_obj.description").alias("error_description")
    )
    .withColumn("ingest_date", F.current_date())
)

print(f"✅ Flattened to {exploded_trail_errors_df.count()} granular error records.")

# ── Step 5: Persistence to Silver ──
(
    exploded_trail_errors_df
    .write
    .mode("overwrite") # Overwrite for benchmarks since they are static
    .option("overwriteSchema", "true")
    .saveAsTable(SILVER_TRAIL)
)

print(f"✅ Silver TRAIL table written: {SILVER_TRAIL}")

# ── Step 6: Summary Analysis ──
print("\n=== Error Distribution by Category ===")
exploded_trail_errors_df.groupBy("error_category").agg(
    F.count("*").alias("total_errors"),
    F.countDistinct("trace_id").alias("affected_traces")
).orderBy(F.desc("total_errors")).show(truncate=False)

print("\n=== Benchmarking Source Summary ===")
exploded_trail_errors_df.groupBy("trail_source").agg(
    F.count("*").alias("error_count"),
    F.countDistinct("trace_id").alias("unique_traces")
).show()
