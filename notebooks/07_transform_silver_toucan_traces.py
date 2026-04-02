# Author: Rajesh Daggupati
# Databricks notebook source
# MAGIC %run ./00_global_config

# COMMAND ----------

"""
TruEval Transformation: Silver Toucan (Enhanced)
- Polished logic with global config
- Broadcast join optimization for incremental loading
- Delta MERGE for atomic upserts
- Automated Quarantine partitioning
"""

# ── Step 1: Initialization ──
from pyspark.sql import functions as F
from pyspark.sql.types import ArrayType, StringType, StructType, StructField
from delta.tables import DeltaTable

# Read raw Bronze data
bronze_raw_df = spark.table(BRONZE_TOUCAN_TABLE)

# ── Step 2: Incremental Delta Detection (Performance Optimized) ──
silver_exists = spark.catalog.tableExists(SILVER_TRACE_TABLE)

if silver_exists:
    # Use BROADCAST join to quickly identify processed UUIDs from Silver
    existing_silver_keys = spark.table(SILVER_TRACE_TABLE).select("uuid", "model_name")
    
    # We derive model_name on the fly for the anti-join
    new_raw_rows_df = (
        bronze_raw_df
        .withColumn("model_name", get_model_name_expr(F.col("source")))
        .join(F.broadcast(existing_silver_keys), on=["uuid", "model_name"], how="left_anti")
    )
    
    delta_count = new_raw_rows_df.count()
    print(f"✅ Incremental: {delta_count} new traces detected.")
else:
    new_raw_rows_df = (
        bronze_raw_df
        .withColumn("model_name", get_model_name_expr(F.col("source")))
    )
    delta_count = new_raw_rows_df.count()
    print(f"🚀 Initializing Silver: Processing {delta_count} traces.")

# ── Step 3: Transformation Pipeline ──
if delta_count == 0:
    print("🏁 Silver is fully up-to-date. Skipping transformation.")
else:
    # Polymorphic schema for SFT/Kimi/Qwen payloads
    message_json_schema = ArrayType(
        StructType([
            StructField("role", StringType()),
            StructField("content", StringType()),
            StructField("function_call", StructType([
                StructField("name", StringType()),
                StructField("arguments", StringType())
            ])),
            StructField("name", StringType())
        ])
    )

    # Core Parser Logic
    parsed_silver_df = (
        new_raw_rows_df
        .withColumn("messages_array", F.from_json("messages", message_json_schema))
        .withColumn("parse_status", F.when(F.col("messages_array").isNull(), "FAILED").otherwise("OK"))
        
        # Tool Call Extraction
        .withColumn("sft_calls", F.size(F.filter("messages_array", lambda x: x["role"] == "tool_call")))
        .withColumn("fc_calls", F.size(F.filter("messages_array", lambda x: x["function_call"]["name"].isNotNull())))
        .withColumn("tool_call_count", F.greatest("sft_calls", "fc_calls"))
        
        # Tool Name Resolution
        .withColumn("sft_names", F.transform(F.filter("messages_array", lambda x: x["role"] == "tool_call"), 
            lambda x: F.regexp_extract(x["content"], r"'name':\s*'([^']+)'", 1)))
        .withColumn("fc_names", F.transform(F.filter("messages_array", lambda x: x["function_call"]["name"].isNotNull()), 
            lambda x: x["function_call"]["name"]))
        .withColumn("tools_used_array", F.when(F.size("fc_names") > 0, F.col("fc_names")).otherwise(F.col("sft_names")))
        .withColumn("tools_used", F.array_join("tools_used_array", ", "))
        
        # Success Metric Logic (endsWith)
        .withColumn("target_tools_array", F.split(F.trim("target_tools"), r",\s*"))
        .withColumn("target_tool_count", F.size("target_tools_array"))
        .withColumn("matches",
            F.size(F.filter("target_tools_array", 
                lambda target: F.exists("tools_used_array", lambda used: used.endswith(target))
            ))
        )
        .withColumn("match_rate",
            F.when(F.col("target_tool_count") == 0, F.lit(None))
             .otherwise(F.col("matches") / F.col("target_tool_count")))
    )

    # ── Step 4: Separation (Good vs. Bad) ──
    good_silver_df = (
        parsed_silver_df
        .filter(F.col("parse_status") == "OK")
        .select(
            "uuid", "subset_name", "model_name", "question",
            "tool_call_count", "tools_used", "target_tools",
            "target_tool_count", "matches", "match_rate"
        )
        .withColumn("ingest_date", F.current_date())
    )

    bad_silver_df = (
        parsed_silver_df
        .filter(F.col("parse_status") == "FAILED")
        .select(
            F.lit("toucan_raw").alias("source_table"),
            "uuid", "messages", "parse_status", 
            F.current_timestamp().alias("quarantine_ts")
        )
    )

    # ── Step 5: Delta MERGE into Silver ──
    if not silver_exists:
        good_silver_df.write.mode("overwrite").saveAsTable(SILVER_TRACE_TABLE)
        print(f"✅ Initial Silver Table created: {SILVER_TRACE_TABLE}")
    else:
        silver_delta = DeltaTable.forName(spark, SILVER_TRACE_TABLE)
        (silver_delta.alias("target")
            .merge(
                good_silver_df.alias("source"),
                "target.uuid = source.uuid AND target.model_name = source.model_name"
            )
            .whenNotMatchedInsertAll()
            .execute())
        print(f"✅ Delta MERGE complete for {good_silver_df.count()} traces.")

    # ── Step 6: Quarantine Logging ──
    if bad_silver_df.count() > 0:
        bad_silver_df.write.mode("append").saveAsTable(QUARANTINE_TABLE)
        print(f"⚠️ {bad_silver_df.count()} failures isolated in {QUARANTINE_TABLE}")

# ── Step 7: Verification ──
silver_stats = spark.table(SILVER_TRACE_TABLE).count()
print(f"\n📊 Total Silver Traces Available: {silver_stats}")
