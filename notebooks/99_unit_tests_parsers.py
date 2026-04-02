# Author: Rajesh Daggupati
# Databricks notebook source
"""
TruEval Unit Testing: Parser Logic
This notebook validates the core regex and multi-format JSON parsing logic
without requiring a full pipeline run.

Usage:
Run this after making changes to the Silver transformation logic in 07.
"""

# ── Step 1: Mock Data ──
from pyspark.sql import functions as F
from pyspark.sql.types import StringType

test_data = [
    # Case 1: SFT Format (tool_call role)
    ("uuid_1", "SFT", "[{'role': 'tool_call', 'content': \"{'name': 'find_rhymes', 'args': {}}\"}]"),
    # Case 2: Kimi/Qwen Format (function_call object)
    ("uuid_2", "Kimi-K2", "[{'role': 'assistant', 'function_call': {'name': 'mcp-server-find_rhymes'}}]"),
    # Case 3: Malformed JSON
    ("uuid_3", "Qwen3", "invalid_json")
]

test_df = spark.createDataFrame(test_data, ["uuid", "source", "messages"])

# ── Step 2: Define Parsing Logic (Synchronized with Notebook 07) ──
from pyspark.sql.types import ArrayType, StructType, StructField

message_json_schema = ArrayType(
    StructType([
        StructField("role", StringType()),
        StructField("content", StringType()),
        StructField("function_call", StructType([
            StructField("name", StringType()),
            StructField("arguments", StringType())
        ]))
    ])
)

# ── Step 3: Execute Test Transformation ──
print("🛠️ Testing Parser Transformation...")

results_df = (
    test_df
    .withColumn("messages_array", F.from_json("messages", message_json_schema))
    .withColumn("parse_status", F.when(F.col("messages_array").isNull(), "FAILED").otherwise("OK"))
    
    # Tool Name Extraction Logic
    .withColumn("sft_names", F.transform(F.filter("messages_array", lambda x: x["role"] == "tool_call"), 
        lambda x: F.regexp_extract(x["content"], r"'name':\s*'([^']+)'", 1)))
    .withColumn("fc_names", F.transform(F.filter("messages_array", lambda x: x["function_call"]["name"].isNotNull()), 
        lambda x: x["function_call"]["name"]))
    .withColumn("tools_used_array", F.when(F.size("fc_names") > 0, F.col("fc_names")).otherwise(F.col("sft_names")))
    .withColumn("tools_used", F.array_join("tools_used_array", ", "))
)

# ── Step 4: Validate Expectations ──
print("\n" + "="*60)
print("            PARSER UNIT TEST RESULTS")
print("="*60)

final_results = results_df.collect()

# Validation 1: SFT Parsing
sft_row = next(r for r in final_results if r["uuid"] == "uuid_1")
sft_success = "find_rhymes" in sft_row["tools_used"]
print(f" {'✅' if sft_success else '❌'} SFT Format Parsing: {sft_row['tools_used']}")

# Validation 2: Kimi/Qwen Parsing
kimi_row = next(r for r in final_results if r["uuid"] == "uuid_2")
kimi_success = "mcp-server-find_rhymes" in kimi_row["tools_used"]
print(f" {'✅' if kimi_success else '❌'} Kimi/Qwen Format Parsing: {kimi_row['tools_used']}")

# Validation 3: Error Handling
error_row = next(r for r in final_results if r["uuid"] == "uuid_3")
error_success = error_row["parse_status"] == "FAILED"
print(f" {'✅' if error_success else '❌'} Error Handling for Malformed JSON")

print("="*60)
if sft_success and kimi_success and error_success:
    print("🌟 ALL UNIT TESTS PASSED")
else:
    print("🚨 SOME TESTS FAILED")
