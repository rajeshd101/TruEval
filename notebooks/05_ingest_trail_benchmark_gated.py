# Author: Rajesh Daggupati
# Databricks notebook source
"""
TruEval Ingestion: Bronze TRAIL Benchmark (Gated Data)
This notebook downloads the TRAIL (Tool-use Reasoning and Action in LLMs) 
benchmark dataset using the Hugging Face Hub API.

Key Features:
- Secure Hub Access: Uses 'huggingface_hub' for gated repo access.
- Multi-source Union: Combines GAIA and SWE-Bench benchmarks into a single TRAIL raw table.
- Schema Adaptation: Uses 'unionByName(allowMissingColumns=True)' to handle source differences.

Execution Instructions:
1. Ensure 'datasets' and 'huggingface_hub' are installed.
2. Replace 'hf_token' with your actual Hugging Face read-access token.
3. This is an idempotent run - it will skip ingestion if the target table has data.
"""

# ── Step 1: Environment Setup ──
# MAGIC %pip install datasets huggingface_hub --upgrade
# MAGIC dbutils.library.restartPython()

# ── Step 2: Download Gated Data ──
from huggingface_hub import hf_hub_download
import os

# Set Hugging Face token for secure access
os.environ["HF_TOKEN"] = "hf_token"  # <-- REPLACE WITH ACTUAL TOKEN

trail_benchmark_files = [
    "data/gaia-00000-of-00001-33a2e72d362d688a.parquet", 
    "data/swe_bench-00000-of-00001-91aa04220f7198b4.parquet"
]

print("⬇️ Downloading TRAIL benchmark datasets...")
for benchmark_file in trail_benchmark_files:
    downloaded_local_path = hf_hub_download(
        repo_id="PatronusAI/TRAIL",
        filename=benchmark_file,
        repo_type="dataset",
        token=os.environ["HF_TOKEN"],
        local_dir="/tmp/trail"
    )
    print(f"  ✅ Local download complete: {downloaded_local_path}")

# ── Step 3: Staging and Spark Load ──
# Move to DBFS for distributed access
dbutils.fs.cp("file:/tmp/trail/data/gaia-00000-of-00001-33a2e72d362d688a.parquet", "dbfs:/tmp/trail_gaia.parquet")
dbutils.fs.cp("file:/tmp/trail/data/swe_bench-00000-of-00001-91aa04220f7198b4.parquet", "dbfs:/tmp/trail_swe.parquet")

# Read into separate dataframes to allow tagging
gaia_raw_df = spark.read.parquet("dbfs:/tmp/trail_gaia.parquet")
swe_bench_raw_df = spark.read.parquet("dbfs:/tmp/trail_swe.parquet")

print(f"📊 GAIA Benchmark: {gaia_raw_df.count()} rows")
print(f"📊 SWE-Bench Benchmark: {swe_bench_raw_df.count()} rows")

# ── Step 4: Union and Enrichment ──
from pyspark.sql import functions as F
import uuid

CATALOG = "bootcamp_students"
target_table_name = f"{CATALOG}.trueval_bronze.trail_raw"

# Tag each record with its specific TRAIL source
gaia_enriched_df = gaia_raw_df.withColumn("trail_source", F.lit("gaia"))
swe_bench_enriched_df = swe_bench_raw_df.withColumn("trail_source", F.lit("swe_bench"))

# Create unified view with metadata
current_run_id = str(uuid.uuid4())
union_trail_df = gaia_enriched_df.unionByName(swe_bench_enriched_df, allowMissingColumns=True)

trail_bronze_df = (
    union_trail_df
    .withColumn("source", F.lit("trail")) # General source tag
    .withColumn("ingest_ts", F.current_timestamp())
    .withColumn("run_id", F.lit(current_run_id))
)

# ── Step 5: Idempotent Write to Bronze ──
try:
    existing_trail_rows = spark.sql(f"SELECT COUNT(*) as cnt FROM {target_table_name}").first()["cnt"]
    if existing_trail_rows > 0:
        print(f"⚠️ TRAIL already loaded in {target_table_name} ({existing_trail_rows} rows). Skipping.")
    else:
        trail_bronze_df.write.mode("append").saveAsTable(target_table_name)
        print(f"✅ Appended {trail_bronze_df.count()} rows to {target_table_name}")
except Exception:
    # Handle first run
    print(f"🚀 Initializing {target_table_name}...")
    trail_bronze_df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(target_table_name)
    print(f"✅ Created {target_table_name} with {trail_bronze_df.count()} rows.")

# ── Step 6: Final Verification ──
spark.sql(f"SELECT trail_source, COUNT(*) FROM {target_table_name} GROUP BY 1").show()
