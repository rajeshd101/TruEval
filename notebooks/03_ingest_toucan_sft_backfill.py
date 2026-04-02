# Author: Rajesh Daggupati
# Databricks notebook source
# MAGIC %run ./00_global_config

# COMMAND ----------

"""
TruEval Ingestion: Bronze Toucan Backfill (Parallel & Merge)
- Multi-threaded downloads to eliminate sequential bottlenecks
- Global config integration
- Delta MERGE for idempotent updates
"""

# ── Step 1: Configuration ──
import urllib.request
from concurrent.futures import ThreadPoolExecutor
from pyspark.sql import functions as F
from delta.tables import DeltaTable
import uuid

# Base URL for SFT shards
huggingface_base_url = "https://huggingface.co/datasets/Agent-Ark/Toucan-1.5M/resolve/refs%2Fconvert%2Fparquet/SFT/train"
shards_to_ingest = ["0001", "0002", "0003"]

# ── Step 2: Parallel Download Utility ──
def download_shard(shard_id):
    """Downloads a single shard to local and stages it in DBFS."""
    try:
        url = f"{huggingface_base_url}/{shard_id}.parquet"
        local_path = f"/tmp/toucan_sft_{shard_id}.parquet"
        dbfs_path = f"dbfs:/tmp/toucan_sft_{shard_id}.parquet"
        
        print(f"⬇️ Downloading Shard {shard_id}...")
        urllib.request.urlretrieve(url, local_path)
        dbutils.fs.cp(f"file:{local_path}", dbfs_path)
        return shard_id, dbfs_path
    except Exception as e:
        print(f"❌ Failed to download Shard {shard_id}: {e}")
        return shard_id, None

# ── Step 3: Run Parallel Downloads ──
print(f"🚀 Starting parallel download for shards: {shards_to_ingest}")
with ThreadPoolExecutor(max_workers=4) as executor:
    download_results = list(executor.map(download_shard, shards_to_ingest))

# ── Step 4: Process and Merge Each Downloaded Shard ──
for shard_id, dbfs_path in download_results:
    if not dbfs_path: continue
    
    try:
        source_label = f"toucan_sft_{shard_id}"
        
        # Read and enrich
        raw_shard_df = spark.read.schema(TOUCAN_EXPECTED_SCHEMA).parquet(dbfs_path)
        current_run_id = str(uuid.uuid4())
        
        enriched_shard_df = (
            raw_shard_df
            .withColumn("source", F.lit(source_label))
            .withColumn("ingest_ts", F.current_timestamp())
            .withColumn("run_id", F.lit(current_run_id))
        )
        
        # Delta MERGE for idempotency
        bronze_table = DeltaTable.forName(spark, BRONZE_TOUCAN_TABLE)
        (bronze_table.alias("target")
            .merge(
                enriched_shard_df.alias("source"),
                "target.uuid = source.uuid AND target.source = source.source"
            )
            .whenNotMatchedInsertAll()
            .execute())
        
        print(f"✅ Successfully merged Shard {shard_id} ({raw_shard_df.count()} rows)")
        dbutils.fs.rm(dbfs_path) # Clean up
        
    except Exception as e:
        print(f"❌ Error merging Shard {shard_id}: {e}")

# Final count
final_total = spark.table(BRONZE_TOUCAN_TABLE).count()
print(f"\n📊 Current Bronze Row Count: {final_total}")
