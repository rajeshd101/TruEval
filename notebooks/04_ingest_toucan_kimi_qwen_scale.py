# Author: Rajesh Daggupati
# Databricks notebook source
# MAGIC %run ./00_global_config

# COMMAND ----------

"""
TruEval Ingestion: Bronze Toucan Scale (Parallel & Merge)
- Multi-threaded downloads to eliminate sequential bottlenecks
- Dynamic subset handling (Kimi-K2 and Qwen3)
- Delta MERGE for high-volume idempotency
"""

# ── Step 1: Configuration ──
import urllib.request
from concurrent.futures import ThreadPoolExecutor
from pyspark.sql import functions as F
from delta.tables import DeltaTable
import uuid

# Base URL for Kimi-K2 and Qwen3 shards
huggingface_base_url = "https://huggingface.co/datasets/Agent-Ark/Toucan-1.5M/resolve/refs%2Fconvert%2Fparquet"
subsets_to_scale = ["Kimi-K2", "Qwen3"]
shards_per_subset = 20 # Can increase up to 50 for a full backfill

# ── Step 2: Parallel Download Utility ──
def download_shard(subset_name, shard_id):
    """Downloads a single shard to local and stages it in DBFS."""
    try:
        url = f"{huggingface_base_url}/{subset_name}/train/{shard_id}.parquet"
        local_path = f"/tmp/toucan_{subset_name}_{shard_id}.parquet"
        dbfs_path = f"dbfs:/tmp/toucan_{subset_name}_{shard_id}.parquet"
        
        # print(f"⬇️ Downloading {subset_name} Shard {shard_id}...")
        urllib.request.urlretrieve(url, local_path)
        dbutils.fs.cp(f"file:{local_path}", dbfs_path)
        return subset_name, shard_id, dbfs_path
    except Exception as e:
        if "404" in str(e): return subset_name, shard_id, None
        print(f"❌ Failed to download {subset_name} Shard {shard_id}: {e}")
        return subset_name, shard_id, None

# ── Step 3: Run Scaling Loop ──
for subset_name in subsets_to_scale:
    print(f"\n🚀 Scaling Ingestion: Processing Subset {subset_name}")
    
    # Run parallel downloads for the current subset
    shards_to_process = [f"{i:04d}" for i in range(shards_per_subset)]
    with ThreadPoolExecutor(max_workers=8) as executor:
        download_results = list(executor.map(lambda sid: download_shard(subset_name, sid), shards_to_process))

    # Process and merge results
    for _, shard_id, dbfs_path in download_results:
        if not dbfs_path: continue
        
        try:
            source_label = f"toucan_{subset_name.lower().replace('-','_')}_{shard_id}"
            raw_shard_df = spark.read.schema(TOUCAN_EXPECTED_SCHEMA).parquet(dbfs_path)
            
            # Enrich
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
            
            print(f"  ✅ Merged {subset_name} Shard {shard_id} ({raw_shard_df.count()} rows)")
            dbutils.fs.rm(dbfs_path)
            
        except Exception as e:
            print(f"  ❌ Error merging {subset_name} Shard {shard_id}: {e}")

# ── Step 4: Final Verification ──
final_total = spark.table(BRONZE_TOUCAN_TABLE).count()
print(f"\n📊 Total Bronze Records across all subsets: {final_total}")
