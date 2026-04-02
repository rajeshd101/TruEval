# Author: Rajesh Daggupati
# Databricks notebook source
# MAGIC %run ./00_global_config

# COMMAND ----------

"""
TruEval Ingestion: Bronze Toucan (SFT Subset)
- Uses Global Config
- Implements Delta MERGE for true upsert/idempotency
"""

# ── Step 1: Configuration ──
import urllib.request
from pyspark.sql import functions as F
import uuid

# Source URL for shard 0000
source_data_url = "https://huggingface.co/datasets/Agent-Ark/Toucan-1.5M/resolve/refs%2Fconvert%2Fparquet/SFT/train/0000.parquet"
local_download_path = "/tmp/toucan_sft.parquet"
dbfs_storage_path = "dbfs:/tmp/toucan_sft.parquet"

# ── Step 2: Ingest and Staging ──
urllib.request.urlretrieve(source_data_url, local_download_path)
dbutils.fs.cp(f"file:{local_download_path}", dbfs_storage_path)

# ── Step 3: Enforced Read ──
raw_toucan_df = spark.read.schema(TOUCAN_EXPECTED_SCHEMA).parquet(dbfs_storage_path)

# ── Step 4: Metadata Enrichment ──
current_run_id = str(uuid.uuid4())
enriched_toucan_df = (
    raw_toucan_df
    .withColumn("source", F.lit("toucan_sft_0000")) 
    .withColumn("ingest_ts", F.current_timestamp())
    .withColumn("run_id", F.lit(current_run_id))
)

# ── Step 5: Delta MERGE to Bronze ──
# Merge ensures we only add new data without full table scans or duplication
if not spark.catalog.tableExists(BRONZE_TOUCAN_TABLE):
    enriched_toucan_df.write.mode("overwrite").saveAsTable(BRONZE_TOUCAN_TABLE)
    print(f"🚀 Initialized {BRONZE_TOUCAN_TABLE}")
else:
    from delta.tables import DeltaTable
    bronze_table = DeltaTable.forName(spark, BRONZE_TOUCAN_TABLE)
    
    (bronze_table.alias("target")
        .merge(
            enriched_toucan_df.alias("source"),
            "target.uuid = source.uuid AND target.source = source.source"
        )
        .whenNotMatchedInsertAll()
        .execute())
    print(f"✅ Merged new rows into {BRONZE_TOUCAN_TABLE}")
