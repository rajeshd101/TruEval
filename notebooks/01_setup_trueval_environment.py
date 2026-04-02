# Author: Rajesh Daggupati
# Databricks notebook source
"""
TruEval Pipeline Setup: Create Schemas
This notebook initializes the environment by creating the necessary Unity Catalog schemas 
(Bronze, Silver, Gold) for the TruEval lakehouse architecture.

Execution Instructions:
1. Ensure you have 'USE CATALOG' and 'CREATE SCHEMA' permissions on the target catalog.
2. Run this once at the start of the project.
"""

# ── Step 1: Configuration ──
# Define the target Unity Catalog
CATALOG = "bootcamp_students"

# ── Step 2: Set Context ──
# Switch to the target catalog context
spark.sql(f"USE CATALOG {CATALOG}")

# ── Step 3: Create Schemas ──
# Bronze: Stores raw data exactly as ingested from source
spark.sql("CREATE SCHEMA IF NOT EXISTS trueval_bronze")

# Silver: Stores cleaned, parsed, and enriched data (flattened traces)
spark.sql("CREATE SCHEMA IF NOT EXISTS trueval_silver")

# Gold: Stores final business metrics, KPIs, and DQ results for the dashboard
spark.sql("CREATE SCHEMA IF NOT EXISTS trueval_gold")

# ── Step 4: Verification ──
print("✅ TruEval Schemas successfully initialized:")
print(f"  - {CATALOG}.trueval_bronze")
print(f"  - {CATALOG}.trueval_silver")
print(f"  - {CATALOG}.trueval_gold")