"""Reconciliation: every record in Bronze must end up in Silver or quarantine.

  bronze rows  = Kafka records received (duplicates included)
  silver rows  = unique valid transactions
  quarantine   = invalid records
  duplicates   = bronze - quarantine - silver   (must equal duplicate deliveries)

Run: spark-submit ... reconcile.py   (same jars and LAKE_PATH as stream_job.py)
"""
import json
import os

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

LAKE = os.environ.get("LAKE_PATH", "/data/lake") + "/" + os.environ.get("ENVIRONMENT", "demo").lower()

spark = (SparkSession.builder.appName("reconcile")
         .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
         .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
         .getOrCreate())
spark.sparkContext.setLogLevel("ERROR")

bronze = spark.read.format("delta").load(f"{LAKE}/bronze/transactions")
silver = spark.read.format("delta").load(f"{LAKE}/silver/transactions")
quarantine = spark.read.format("delta").load(f"{LAKE}/quarantine/transactions")
gold = spark.read.format("delta").load(f"{LAKE}/gold/txn_type_minute")

b, s, q = bronze.count(), silver.count(), quarantine.count()
report = {
    "bronze_records": b,
    "silver_unique_transactions": s,
    "silver_distinct_ids": silver.select("transaction_id").distinct().count(),
    "silver_flagged_late": silver.filter("is_late").count(),
    "quarantine_records": q,
    "quarantine_by_error": {r["validation_error"]: r["n"] for r in
                            quarantine.groupBy("validation_error").agg(F.count("*").alias("n")).collect()},
    "implied_duplicates_dropped": b - s - q,
    "gold_txn_count_sum": gold.agg(F.sum("txn_count")).first()[0],
    "bronze_files": len(bronze.inputFiles()),
}
print(json.dumps(report, indent=2))
