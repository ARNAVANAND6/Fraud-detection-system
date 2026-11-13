"""Spark Structured Streaming: Kafka -> validate -> forward, plus the Delta lake.

Four streaming queries, each with its own checkpoint:

  forward valid records are republished unchanged to the `validated` topic,
          which the scoring service consumes (PIPELINE_MODE=spark). Invalid
          ones go to the DLQ topic. This is the low-latency query, so it has
          its own short trigger. It is at-least-once: after a crash a batch
          can be forwarded twice, and the scorer's idempotent writes absorb
          that.
  bronze  raw Kafka records, unparsed, append-only. Replayable source of truth.
  silver  parsed + validated events. Invalid records go to the quarantine
          table. Deduplicated with a Delta MERGE on
          transaction_id, so restarts and producer duplicates never create
          a second row (idempotent sink). Late events are kept and flagged.
  gold    per-minute aggregates by transaction type, using an event-time
          watermark. Events later than the watermark are left out of Gold by
          design; Silver still has them, and the reconciliation counts them.

Delivery guarantee: at-least-once from Kafka, made effectively-once in Delta
by checkpoints + MERGE. This is not end-to-end exactly-once.
"""
from __future__ import annotations

import os

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T

KAFKA = os.environ.get("KAFKA_BOOTSTRAP", "localhost:9092")
ENVIRONMENT = os.environ.get("ENVIRONMENT", "demo").lower()
TOPIC = f"{ENVIRONMENT}.payments.transactions.v1"
TOPIC_VALIDATED = f"{ENVIRONMENT}.payments.transactions.validated.v1"
TOPIC_DLQ = f"{ENVIRONMENT}.payments.transactions.dlq.v1"
LAKE = os.environ.get("LAKE_PATH", "/data/lake") + f"/{ENVIRONMENT}"  # s3a://bucket/lake on AWS
TRIGGER = os.environ.get("TRIGGER_INTERVAL", "10 seconds")          # lake tables
FORWARD_TRIGGER = os.environ.get("FORWARD_TRIGGER", "100 milliseconds")  # scoring path
LATE_THRESHOLD = os.environ.get("LATE_THRESHOLD", "10 minutes")

BRONZE = f"{LAKE}/bronze/transactions"
SILVER = f"{LAKE}/silver/transactions"
QUARANTINE = f"{LAKE}/quarantine/transactions"
GOLD = f"{LAKE}/gold/txn_type_minute"
CHECKPOINTS = f"{LAKE}/_checkpoints"

# Mirrors fraudlib.schemas.TransactionEvent (tests/unit/test_contracts.py checks this).
EVENT_SCHEMA = T.StructType([
    T.StructField("schema_version", T.IntegerType()),
    T.StructField("transaction_id", T.StringType()),
    T.StructField("event_time", T.StringType()),
    T.StructField("account_id", T.StringType()),
    T.StructField("counterparty_id", T.StringType()),
    T.StructField("txn_type", T.StringType()),
    T.StructField("amount", T.DoubleType()),
    T.StructField("old_balance_orig", T.DoubleType()),
    T.StructField("old_balance_dest", T.DoubleType()),
    T.StructField("channel", T.StringType()),
    T.StructField("region", T.StringType()),
    T.StructField("device_id", T.StringType()),
    T.StructField("source", T.StringType()),
    T.StructField("ingested_at", T.StringType()),
])
SOURCES = ["LIVE_API", "LIVE_WEBHOOK", "KAFKA_STREAM", "HISTORICAL_IMPORT", "SIMULATED_DEMO"]
TXN_TYPES = ["PAYMENT", "TRANSFER", "CASH_OUT", "CASH_IN", "DEBIT"]


def spark_session() -> SparkSession:
    return (
        SparkSession.builder.appName("fraud-streaming")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.shuffle.partitions", os.environ.get("SHUFFLE_PARTITIONS", "4"))
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.scheduler.mode", "FAIR")
        .getOrCreate()
    )


def read_kafka(spark: SparkSession) -> DataFrame:
    return (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA)
        .option("subscribe", TOPIC)
        .option("startingOffsets", "earliest")
        .option("maxOffsetsPerTrigger", os.environ.get("MAX_OFFSETS_PER_TRIGGER", "50000"))
        .load()
    )


def parse(raw: DataFrame) -> DataFrame:
    """Parse JSON and attach a validation error column (null = valid)."""
    parsed = raw.select(
        F.col("key").alias("raw_key"),
        F.col("value").cast("string").alias("raw_value"),
        F.col("partition").alias("kafka_partition"),
        F.col("offset").alias("kafka_offset"),
        F.col("timestamp").alias("kafka_timestamp"),
        F.from_json(F.col("value").cast("string"), EVENT_SCHEMA).alias("e"),
    ).select("*", "e.*").drop("e")
    parsed = parsed.withColumn("event_time", F.to_timestamp("event_time"))
    error = (
        F.when(F.col("transaction_id").isNull(), F.lit("UNPARSEABLE_OR_MISSING_ID"))
        .when(F.col("schema_version") != 1, F.lit("UNSUPPORTED_SCHEMA_VERSION"))
        .when(F.col("event_time").isNull(), F.lit("BAD_EVENT_TIME"))
        .when(F.col("account_id").isNull() | F.col("counterparty_id").isNull(), F.lit("MISSING_PARTY"))
        .when(~F.col("txn_type").isin(TXN_TYPES), F.lit("BAD_TXN_TYPE"))
        .when(F.col("amount").isNull() | (F.col("amount") <= 0), F.lit("BAD_AMOUNT"))
        .when(F.col("old_balance_orig").isNull() | F.col("old_balance_dest").isNull()
              | (F.col("old_balance_orig") < 0) | (F.col("old_balance_dest") < 0), F.lit("BAD_BALANCE"))
        .when(F.col("source").isNotNull() & ~F.col("source").isin(SOURCES), F.lit("BAD_SOURCE"))
        .when((F.lit(ENVIRONMENT) == "production") & (F.col("source") == "SIMULATED_DEMO"),
              F.lit("SIMULATED_EVENT_IN_PRODUCTION"))
    )
    return parsed.withColumn("validation_error", error)


def ensure_tables(spark: SparkSession) -> None:
    if not DeltaTable.isDeltaTable(spark, SILVER):
        cols = ", ".join(f"{f.name} {f.dataType.simpleString()}" for f in EVENT_SCHEMA.fields
                         if f.name not in ("event_time", "ingested_at"))
        spark.sql(f"""
            CREATE TABLE IF NOT EXISTS delta.`{SILVER}` (
              {cols}, event_time timestamp, ingested_at timestamp, event_date date, is_late boolean,
              kafka_partition int, kafka_offset bigint, lake_written_at timestamp
            ) USING delta PARTITIONED BY (event_date)
        """)


def write_silver_batch(batch: DataFrame, batch_id: int) -> None:
    spark = batch.sparkSession
    batch = batch.persist()
    invalid = batch.filter(F.col("validation_error").isNotNull())
    valid = batch.filter(F.col("validation_error").isNull())

    # Quarantine: Delta table for analysis (the forward query sends the same records to the DLQ topic).
    # A batch is re-run after a crash, so a plain append would duplicate rows
    # (the recovery test caught this). txnAppId/txnVersion make Delta skip a
    # batch_id it has already committed.
    (invalid.select("raw_value", "validation_error", "kafka_partition", "kafka_offset",
                    F.current_timestamp().alias("quarantined_at"))
        .write.format("delta").mode("append")
        .option("txnAppId", "silver-quarantine").option("txnVersion", batch_id)
        .save(QUARANTINE))
    # Late flag: event time far behind what this batch has already seen.
    max_seen = valid.agg(F.max("event_time")).first()[0]
    if max_seen is None:  # nothing valid in this batch
        batch.unpersist()
        return
    deduped = (
        valid.dropDuplicates(["transaction_id"])
        .withColumn("event_date", F.to_date("event_time"))
        .withColumn("is_late", F.lit(max_seen).isNotNull()
                    & (F.col("event_time") < F.lit(max_seen) - F.expr(f"INTERVAL {LATE_THRESHOLD}")))
        .withColumn("lake_written_at", F.current_timestamp())
        .withColumn("ingested_at", F.coalesce(F.to_timestamp("ingested_at"), F.current_timestamp()))
        .withColumn("source", F.coalesce("source", F.lit("KAFKA_STREAM")))
        .drop("raw_key", "raw_value", "kafka_timestamp", "validation_error")
    )
    (DeltaTable.forPath(spark, SILVER).alias("t")
        .merge(deduped.alias("s"), "t.transaction_id = s.transaction_id")
        .whenNotMatchedInsertAll()
        .execute())
    batch.unpersist()


def main() -> None:
    spark = spark_session()
    spark.sparkContext.setLogLevel("WARN")
    ensure_tables(spark)
    raw = read_kafka(spark)

    spark.sparkContext.setLocalProperty("spark.scheduler.pool", "lake")
    bronze = (
        raw.select(
            F.col("key").cast("string").alias("key"),
            F.col("value").cast("string").alias("value"),
            "topic", "partition", "offset", "timestamp",
            F.current_timestamp().alias("ingested_at"),
        ).withColumn("ingest_date", F.to_date("ingested_at"))
        .writeStream.format("delta").outputMode("append")
        .partitionBy("ingest_date")
        .option("checkpointLocation", f"{CHECKPOINTS}/bronze")
        .trigger(processingTime=TRIGGER)
        .start(BRONZE)
    )

    parsed = parse(raw)

    # Scoring path. One Kafka write per micro-batch: the `topic` column routes
    # each record to the validated topic or the DLQ. It runs in its own FAIR
    # scheduler pool so the slower lake queries cannot hold it up. (Before
    # this, with FIFO scheduling, measured pipeline P50 was about 7 s.)
    spark.sparkContext.setLocalProperty("spark.scheduler.pool", "forward")
    is_valid = F.col("validation_error").isNull()
    forward = (
        parsed.select(
            F.col("raw_key").alias("key"), F.col("raw_value").alias("value"),
            F.when(is_valid, F.lit(TOPIC_VALIDATED)).otherwise(F.lit(TOPIC_DLQ)).alias("topic"),
            F.when(~is_valid, F.array(
                F.struct(F.lit("error").alias("key"), F.col("validation_error").cast("binary").alias("value")),
                F.struct(F.lit("stage").alias("key"), F.lit("spark").cast("binary").alias("value")),
            )).alias("headers"),
        )
        .writeStream.format("kafka").option("kafka.bootstrap.servers", KAFKA)
        .option("includeHeaders", "true")
        .option("kafka.linger.ms", "0")
        .option("checkpointLocation", f"{CHECKPOINTS}/forward")
        .trigger(processingTime=FORWARD_TRIGGER)
        .start()
    )
    spark.sparkContext.setLocalProperty("spark.scheduler.pool", "lake")
    silver = (
        parsed.writeStream.foreachBatch(write_silver_batch)
        .option("checkpointLocation", f"{CHECKPOINTS}/silver")
        .trigger(processingTime=TRIGGER)
        .start()
    )

    gold = (
        parsed.filter(F.col("validation_error").isNull())
        .withWatermark("event_time", LATE_THRESHOLD)
        .dropDuplicatesWithinWatermark(["transaction_id"])
        .groupBy(F.window("event_time", "1 minute"), "txn_type")
        .agg(F.count("*").alias("txn_count"), F.sum("amount").alias("amount_sum"),
             F.approx_count_distinct("account_id").alias("active_accounts"))
        .select(F.col("window.start").alias("window_start"), F.col("window.end").alias("window_end"),
                "txn_type", "txn_count", "amount_sum", "active_accounts")
        .writeStream.format("delta").outputMode("append")
        .option("checkpointLocation", f"{CHECKPOINTS}/gold")
        .trigger(processingTime=TRIGGER)
        .start(GOLD)
    )

    for q in (forward, bronze, silver, gold):
        print(f"started query {q.name or q.id}")
    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()
