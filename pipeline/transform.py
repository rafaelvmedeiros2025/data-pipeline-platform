from pyspark.sql import functions as F, Window
from pyspark.sql.types import StructType, StructField, StringType

FIELDS = ["event_id", "order_id", "customer_id", "amount_cents", "currency", "event_time", "updated_at"]
SCHEMA = StructType([StructField(name, StringType()) for name in FIELDS + ["_corrupt_record"]])

def transform(raw, batch_date):
    frame = raw.withColumn("amount", F.expr("try_cast(amount_cents as bigint)"))
    frame = frame.withColumn("event_ts", F.expr("try_cast(event_time as timestamp)"))
    frame = frame.withColumn("update_ts", F.expr("try_cast(updated_at as timestamp)"))
    checks = [
        (F.col("_corrupt_record").isNotNull(), "malformed_json"),
        (F.col("event_id").isNull() | (F.trim("event_id") == ""), "missing_event_id"),
        (F.col("order_id").isNull() | (F.trim("order_id") == ""), "missing_order_id"),
        (F.col("customer_id").isNull() | (F.trim("customer_id") == ""), "missing_customer_id"),
        (F.col("amount").isNull() | (F.col("amount") <= 0), "invalid_amount"),
        (F.col("currency").isNull() | ~F.col("currency").isin("USD", "BRL", "EUR"), "invalid_currency"),
        (F.col("event_ts").isNull() | F.col("update_ts").isNull(), "invalid_timestamp"),
        (F.to_date("event_ts") != F.lit(batch_date), "outside_batch_date"),
    ]
    reason = F.lit(None).cast("string")
    for condition, name in reversed(checks):
        reason = F.when(condition, F.lit(name)).otherwise(reason)
    frame = frame.withColumn("reason", reason).cache()
    rejected = frame.filter(F.col("reason").isNotNull()).select(*FIELDS, "_corrupt_record", "reason")
    valid = frame.filter(F.col("reason").isNull())
    # Exact duplicates collapse; equal-version conflicting payloads fail the batch.
    valid = valid.select("event_id", "order_id", "customer_id", "amount", "currency", "event_ts", "update_ts").distinct()
    conflicts = valid.groupBy("event_id", "update_ts").count().filter("count > 1")
    if conflicts.limit(1).count():
        frame.unpersist()
        raise ValueError("Conflicting payloads for the same event_id and updated_at")
    rank = Window.partitionBy("event_id").orderBy(F.col("update_ts").desc())
    silver = valid.withColumn("rank", F.row_number().over(rank)).filter("rank = 1").drop("rank")
    silver = silver.withColumn("event_date", F.to_date("event_ts"))
    gold = silver.groupBy("event_date", "currency").agg(F.count("event_id").alias("event_count"), F.sum("amount").alias("revenue_cents"))
    return silver, gold, rejected, frame
