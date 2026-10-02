import argparse
import hashlib
import json
import os
import shutil
import uuid
from datetime import date
from pathlib import Path
from pyspark.sql import SparkSession
from .transform import SCHEMA, transform

VERSION = "1"

def run(source, lake, batch_date, max_reject_ratio=0.25, spark=None):
    date.fromisoformat(batch_date)
    if not 0 <= max_reject_ratio <= 1:
        raise ValueError("Reject ratio must be between zero and one")
    source, lake = Path(source), Path(lake)
    payload = source.read_bytes()
    digest = hashlib.sha256(payload).hexdigest()
    identity = hashlib.sha256(f"{VERSION}:{batch_date}:{digest}:{max_reject_ratio}".encode()).hexdigest()
    destination = lake / "batches" / identity
    lake.mkdir(parents=True, exist_ok=True)
    # One local writer per lake. O_EXCL also works across processes on one filesystem.
    lock = lake / ".writer.lock"
    fd = os.open(lock, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
    stage = lake / "staging" / str(uuid.uuid4())
    own_spark = spark is None
    frame = None
    try:
        os.write(fd, str(os.getpid()).encode())
        if destination.exists():
            return json.loads((destination / "manifest.json").read_text())
        spark = spark or SparkSession.builder.master("local[2]").appName("order-data-pipeline").config("spark.sql.session.timeZone", "UTC").getOrCreate()
        spark.conf.set("spark.sql.session.timeZone", "UTC")
        stage.mkdir(parents=True)
        (stage / "bronze.jsonl").write_bytes(payload)
        raw = spark.read.schema(SCHEMA).option("mode", "PERMISSIVE").json(str(stage / "bronze.jsonl")).cache()
        total = raw.count()
        if total == 0:
            raise ValueError("Empty input batch")
        silver, gold, rejected, frame = transform(raw, batch_date)
        rejected_count = rejected.count()
        if rejected_count / total > max_reject_ratio:
            raise ValueError("Data quality threshold exceeded")
        accepted = silver.count()
        if not accepted:
            raise ValueError("No valid records to publish")
        silver.write.mode("error").partitionBy("event_date").parquet(str(stage / "silver"))
        gold.write.mode("error").partitionBy("event_date").parquet(str(stage / "gold"))
        rejected.write.mode("error").parquet(str(stage / "quarantine"))
        manifest = {"batch_id": identity, "batch_date": batch_date, "source_sha256": digest, "pipeline_version": VERSION, "input_records": total, "accepted_records": accepted, "rejected_records": rejected_count, "duplicates_removed": total-rejected_count-accepted}
        (stage / "manifest.json").write_text(json.dumps(manifest, indent=2) + "\n")
        destination.parent.mkdir(parents=True, exist_ok=True)
        # Readers see either the complete immutable batch or no batch.
        stage.rename(destination)
        return manifest
    finally:
        if frame is not None:
            frame.unpersist()
        if "raw" in locals():
            raw.unpersist()
        if own_spark and spark is not None:
            spark.stop()
        shutil.rmtree(stage, ignore_errors=True)
        os.close(fd)
        lock.unlink()

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--source", required=True)
    parser.add_argument("--lake", default="lake")
    parser.add_argument("--date", required=True)
    parser.add_argument("--max-reject-ratio", type=float, default=0.25)
    args = parser.parse_args()
    print(json.dumps(run(args.source, args.lake, args.date, args.max_reject_ratio)))

if __name__ == "__main__":
    main()
