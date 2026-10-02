from datetime import datetime, timedelta
from airflow.sdk import dag, task

@dag(schedule="@daily", start_date=datetime(2026, 1, 1), catchup=False, max_active_runs=1, default_args={"retries": 2, "retry_delay": timedelta(minutes=1)}, tags=["pyspark", "data-quality"])
def orders_daily():
    @task
    def process(ds=None):
        import os
        from pathlib import Path
        from pipeline.run import run
        source = Path(os.environ.get("PIPELINE_INPUT", "/opt/airflow/input")) / f"{ds}.jsonl"
        lake = os.environ.get("PIPELINE_LAKE", "/opt/airflow/lake")
        result = run(source, lake, ds)
        return str(Path(lake) / "batches" / result["batch_id"])

    @task
    def publish_if_configured(batch):
        import os
        from pipeline.publish import publish
        bucket = os.environ.get("PIPELINE_S3_BUCKET")
        return publish(batch, bucket) if bucket else {"local_batch": batch, "s3": "disabled"}

    publish_if_configured(process())

orders_daily()
