import json
from pathlib import Path
import pytest
from pyspark.sql import SparkSession
from pipeline.run import run
from pipeline.publish import publish

@pytest.fixture(scope="session")
def spark():
    session = SparkSession.builder.master("local[2]").appName("pipeline-tests").config("spark.sql.shuffle.partitions", "2").config("spark.sql.session.timeZone", "UTC").getOrCreate()
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()

def source(tmp_path, rows):
    path = tmp_path / "input.jsonl"
    path.write_text("".join(json.dumps(row) + "\n" for row in rows))
    return path

def event(**changes):
    return {"event_id":"e1", "order_id":"o1", "customer_id":"c1", "amount_cents":"100", "currency":"USD", "event_time":"2026-01-01T12:00:00Z", "updated_at":"2026-01-01T12:00:00Z", **changes}

def test_deduplication_quality_and_exact_integer_aggregation(tmp_path, spark):
    path = source(tmp_path, [event(), event(), event(event_id="e2", amount_cents="250"), event(event_id="bad", amount_cents="-1")])
    manifest = run(path, tmp_path/"lake", "2026-01-01", spark=spark)
    assert (manifest["accepted_records"],manifest["duplicates_removed"],manifest["rejected_records"]) == (2,1,1)
    batch=tmp_path/"lake"/"batches"/manifest["batch_id"]
    assert spark.read.parquet(str(batch/"gold")).first().revenue_cents == 350
    assert spark.read.parquet(str(batch/"quarantine")).first().reason == "invalid_amount"
    assert (batch/"bronze.jsonl").read_bytes() == path.read_bytes()

def test_replay_is_immutable_and_idempotent(tmp_path, spark):
    path=source(tmp_path,[event()])
    first=run(path,tmp_path/"lake","2026-01-01",spark=spark)
    assert run(path,tmp_path/"lake","2026-01-01",spark=spark) == first
    assert len(list((tmp_path/"lake"/"batches").iterdir())) == 1

def test_latest_version_wins(tmp_path,spark):
    path=source(tmp_path,[event(),event(amount_cents="200",updated_at="2026-01-02T12:00:00Z")])
    result=run(path,tmp_path/"lake","2026-01-01",spark=spark)
    batch=tmp_path/"lake"/"batches"/result["batch_id"]
    assert spark.read.parquet(str(batch/"silver")).first().amount == 200

def test_same_version_conflict_publishes_nothing(tmp_path,spark):
    with pytest.raises(ValueError,match="Conflicting"):
        run(source(tmp_path,[event(),event(amount_cents="200")]),tmp_path/"lake","2026-01-01",spark=spark)
    assert not (tmp_path/"lake"/"batches").exists()
    assert not (tmp_path/"lake"/".writer.lock").exists()

def test_quality_gate_prevents_publication(tmp_path,spark):
    with pytest.raises(ValueError,match="threshold"):
        run(source(tmp_path,[event(amount_cents="not-money")]),tmp_path/"lake","2026-01-01",spark=spark)
    assert not (tmp_path/"lake"/"batches").exists()

def test_malformed_json_is_quarantined(tmp_path,spark):
    path=source(tmp_path,[event()]);path.write_text(path.read_text()+"{broken\n")
    result=run(path,tmp_path/"lake","2026-01-01",0.5,spark=spark)
    assert result["rejected_records"] == 1

def test_outside_date_and_unknown_currency_rejected(tmp_path,spark):
    rows=[event(),event(event_id="e2",currency="XXX"),event(event_id="e3",event_time="2026-01-02T00:00:00Z")]
    result=run(source(tmp_path,rows),tmp_path/"lake","2026-01-01",0.8,spark=spark)
    assert result["rejected_records"] == 2

def test_existing_lock_blocks_second_writer(tmp_path):
    lake=tmp_path/"lake";lake.mkdir();(lake/".writer.lock").write_text("other")
    with pytest.raises(FileExistsError):
        run(source(tmp_path,[event()]),lake,"2026-01-01")
    assert (lake/".writer.lock").read_text() == "other"

class FakeS3:
    def __init__(self, fail=False):self.calls=[];self.fail=fail
    def put_object(self,**kw):
        if self.fail:raise RuntimeError("upload interrupted")
        self.calls.append(kw)

def test_s3_manifest_is_last_and_failure_cannot_commit(tmp_path):
    batch=tmp_path/"abc";batch.mkdir();(batch/"manifest.json").write_text(json.dumps({"batch_id":"abc"}))
    (batch/"silver").mkdir();(batch/"silver"/"part.parquet").write_bytes(b"example")
    client=FakeS3();assert publish(batch,"demo",client=client) == "s3://demo/data-pipeline/batches/abc"
    assert client.calls[-1]["Key"].endswith("manifest.json")
    assert json.loads(client.calls[-1]["Body"])["objects"][0]["bytes"] == 7
    failed=FakeS3(True)
    with pytest.raises(RuntimeError):publish(batch,"demo",client=failed)
    assert failed.calls == []
