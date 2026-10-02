import argparse
import hashlib
import json
from pathlib import Path
import boto3

def publish(batch, bucket, prefix="data-pipeline", client=None):
    batch = Path(batch)
    manifest = json.loads((batch / "manifest.json").read_text())
    if batch.name != manifest["batch_id"]:
        raise ValueError("Batch directory and manifest disagree")
    client = client or boto3.client("s3")
    root = f"{prefix.strip('/')}/batches/{batch.name}"
    objects = []
    # Commit marker is uploaded last. Retries target identical content-addressed keys.
    for path in sorted(batch.rglob("*")):
        if not path.is_file() or path.name == "manifest.json":
            continue
        relative = path.relative_to(batch).as_posix()
        payload = path.read_bytes()
        checksum = hashlib.sha256(payload).hexdigest()
        client.put_object(Bucket=bucket, Key=f"{root}/{relative}", Body=payload, Metadata={"sha256": checksum})
        objects.append({"path": relative, "sha256": checksum, "bytes": len(payload)})
    committed = {**manifest, "objects": objects}
    client.put_object(Bucket=bucket, Key=f"{root}/manifest.json", Body=json.dumps(committed).encode(), ContentType="application/json")
    return f"s3://{bucket}/{root}"

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--batch", required=True)
    parser.add_argument("--bucket", required=True)
    parser.add_argument("--prefix", default="data-pipeline")
    args = parser.parse_args()
    print(publish(args.batch, args.bucket, args.prefix))
