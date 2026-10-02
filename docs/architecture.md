# Architecture and recovery

## Publication boundary

A batch identity hashes the input bytes, event date, transform version and reject threshold. Input bytes are captured once before parsing. Spark reads the preserved copy so the manifest fingerprint and Bronze cannot disagree if the source file changes. Increment VERSION whenever transformation semantics change.

An exclusive lock protects one local lake writer. Work happens under staging. Successful Spark output plus manifest is renamed into batches on the same filesystem. Failed quality checks or writes remove staging and release the lock. A completed identity is replayed from its manifest. Readers enumerate batches, never staging.

This protects process-level failures; rename atomicity is not an fsync-backed guarantee against host power loss. A corrupt existing manifest fails rather than silently regenerating output. Operators should verify it and rebuild from Bronze into a separate lake.

## Validation and versions

Missing identifiers, unsupported currency, nonpositive/noninteger/overflowing amounts, unparseable timestamps, malformed JSON and wrong event dates receive deterministic first-error reasons. Valid exact duplicates collapse. Multiple different payloads for the same event ID and update timestamp fail the whole batch instead of choosing an arbitrary row. Latest update wins otherwise. Gold totals never mix currencies.

The rejection ratio is calculated before deduplication: rejected/input, not rejected/unique events. At least one valid event is required. The pipeline assumes immutable daily full snapshots. A corrected file creates a separate batch and requires deliberate selection by downstream consumers.

## S3 protocol

Publish only local completed batches. Each uploaded object gets SHA-256 metadata; the final manifest lists object paths, sizes and hashes. A failure before the final PUT leaves partial uncommitted objects. Retrying overwrites the same keys and writes the marker only after successful object uploads. Consumers must not treat an arbitrary prefix listing as committed data.

The tests verify object ordering and failure handling with a fake client. They do not validate IAM, encryption, bucket policies, network access or Athena execution. Production deployment should add bucket versioning, scoped identities, lifecycle cleanup of abandoned uploads, checksum verification and catalog registration coordinated with completion.

## Airflow

Airflow invokes the same Python API used by the CLI. Process returns the immutable batch path; publication uses that path. Task retries reuse completed transformation output. Only the path travels through XCom, not a Spark DataFrame or full dataset. Missing dated inputs fail. Standalone with a local persistent output volume is a development topology, not a production scheduler deployment.
