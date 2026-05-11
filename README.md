# S3 → Elasticsearch Pipeline (Java 21)

## Build & Run

```bash
# Build fat JAR
mvn clean package -q

# Set environment variables
export S3_BUCKET=my-bucket
export S3_PREFIX=testqueue/
export JSON_FILE_NAME=data.json      # filename inside each prefix key
export AWS_REGION=us-east-1
export ES_HOST=localhost
export ES_PORT=9200
export ES_INDEX=testqueue-index
export ES_USERNAME=elastic
export ES_PASSWORD=changeme
export CHECKPOINT_PATH=/tmp/pipeline-checkpoint.txt
export DLQ_PATH=/tmp/pipeline-dlq.jsonl

# AWS credentials via standard chain (env vars, ~/.aws/credentials, or IAM role)
export AWS_ACCESS_KEY_ID=...
export AWS_SECRET_ACCESS_KEY=...

# Run
java -jar target/s3-to-es-pipeline-1.0.0.jar
```

## Before Running — ES Index Settings

Set these on your index before bulk loading to maximize throughput:

```bash
curl -X PUT "localhost:9200/testqueue-index/_settings" -H 'Content-Type: application/json' -d'
{
  "index.refresh_interval": "-1",
  "index.number_of_replicas": 0
}'
```

After the run completes, restore:

```bash
curl -X PUT "localhost:9200/testqueue-index/_settings" -H 'Content-Type: application/json' -d'
{
  "index.refresh_interval": "1s",
  "index.number_of_replicas": 1
}'

# Force a refresh so docs are searchable
curl -X POST "localhost:9200/testqueue-index/_refresh"
```

## Architecture

```
S3 Bucket (testqueue/)
    │
    │ list_objects_v2 (Delimiter='/', PageSize=1000)
    ▼
Paginator ──── saves token ──► CheckpointStore (file)
    │                          (resume on crash)
    │ puts prefix keys
    ▼
BlockingQueue<String> [capacity=10,000]   ← backpressure
    │
    │ taken by 150 Virtual Threads (Java 21)
    ▼
S3JsonFetcher
    │ GetObject → prefix + data.json
    │ parse JSON (Jackson)
    ▼
ElasticsearchBulkWriter
    │ buffer 1,000 docs
    │ bulk() API with retry (exponential backoff on 429)
    ▼
Elasticsearch Index
```

## Key Design Decisions

| Decision | Rationale |
|---|---|
| Virtual Threads (Java 21) | 150 threads cost ~1MB vs 150GB for platform threads. Network I/O releases the thread while waiting. |
| `Delimiter='/'` in S3 list | Returns logical prefix groups, not individual files. One API call covers the whole prefix folder. |
| `BlockingQueue` with maxSize | Producer blocks when workers fall behind — prevents OOM from listing millions of keys faster than they're consumed. |
| Checkpoint after every S3 page | A crash at key #800,000 resumes from there, not from zero. Token saved atomically. |
| ES batch size 1,000 | Sweet spot between round-trip overhead and memory per flush. Tune up to 5,000 for very small docs. |
| `refresh_interval=-1` during load | Disabling refresh removes the most expensive ES operation during bulk load — can 3-5x throughput. |
| Dead Letter Queue | Failed keys (missing JSON, ES reject) written to JSONL file. Reprocess independently after main run. |

## Tuning for Your Scale

| Variable | Default | When to change |
|---|---|---|
| `WORKER_THREADS` | 150 | Increase if S3 GetObject latency is high; decrease if you hit S3 rate limits (503) |
| `ES_BATCH_SIZE` | 1,000 | Increase for small docs (<1KB); decrease if ES heap pressure is high |
| `KEY_QUEUE_CAPACITY` | 10,000 | Increase if lister is frequently blocked; decrease to reduce memory |

## Reprocessing the DLQ

```bash
# The DLQ is a JSONL file — each line is {"key":"...", "reason":"..."}
# Extract just the keys and re-run against them:
cat /tmp/pipeline-dlq.jsonl | jq -r '.key' > failed-keys.txt

# Then implement a DlqReplayRunner that reads this file
# and runs those keys directly through S3JsonFetcher + ESWriter
```
