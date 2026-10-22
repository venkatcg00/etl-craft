# Signalling a pipeline from other systems

A small cloud function inserts a signal into a team-owned warehouse table. An `EVENT_WAIT`
task reads new signal ids; etl-craft records their consumption with the wait's success. The
function knows nothing about pipeline runs. It never calls etl-craft's API or connects to its
Engine DB.

## The signal table and sender

Create the table through your warehouse deployment. This PostgreSQL example uses a JSON text
payload; use the corresponding string and timestamp types on your warehouse:

```sql
CREATE TABLE integration.signals (
    event_name VARCHAR NOT NULL,
    event_id VARCHAR PRIMARY KEY,
    payload TEXT NOT NULL,
    received_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP
);
GRANT INSERT ON integration.signals TO signal_sender;
```

The function's database identity has INSERT on this table and nothing else. Schema creation,
grants and retention belong to the deployment, using another identity. Keep the connection
credential in the cloud's secret manager and connect over TLS. Never include it in the event
payload or logs.

The function executes a parameterized INSERT. On PostgreSQL, a repeated delivery is a no-op:

```sql
INSERT INTO integration.signals (event_name, event_id, payload)
VALUES (:event_name, :event_id, :payload)
ON CONFLICT (event_id) DO NOTHING;
```

Use your driver's bind syntax. On other warehouses, use their insert-only duplicate handling,
or acknowledge an enforced duplicate-key error. Do not grant SELECT, UPDATE or DELETE to the
function. Some warehouses treat primary keys as informational: repeated physical rows may
then exist, but etl-craft still consumes each event id once and collapses identical results
within a probe. Conflicting payloads for one id fail rather than selecting one.

Build `event_id` from the storage account/bucket, object key and version or ETag, using an
unambiguous encoding such as a JSON array. Do not use only the object name if it can be
replaced. A redelivery uses the same id. Store the original object coordinates in the JSON
payload so a downstream task can fetch that exact version. A new message id or object version
is a new signal; reusing an id never reprocesses it for the same wait task.

## S3 notifications to Lambda

1. Configure S3 object-created notifications, filtered to the input prefix, with a Lambda
   destination. Allow invocation from that bucket.
2. Read the warehouse credential from AWS Secrets Manager using the Lambda execution role;
   limit secret access to that credential.
3. For each record, URL-decode `s3.object.key`, then combine `s3.bucket.name`, the key and
   `s3.object.versionId` into the id. Enable bucket versioning for replaceable objects. For an
   unversioned object use its `eTag`; refuse an event lacking both.
4. Insert `event_name='orders_ready'`, that id and the JSON object coordinates. Acknowledge
   only after commit; retry a transient database error. A duplicate id is acknowledged.

The function can construct this signal from the notification without reading the object.
See AWS's [S3 notification fields](https://docs.aws.amazon.com/AmazonS3/latest/userguide/notification-content-structure.html)
and [Lambda destination setup](https://docs.aws.amazon.com/lambda/latest/dg/with-s3.html).

## Azure Event Grid to Azure Functions

1. Add an Event Grid subscription for `Microsoft.Storage.BlobCreated`, filtered to the input
   container/prefix, targeting an Event Grid-triggered Azure Function.
2. Give the function's managed identity access to its warehouse credential in Azure Key
   Vault. The database login remains INSERT-only.
3. Combine the storage account, blob URL/key and `data.eTag` for the event id, and retain those
   coordinates in the payload. Use the blob version id when the notification provides it.
4. Insert and commit, treating duplicate delivery as success. Let transient failures retry.

Subscribe to completed object creation events for the upload protocol you use; an early
file-creation event is not proof that its content is complete. See the
[BlobCreated event schema](https://learn.microsoft.com/en-us/azure/event-grid/event-schema-blob-storage)
and [Event Grid function trigger](https://learn.microsoft.com/azure/azure-functions/functions-bindings-event-grid-trigger).

## GCS notifications through Pub/Sub to Cloud Functions

1. Configure the bucket to publish `OBJECT_FINALIZE` notifications to a Pub/Sub topic, and
   subscribe a Cloud Function to that topic. Authorize the bucket's publisher identity.
2. Use the function's service account to read its warehouse credential from Google Secret
   Manager, restricted to that secret.
3. Decode the Pub/Sub message. Combine the bucket, object name and object generation into
   the id; retain them in the payload. The generation identifies this object version.
4. Insert and commit before acknowledging. Redelivery keeps the same id; transient database
   failures retry.

See [Cloud Storage Pub/Sub notifications](https://docs.cloud.google.com/storage/docs/pubsub-notifications)
and the [topic-to-function setup](https://docs.cloud.google.com/run/docs/triggering/storage-triggers).

## The wait and downstream tasks

Select one event name within a bounded window. This PostgreSQL query returns the id and
payload; adjust interval syntax for your warehouse:

```sql
SELECT event_id, payload
FROM integration.signals
WHERE event_name = 'orders_ready'
  AND received_at >= CURRENT_TIMESTAMP - INTERVAL '7 days'
ORDER BY received_at, event_id
```

Set these task parameters:

| Parameter | Value |
|---|---|
| `EVENT_SQL` | the SELECT above |
| `EVENT_KEY_COLUMN` | `event_id` |
| `EVENT_MAX_ROWS` | `10000` |
| `EVENT_POLL_SECONDS` | `60` |
| `EVENT_TIMEOUT_MINUTES` | `NONE` |

The window must cover expected downtime. Do not silently truncate a batch with LIMIT: the
engine reads at most the limit plus one overflow row and fails if the query exceeds
`EVENT_MAX_ROWS`. Narrow the window or increase the bound when that happens. It checks the
bound before removing consumed keys, so retained old signals also count.

Each selected, unconsumed id is recorded in `AUD_EVENT_KEYS`, unique on task and id, in the
same transaction as the wait's successful ending. A failed completion consumes nothing.
Downstream failure keeps the wait's evidence for retry and recovery of that exact run. Signals
arriving after the wait succeeds belong to a later run.

Downstream SQL uses escaped literals, for example `WHERE event_id IN ($$event_keys)`.
Python receives `task.event_keys` as a tuple and `task.event_rows` as a tuple of mappings of
the other selected columns, in the same order. JSON text payloads can be decoded with
`json.loads(row['payload'])`. An `INPUT_PARAMS` value exactly `"$$event_keys"` becomes a list.
Use SUCCESS dependencies on the wait. The saved evidence comes from the exact pipeline run,
including after a controller restart; it is not reread from the signal table.

Without `EVENT_KEY_COLUMN`, a SQL wait retains its first-row readiness behavior. A top-level
aggregate without GROUP BY or HAVING is refused because it returns a row even for no input;
use `SELECT 1 AS ready ... WHERE ...` or add `HAVING count(*) > 0`.

## Retention and streams

The team purges signal rows by age with its maintenance identity. Keep them long enough for
expected outages and replay windows. etl-craft only reads this table; there is no team-written
claim, delete or release SQL in the pipeline. S6.A owns retention of the consumption ledger.
Keeping or deleting a source row does not undo recorded consumption.

Kafka, Kinesis and Event Hubs are streams. Read them in batches and commit offsets with task
success; partitioned offsets are S5.AH. Signals announce bounded work and do not replace a
stream's offset or replay protocol.
