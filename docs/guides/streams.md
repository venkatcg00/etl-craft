# Reading from streams

A stream (Kafka, Kinesis, Azure Event Hubs) is read in batches, not waited for. A Python task
reads from the positions etl-craft stored, lands what it read, and returns the next position of
every partition as a `PARTITIONS` offset. etl-craft stores those positions only when the attempt
succeeds, in the same Engine DB transaction as its outcome, and checks that no partition moves
backwards or disappears.

For discrete events, such as a file that landed or a job that finished, use a signal row
instead: see [Signalling a pipeline from other systems](external-events.md).

## The pipeline

- Schedule it frequently, for example every five minutes, with `OVERLAP_POLICY='SKIP'`, so one
  run reads at a time.
- One Python task reads the topic and lands the rows. Downstream tasks depend on it with
  `HAS_DATA`, so an empty batch skips them.
- `INPUT_PARAMS` carries the topic and a row limit:
  `{"topic": "clicks", "max_rows": 50000, "bootstrap": "kafka:9092"}`.

## The script

```python
from confluent_kafka import Consumer, TopicPartition

from etl_craft.scripting import Offset, PermanentError, ScriptResult, TransientError


def run(task):
    topic = task.input_params["topic"]
    limit = int(task.input_params["max_rows"])
    stored = dict(task.offset.value) if task.offset else {}
    consumer = Consumer(
        {
            "bootstrap.servers": task.input_params["bootstrap"],
            "group.id": f"etl-craft-{task.pipeline_code}-{task.task_code}",
            "enable.auto.commit": False,
        }
    )
    try:
        partitions = consumer.list_topics(topic, timeout=30).topics[topic].partitions
        plan, positions = [], {}
        for number in sorted(partitions):
            low, high = consumer.get_watermark_offsets(TopicPartition(topic, number), timeout=30)
            start = stored.get(str(number), low)
            if start < low:
                raise PermanentError(
                    f"{topic} partition {number}: stored position {start} is older than the "
                    f"earliest record still kept, {low}; reset it with etl-craft mark --offset"
                )
            positions[str(number)] = start
            if start < high:
                plan.append((number, start, high))
        consumer.assign([TopicPartition(topic, n, start) for n, start, _ in plan])
        rows, open_ends = [], {n: high for n, _, high in plan}
        while open_ends and len(rows) < limit:
            message = consumer.poll(5)
            if message is None:
                continue
            if message.error():
                raise TransientError(f"{topic}: {message.error()}")
            number = message.partition()
            if number not in open_ends or message.offset() >= open_ends[number]:
                continue
            rows.append((topic, number, message.offset(), message.value()))
            positions[str(number)] = message.offset() + 1
            if positions[str(number)] >= open_ends[number]:
                del open_ends[number]
        land(task, rows)
        return ScriptResult(len(rows), Offset.partitions(positions))
    finally:
        consumer.close()
```

`land` writes the rows to the landing table: see the rules below.

## The rules the example follows

- **Assign partitions; don't subscribe.** The script assigns every partition itself, so a
  consumer group never rebalances partitions away in the middle of a batch.
- **Read to the end found at the start of the run.** Each run reads from the stored positions to
  each partition's end as the run found it (`high`), capped by `max_rows`, and never chases the
  tail. A busy topic is caught up by the next runs.
- **Make the landing write safe to repeat.** A crash after the warehouse commit and before the
  attempt's ending makes the retry read the same range again. Land topic, partition and offset
  with every row, and either replace the task run's batch or merge on topic, partition and
  offset.
- **Store the next position to read**, as Kafka reports it (`message.offset() + 1`), and never
  compute it as last plus one across a gap: compacted and transactional topics leave gaps.
- **Fail when the stream has moved past you.** A stored position older than the earliest record
  still kept means records were lost to retention. The script fails, naming the topic, the
  partition and both positions; the remedy is a deliberate reset, never skipping ahead quietly.
- **Return `row_count = 0` for an empty batch**, with the positions unchanged, so `HAS_DATA`
  dependents skip.
- **etl-craft's offset is the source of truth.** Committing the consumer group's offset after
  the run is only for lag monitoring; the next run reads from etl-craft's stored positions.

## What etl-craft checks

When the attempt returns its offset, before anything commits:

- a partition behind its stored position fails the attempt, naming the partition and both
  positions;
- a stored partition missing from the result fails the attempt; a partition that really went
  away is removed with a reset, never by omission;
- a new partition is accepted and named in the task log.

A failed attempt keeps the stored positions, so its retry reads the same range.

## Moving a position on purpose

```bash
etl-craft mark --pipeline-code CLICKS --task-code read_topic \
  --offset '{"3": 120, "7": null}' --reason 'partition 7 removed; replay partition 3'
```

Named partitions are set or added, `null` removes one, and the others keep their positions. The
reset is refused while the task has a live attempt, and is recorded with its reason and actor
(see [Stepping in](run-control.md#reset-a-python-scripts-offset)).
