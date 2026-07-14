# ch8 Multi threaded

A pipeline that streams Wikimedia edit events through Redpanda, consumes them with
several workers at once, and batches the writes into Scylla.

## What it does

The producer reads the live Wikimedia recent changes stream, encodes each event as
protobuf, and produces it to a Redpanda topic.

The consumer runs several workers inside one process. Each worker is its own client
in the same consumer group, so Redpanda splits the topic partitions across them and
they consume in parallel. Every worker buffers the events it reads and writes the
whole buffer to Scylla in one batch, then commits the Kafka offsets for that batch.

The old in memory stats (total messages, distinct users, bots vs non bots, per
server counts) and the Prometheus metrics from before still work.

## How it stays safe on restart

Offsets are committed only after the batch is written to Scylla, never before. If
the app dies mid batch the offsets were not committed, so those records are read
again on the next start. Each row is keyed by its Kafka partition and offset, so a
re read writes the same row instead of a duplicate. Nothing is lost and nothing is
doubled. On a normal shutdown every worker flushes its buffer one last time before
it exits.

## Layout

`cmd/producer` reads the Wikimedia stream and produces to Redpanda.

`cmd/consumer` runs the worker pool, batches events, and commits offsets.

`internal/stats` is the stats store and the Scylla batch writer.

`internal/metrics` holds the Prometheus counters and serves `/metrics`.

`internal/wiki/v1` holds generated protobuf types.

`prometheus.yml` tells Prometheus which apps to scrape.

`grafana/` holds datasource and dashboard provisioning.

## Config

The consumer is controlled by environment variables:

`NUM_CONSUMERS` how many workers to run (default 3)

`BATCH_SIZE` how many events to buffer before writing (default 100)

`BATCH_TIMEOUT` flush the buffer after this long even if it is not full (default 5s)

`SCYLLA_HOST` and `SCYLLA_PORT` where Scylla lives

`REDPANDA_BROKERS`, `WIKI_TOPIC`, `CONSUMER_GROUP`, `METRICS_ADDR`

## Run it

```
docker compose up --build
```

On a fresh volume create the topic before the producer starts:

```
docker compose exec redpanda rpk topic create wiki-events-proto \
  --partitions 6 --replicas 1 \
  --topic-config cleanup.policy=delete \
  --topic-config retention.ms=604800000 \
  --topic-config compression.type=zstd \
  --topic-config max.message.bytes=1048576
```

## Tests

```
go test -race ./...
```

The race test drives many workers at the shared store at once and checks there are
no data races and no lost updates.

## Where to look

Redpanda Console at http://localhost:8080

Producer metrics at http://localhost:2112/metrics

Consumer metrics at http://localhost:2113/metrics

Prometheus at http://localhost:9090

Grafana at http://localhost:3000

Rows in Scylla:

```
docker compose exec scylla cqlsh -e "SELECT COUNT(*) FROM wiki.events;"
```
