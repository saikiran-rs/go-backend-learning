# ch9 Kubernetes

A pipeline that streams Wikimedia edit events through Redpanda, consumes them with
several workers at once, and batches the writes into Scylla, deployed into
Kubernetes (Minikube). The application code is identical to ch8. This chapter adds
the `k8s/` manifests that run every component in a cluster.

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

## Run it on Kubernetes (Minikube)

Everything lives in `k8s/`, numbered so `kubectl apply -f k8s/` applies in order.
All resources go in the `wiki` namespace. Redpanda and Scylla are StatefulSets with
persistent volumes; the producer, consumer, Prometheus and Grafana are Deployments.
Every app knob is externalized in a per-app ConfigMap and injected with `envFrom`,
so all configuration is controllable from the deployment.

### 1. Start Minikube

Scylla and Redpanda need memory headroom. The 2 GB default will OOM Scylla.

```
minikube start --memory=6144 --cpus=4 --disk-size=20g
```

### 2. Build the app images into Minikube's Docker daemon

No registry needed. Build inside Minikube so the `:local` images are found locally
(the Deployments use `imagePullPolicy: IfNotPresent`).

```
eval $(minikube docker-env)
docker build -f cmd/producer/Dockerfile -t wiki-producer:local .
docker build -f cmd/consumer/Dockerfile -t wiki-consumer:local .
```

### 3. Deploy

```
kubectl apply -f k8s/
kubectl -n wiki get pods -w        # wait until all Running / topic Job Completed
```

The consumer's `wait-scylla` initContainer holds it until Scylla's CQL port is open.
The `create-topic` Job makes the 6-partition topic (retries until Redpanda is up).

### 4. Look at it

```
kubectl -n wiki port-forward svc/grafana 3000:3000      # open the dashboard
kubectl -n wiki port-forward svc/prometheus 9090:9090   # targets page shows producer and consumer UP
kubectl -n wiki logs -f deploy/consumer                 # stats climbing
kubectl -n wiki exec sts/scylla -- cqlsh -e "SELECT COUNT(*) FROM wiki.events;"
```

### 5. Change any config from the deployment

```
kubectl -n wiki edit configmap consumer-config          # e.g. bump NUM_CONSUMERS
kubectl -n wiki rollout restart deploy/consumer
```

### Tear down

```
kubectl delete namespace wiki
# or drop the whole cluster: minikube delete
```

## Run it with docker-compose (ch8 path, still works)

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
