# ch7 Observability

A small pipeline that streams Wikimedia edit events through Redpanda and watches
itself with Prometheus and Grafana.

## What it does

The producer reads the live Wikimedia recent changes stream, encodes each event as
protobuf, and produces it to a Redpanda topic. The consumer reads those events back,
decodes them, and keeps running stats (total messages, distinct users, bots vs non
bots, per server counts) in memory.

Both apps count what they do (events consumed, produced, processed, failed) and
expose those numbers at `/metrics`. Prometheus scrapes both apps every 5 seconds,
and Grafana graphs the result.

## Layout

`cmd/producer` reads the Wikimedia stream and produces to Redpanda.

`cmd/consumer` consumes from Redpanda and aggregates stats.

`internal/metrics` defines and holds the Prometheus counters, and serves `/metrics`.

`internal/stats` is the in memory (and DB) stats store.

`internal/wiki/v1` holds generated protobuf types.

`prometheus.yml` tells Prometheus which apps to scrape.

`grafana/` holds datasource and dashboard provisioning.

## Run it

```
docker compose up --build
```

## Where to look

Redpanda Console at http://localhost:8080

Producer metrics at http://localhost:2112/metrics

Consumer metrics at http://localhost:2113/metrics

Prometheus at http://localhost:9090

Grafana at http://localhost:3000 (anonymous access is on, so no login needed to
view; sign in with admin / admin only if you want to edit dashboards)

## How the metrics work

Nothing is pushed to Prometheus. Each app keeps counters in memory and exposes them
at `/metrics`. Prometheus pulls from the addresses listed in `prometheus.yml` on a
5 second interval. Grafana then queries Prometheus to draw the graphs.
