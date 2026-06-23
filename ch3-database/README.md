# Chapter 3 — Database

Adds a ScyllaDB backend to persist data with Docker Compose.

## Run with Docker Compose

```bash
docker compose up
```

### Verify:

- In-memory: `http://localhost:7001/stats`

- ScyllaDB runs on port `9042`: `cd ch3-database && docker compose exec scylla cqlsh -e "SELECT * FROM wiki.stats_snapshots LIMIT 5;"`

## Dependencies

- [`github.com/apache/cassandra-gocql-driver`](https://github.com/apache/cassandra-gocql-driver) — ScyllaDB/Cassandra driver
