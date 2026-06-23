# Chapter 2 - Docker

Containerizing the Go server from Chapter 1 using a multi-stage Dockerfile.

## Run locally

```bash
go run ./cmd/server
```

## Build & run with Docker

```bash
docker build -t go-docker .
docker run -p 8080:8080 go-docker
```
