# Chapter 4 — CI/CD

Adds GitHub Actions workflows for automated testing on PRs and Docker image publishing on merge to `main`.

## Workflows

| Workflow | Trigger | What it does |
|---|---|---|
| `pr-checks.yml` | Pull request → `main` | `go vet`, `golangci-lint`, unit tests, integration tests against ScyllaDB |
| `publish.yml` | Push → `main` | Builds and pushes Docker image to `ghcr.io` |

## Run with Docker Compose

```bash
docker compose up
```

The app is available at `http://localhost:7001`. ScyllaDB runs on port `9042`.
