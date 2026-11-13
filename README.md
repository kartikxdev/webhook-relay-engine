# Webhook Relay Engine

A self-hosted, lightweight webhook relay and retry worker built with Python, FastAPI, and SQLite WAL mode. It buffers incoming HTTP payloads, guarantees at-least-once delivery using exponential backoff with full decorrelated jitter, and routes exhausted attempts into a Dead-Letter Queue (DLQ).

---

## Architecture Overview

```
Inbound Producer ──► FastAPI Gateway ──► SQLite (WAL Mode) ──► Async Worker Pool ──► Destination
                            │                                          │
                     Idempotency Check                         Retries with Jitter
                                                                       │
                                                              [Exhausted Retries]
                                                                       ▼
                                                               Dead-Letter Queue
```

### Technical Trade-Offs

1. **Storage Choice (SQLite WAL Mode vs External Broker):**
   - Eliminates external dependencies like Redis or RabbitMQ for self-hosted developer setups.
   - Using `PRAGMA journal_mode = WAL` and `PRAGMA busy_timeout = 5000` allows non-blocking concurrent reads alongside background writes.

2. **Retry Logic (Decorrelated Full Jitter):**
   - Prevents synchronized retry waves against recovering downstream servers.
   - Interval equation:
     $$\text{delay} = \min(\text{max\_backoff}, \text{base} \times 2^{\text{attempt}}) \times \text{uniform}(0.5, 1.5)$$

3. **Concurrency & Deduplication:**
   - Ingestion supports unique `idempotency_key` values to eliminate duplicate events.
   - Workers claim batches using atomic database status updates (`PENDING` to `PROCESSING`).

---

## Directory Layout

- `src/api/`: REST endpoints for webhook ingestion, delivery status, and DLQ inspection.
- `src/cli/`: Command-line interface for running workers, server, and replaying failed jobs.
- `src/engine/`: Core retry algorithms, worker loop, HMAC signature generator, and HTTP client.
- `src/models/`: Pydantic domain models and state machine enumerations.
- `src/storage/`: SQLite schema, connection management, and transactional repository.
- `tests/`: Pytest suite covering backoff mathematics, storage operations, and API routes.
- `.github/workflows/ci.yml`: Continuous integration test pipeline across Python versions.

---

## Getting Started

### Installation

```bash
python -m venv .venv
# Activate environment:
# Windows: .venv\Scripts\activate
# Linux/macOS: source .venv/bin/activate

pip install -e ".[dev]"
```

### Initialize Database

```bash
python -m scripts.init_db
```

### Run Tests

```bash
pytest -v
```

---

## Usage

### Run the API Server

```bash
relayforge api --port 8000
```

### Run the Background Worker

```bash
relayforge worker --concurrency 8
```

### Ingest a Webhook

```bash
relayforge ingest --url "https://httpbin.org/post" --payload '{"event": "ping"}'
```

---

## Active Development Roadmap

The following 5 milestones outline upcoming enhancements:

1. **Prometheus Metrics Endpoint:** Export queue depth, worker saturation, and delivery latency metrics via `/metrics`.
2. **Domain Rate Limiting:** Implement per-host token bucket limits to prevent HTTP 429 throttling.
3. **Payload Encryption at Rest:** Store payload bodies with AES-256-GCM encryption in the SQLite database.
4. **Selective Batch DLQ Replay:** Support filtering dead-letter jobs by destination URL or HTTP status code before requeuing.
5. **WebSocket Live Telemetry:** Stream delivery progress and attempt logs to connected dashboard clients in real time.
