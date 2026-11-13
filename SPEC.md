# SPEC: RelayForge — Resilient Webhook Relay & Retry Engine

## 1. System Mission & Context
Modern distributed architectures rely on webhooks for asynchronous, event-driven integration between third-party systems (e.g., Stripe, Shopify, GitHub, internal microservices). However, receiving endpoints frequently experience temporary downtime, rate limits (HTTP 429), network partitions, or timeouts (HTTP 504).

**RelayForge** is a self-hosted, local-first webhook relay engine engineered in Python. It ingests inbound webhooks, guarantees at-least-once forwarding with exponential backoff and decorrelated jitter, safeguards downstream systems via concurrency caps, routes repeatedly failing events into an inspectable Dead-Letter Queue (DLQ), and supports manual or automated replays.

---

## 2. Architectural Pillars

```
 [Inbound Webhook Producers]
             │
             ▼  (HTTP POST /api/v1/webhooks/ingest)
 ┌─────────────────────────────────────────────────────────────┐
 │ FastAPI Ingestion Layer                                     │
 │  - Payload schema validation                                │
 │  - Optional HMAC-SHA256 verification                        │
 │  - Idempotency deduplication check                          │
 └───────────────────────────┬─────────────────────────────────┘
                             │
                             ▼  (Atomic Insert)
 ┌─────────────────────────────────────────────────────────────┐
 │ Storage Layer: SQLite (WAL Mode + Pragmas)                  │
 │  - In-process durability without external message broker   │
 │  - Indexed on: status, next_retry_at, idempotency_key       │
 └───────────────────────────┬─────────────────────────────────┘
                             │
                             ▼  (Poll / Claim batch)
 ┌─────────────────────────────────────────────────────────────┐
 │ Asynchronous Worker Engine (Asyncio Task Pool)              │
 │  - Bounded concurrency consumer                             │
 │  - Decorrelated Full Jitter Exponential Backoff             │
 │  - Circuit safety: timeouts & response capture              │
 └─────────────┬─────────────────────────────┬─────────────────┘
               │                             │
    [Success: HTTP 2xx]             [Max Attempts Reached]
               │                             │
               ▼                             ▼
   ┌───────────────────────┐     ┌───────────────────────┐
   │ Status = DELIVERED    │     │ Status = DEAD_LETTER  │
   │ Log attempt metadata  │     │ Move to DLQ Table     │
   └───────────────────────┘     └───────────┬───────────┘
                                             │
                                 [Replay via API / CLI]
                                             │
                                             ▼
                                  (Re-queued to PENDING)
```

### Architectural Decisions & Trade-Offs

1. **Storage Choice (SQLite in WAL Mode vs. Redis/RabbitMQ):**
   - *Rationale:* Eliminates operational overhead and external dependencies for self-hosted developer setups, edge deployments, and local testing.
   - *Optimization:* Configured with `PRAGMA journal_mode = WAL;`, `PRAGMA synchronous = NORMAL;`, `PRAGMA busy_timeout = 5000;`, and `PRAGMA foreign_keys = ON;`. WAL allows concurrent readers alongside a single writer without locking contention.

2. **Retry Strategy (Decorrelated Full Jitter Backoff):**
   - Avoids "thundering herd" spikes against recovering downstream endpoints.
   - Formula:
     $$\text{sleep} = \min(\text{max\_backoff}, \text{base} \times 2^{\text{attempt}}) \times \text{uniform}(0.5, 1.5)$$

3. **At-Least-Once Delivery & Idempotency:**
   - Ingestion records support client-provided `idempotency_key` or auto-generated SHA-256 hash of `(destination_url + payload)`. Duplicate transmissions within a deduplication window are answered with `HTTP 200` returning the existing delivery ID.

---

## 3. Data Dictionary & Schemas

### Table: `webhook_deliveries`
| Column | Type | Description |
|---|---|---|
| `id` | TEXT (UUID4) | Primary key |
| `destination_url` | TEXT | Target HTTP endpoint |
| `http_method` | TEXT | Target method (POST/PUT/PATCH) |
| `payload` | TEXT | JSON string payload |
| `headers` | TEXT | JSON string headers (e.g., Auth, Custom headers) |
| `secret_token` | TEXT | Optional secret used for HMAC-SHA256 signature generation |
| `status` | TEXT | `PENDING`, `PROCESSING`, `DELIVERED`, `FAILED`, `DEAD_LETTER` |
| `attempt_count` | INTEGER | Number of attempts completed |
| `max_attempts` | INTEGER | Maximum allowed retry attempts |
| `idempotency_key` | TEXT | Deduplication key (Unique index) |
| `next_retry_at` | TIMESTAMP | UTC scheduled execution time |
| `created_at` | TIMESTAMP | UTC record creation time |
| `updated_at` | TIMESTAMP | UTC last update time |

### Table: `delivery_attempts`
| Column | Type | Description |
|---|---|---|
| `id` | TEXT (UUID4) | Primary key |
| `delivery_id` | TEXT | Foreign key to `webhook_deliveries.id` |
| `attempt_number` | INTEGER | Sequential attempt number (1..N) |
| `status_code` | INTEGER | HTTP response status code (or NULL on network error) |
| `response_body` | TEXT | Truncated response body (max 4KB) |
| `error_message` | TEXT | Exception details on network/DNS/timeout error |
| `execution_duration_ms` | REAL | Request round-trip time in milliseconds |
| `attempted_at` | TIMESTAMP | UTC attempt timestamp |

### Table: `dead_letter_records`
| Column | Type | Description |
|---|---|---|
| `id` | TEXT (UUID4) | Primary key |
| `delivery_id` | TEXT | Foreign key to `webhook_deliveries.id` (Unique) |
| `failure_reason` | TEXT | Root cause summary (e.g., `MAX_RETRIES_EXCEEDED (HTTP 500)`) |
| `moved_to_dlq_at` | TIMESTAMP | UTC timestamp when moved to DLQ |
| `replay_count` | INTEGER | Number of times replayed |
| `last_replayed_at`| TIMESTAMP | UTC timestamp of most recent replay |

---

## 4. State Transition Machine

```
               ┌──────────┐
               │ PENDING  │◄─────────────────┐ (Manual / Scheduled Replay)
               └────┬─────┘                  │
                    │ Claim batch            │
                    ▼                        │
              ┌────────────┐                 │
              │ PROCESSING │                 │
              └─────┬──────┘                 │
                    │                        │
       ┌────────────┴────────────┐           │
       ▼                         ▼           │
[HTTP 2xx Success]       [HTTP Error / Net Err]
       │                         │
       ▼                         ▼
┌───────────┐         (attempts < max_attempts)
│ DELIVERED │                 ┌─┴─┐
└───────────┘                YES  NO
                              │    │
                              ▼    ▼
                       ┌─────────┐ ┌─────────────┐
                       │ PENDING │ │ DEAD_LETTER │
                       │ (retry) │ └──────┬──────┘
                       └─────────┘        │
                                          └───── Replay Triggered
```

---

## 5. Security & Ingestion Integrity
- Outgoing payloads are signed with an HMAC-SHA256 signature passed in the `X-RelayForge-Signature` header whenever `secret_token` is configured.
- Inbound ingestion supports API key authentication via `X-API-Key`.
- Response bodies stored in `delivery_attempts` are strictly truncated at 4096 characters to prevent disk exhaustion attacks.
