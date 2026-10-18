# outbox-demo: reactive outbox on Aerospike + Kafka

Spring Boot 3.3 (WebFlux) · Spring Data **Reactive** Aerospike · Spring Kafka · Gradle · Java 17+

## What it does

```
POST /requests ──► Aerospike: save record, status RECEIVED      (one atomic write = the outbox)
                         │
OutboxPublisher ◄────────┘  polls RECEIVED, claims (CAS), sends id to Kafka, marks PUBLISHED
                         │
RequestConsumer ◄─ Kafka ┘  loads record, skips if COMPLETED/FAILED (dedup), claims (CAS),
                            calls the external API, saves response, status COMPLETED / FAILED
StuckRequestSweeper         requeues records nobody is progressing (dead consumer, lost message)
GET /requests/{id}          shows status, response, error, attempts
```

Status flow: `RECEIVED → PUBLISHED → PROCESSING → COMPLETED | FAILED`

There is no separate outbox table: Aerospike writes one record atomically, so the request record
itself is the outbox entry. The Kafka message carries only the record id; data is always read fresh.

## Run it

```bash
docker compose up -d          # Aerospike (:3000) + Kafka (:9092)
gradle wrapper --gradle-version 8.10.2   # once, generates ./gradlew (skip if you already have it)
./gradlew bootRun                         # app on :8080
```

Try each path (the built-in mock external API reacts to a `simulate` field in the payload):

```bash
# happy path
curl -i -X POST localhost:8080/requests -H 'Content-Type: application/json' \
     -d '{"orderId": 1001, "amount": 250}'
curl localhost:8080/requests/<id>                       # RECEIVED/PUBLISHED -> PROCESSING -> COMPLETED

# permanent failure (HTTP 422 from the API) -> FAILED, no retries
curl -X POST localhost:8080/requests -H 'Content-Type: application/json' -d '{"simulate":"bad"}'

# transient failure: API returns 503 twice, then succeeds -> COMPLETED with attempts = 3
curl -X POST localhost:8080/requests -H 'Content-Type: application/json' -d '{"simulate":"flaky"}'

# timeout: API answers after 30 s, client gives up at 10 s -> retried -> eventually FAILED + dead-lettered
curl -X POST localhost:8080/requests -H 'Content-Type: application/json' -d '{"simulate":"slow"}'

# idempotent POST: same key twice -> second call returns 200 with the first record, no duplicate
curl -i -X POST localhost:8080/requests -H 'Content-Type: application/json' \
     -H 'Idempotency-Key: order-1001' -d '{"orderId": 1001}'
```

Things worth trying:

- `docker compose stop kafka`, POST a few requests (they are accepted and stay `RECEIVED`), then
  `docker compose start kafka`: the publisher drains them.
- Kill the app while a `slow` request is `PROCESSING`, restart: it is redelivered / requeued, nothing is lost.
- Same Kafka message twice (restart mid-flight): the second one logs "duplicate delivery, skipping".

## Pointing it at your real API

1. `application.yml`: set `app.external.base-url`, `app.external.path`, `app.external.mock-enabled: false`.
2. Delete `external/MockExternalApiController.java`.
3. If the real contract differs (auth header, body shape, which statuses are transient), change only
   `external/WebClientExternalApiClient.java`.

The client sends `Idempotency-Key: <record id>` on every call. **If the real API does not honour an
idempotency key, one duplicate call is possible** (crash after the API succeeded but before the
COMPLETED write). No pattern removes that window; you would need a "did request X succeed?" lookup.

## Failure matrix

| Crash / failure point | Result |
|---|---|
| After POST save, before publish | Record stays `RECEIVED`; publisher picks it up. |
| After Kafka ack, before `PUBLISHED` write | Published again after the lease; consumer dedups. |
| Kafka down | Records stay `RECEIVED`, publisher keeps retrying. |
| Consumer dies mid-processing | Offset not committed → redelivered; else lease expires → sweeper requeues. |
| API returns 4xx | `FAILED`, message acked. |
| API 5xx / timeout | Retried with backoff; after `max-retries` → `FAILED` + message to `<topic>.DLT`. |
| After API success, before `COMPLETED` write | API called again with the same idempotency key (the unavoidable window). |

## Config knobs (`app.*` in application.yml)

Startup fails fast if `processor.lease` ≤ 2 × `external.timeout` or `publisher.claim-lease` ≤ 2 × `publisher.send-timeout`,
because a lease shorter than the work it protects lets two workers handle the same record.

## Known limits

- **Not compiled or run when this was generated** (the build environment could not reach Maven
  Central; build with `./gradlew build`). Versions pinned: Spring Boot 3.3.5, spring-data-aerospike 4.8.0. If your resolver
  complains about the Aerospike client / reactor client, align them with the spring-data-aerospike
  release you use.
- Aerospike Community Edition has no multi-record transactions. This design needs none (single-record
  writes only).
- The `status` secondary index is low-cardinality; it stays small because records leave `RECEIVED` and
  `PUBLISHED` quickly. Do not query the `COMPLETED` set.
- Finished records are never deleted. Add a TTL (`@Document(expiration = ...)`, requires `nsup-period`
  configured on the namespace) or a cleanup job.
- Aerospike's record size limit (default 1 MB) applies to payload + response.
- Blocking Kafka retry pauses a partition while one record is retried. Fine for moderate volume.
