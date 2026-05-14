# HLD — uclm-dlr-enricher

**Role:** Kafka-to-Kafka DLR enrichment pipeline. Correlates raw DLRs with original dispatch records from Aerospike cache, enriches the DLR with full context, and publishes enriched records downstream. Handles cache misses via exponential-backoff retry scheduling.

---

## 1. Purpose & Responsibilities

| Responsibility | Detail |
|---------------|--------|
| **DLR Correlation** | Joins raw DLR (from gateway) with dispatch record (from Aerospike) using a configurable join key |
| **Field Enrichment** | Merges DLR fields into dispatch record using configurable field mapping + overwrite strategy |
| **Cache Miss Retry** | Schedules retries with exponential backoff (15 → 30 → 60 min) when Aerospike record not yet loaded |
| **Circuit Breaker** | Protects Aerospike from cascading failures via Resilience4j CircuitBreaker |
| **Manual Offset ACK** | Commits Kafka offset only after successful publish — zero data loss |
| **DLQ Routing** | Routes non-retriable errors (parse, missing key, max retries) to DLQ |
| **Analytics** | Publishes `AnalyticsEventDTO` to reporting topic on successful enrichment |
| **Generic Design** | Fully configurable DTO class, key path, field mappings via `application.yml` |

---

## 2. High-Level Architecture

```
┌────────────────────────────────────────────────────────────────────────────────────┐
│                         DLR ENRICHER SERVICE                                       │
│                                                                                    │
│  ┌─────────────────────────────────────────────────────────────────────────────┐   │
│  │  GenericRawDlrConsumer                                                      │   │
│  │  @KafkaListener(topic=${topics.raw})                                        │   │
│  │                                                                             │   │
│  │  consumeRaw(message, Acknowledgment ack)                                   │   │
│  │    ├── STEP 1: Parse JSON → DTO (dtoClass from config)                     │   │
│  │    │     └── JsonProcessingException → DLQ → ACK                          │   │
│  │    ├── STEP 2: Extract lookup key from configured key path                  │   │
│  │    │     └── Missing / blank key → DLQ → ACK                              │   │
│  │    ├── STEP 3: AerospikeLookupService.get(lookupKey)                       │   │
│  │    │     └── CircuitBreaker protected                                      │   │
│  │    ├── STEP 4a: Cache HIT → GenericEnrichmentService.enrich(base, dlr)    │   │
│  │    │              → publisher.publishEnriched(enriched)                   │   │
│  │    │              → analyticsEventPublisher.publish()                     │   │
│  │    │              → ack.acknowledge()                                     │   │
│  │    └── STEP 4b: Cache MISS → publisher.publishRetry(RetryEnvelope)        │   │
│  │                               → ack.acknowledge()                         │   │
│  └─────────────────────────────────────────────────────────────────────────────┘   │
│                                                                                    │
│  ┌─────────────────────────────────────────────────────────────────────────────┐   │
│  │  GenericRetryScheduler                                                      │   │
│  │  @KafkaListener(topic=${topics.retry})                                      │   │
│  │                                                                             │   │
│  │  consumeRetry(message, Acknowledgment ack)                                 │   │
│  │    ├── Deserialize RetryEnvelope { payload, lookupKey, attempt, scheduledAt}│   │
│  │    ├── Wait until scheduledAt (if not yet reached → nack + brief wait)    │   │
│  │    ├── attempt >= maxRetries → DLQ → ACK                                  │   │
│  │    ├── AerospikeLookupService.get(lookupKey)                               │   │
│  │    ├── Cache HIT → enrich + publish + ACK                                 │   │
│  │    └── Cache MISS → publish new RetryEnvelope with increased delay → ACK  │   │
│  └─────────────────────────────────────────────────────────────────────────────┘   │
└────────────────────────────────────────────────────────────────────────────────────┘
           │                    │                    │
           ▼                    ▼                    ▼
  ┌──────────────┐    ┌──────────────────┐    ┌──────────────────────┐
  │  Aerospike   │    │  enriched-dlr    │    │  cs_raw_reporting    │
  │  Cluster     │    │  topic           │    │  topic (analytics)   │
  └──────────────┘    └──────────────────┘    └──────────────────────┘
```

---

## 3. Detailed Processing Flow — Raw Consumer

```mermaid
sequenceDiagram
    participant KR as Kafka (raw DLR topic)
    participant RC as GenericRawDlrConsumer
    participant LS as AerospikeLookupService
    participant CB as CircuitBreaker (aerospike)
    participant AS as Aerospike Cluster
    participant ES as GenericEnrichmentService
    participant FM as GenericFieldMapper
    participant KP as KafkaPublisherService
    participant KE as Kafka (enriched topic)
    participant KRT as Kafka (retry topic)
    participant KD as Kafka (DLQ topic)
    participant AP as AnalyticsEventPublisher
    participant AK as Kafka (analytics topic)

    KR->>RC: raw DLR string (manual ACK)
    RC->>RC: MDC.put(traceId)

    RC->>RC: STEP 1 — mapper.readValue(message, dtoClass)
    alt JSON parse error
        RC->>KP: publishDlq(DlqEnvelope{JSON_PARSE_ERROR})
        KP->>KD: DLQ envelope
        RC->>KR: ack.acknowledge()
        RC-->>KR: return
    end

    RC->>RC: STEP 2 — fieldMapper.getValue(dlr, config.keyPath)
    alt Key missing or blank
        RC->>KP: publishDlq(DlqEnvelope{MISSING_JOIN_KEY})
        KP->>KD: DLQ envelope
        RC->>KR: ack.acknowledge()
        RC-->>KR: return
    end

    RC->>LS: STEP 3 — get(lookupKey)
    LS->>CB: CircuitBreaker.decorateSupplier(aerospikeCircuitBreaker, fetchFromAerospike)
    CB->>AS: client.get(Key(namespace, set, lookupKey))

    alt Cache HIT
        AS-->>CB: Record { payload: "{...}" }
        CB-->>LS: GenericDml
        LS-->>RC: GenericDml (base)

        RC->>ES: STEP 4a — enrich(base, dlr)
        ES->>ES: storeRawInput(base, dlr)  set rawInputField = JSON(dlr)
        ES->>FM: applyFieldMappings(base, dlr)  iterate configured mappings
        Note over FM: mapping: source=status  target=deliveryStatus<br/>strategy: ALWAYS or ONLY_IF_NULL
        ES->>ES: setEnrichmentMetadata  enrichment_timestamp, input_dto_type
        ES-->>RC: enriched GenericDml

        RC->>KP: publishEnriched(enriched)
        KP->>KE: enriched GenericDml JSON
        RC->>AP: publish AnalyticsEventDTO
        AP->>AK:  cs_raw_reporting_topic
        RC->>KR: ack.acknowledge()

    else Cache MISS (GenericDml == null)
        AS-->>CB: null (record not found)
        CB-->>LS: null
        LS-->>RC: null

        RC->>RC: STEP 4b — create RetryEnvelope
        Note over RC: RetryEnvelope {<br/>  payload: dlr,<br/>  lookupKey: "xxx",<br/>  attempt: 1,<br/>  scheduledAt: now + 15min<br/>}
        RC->>KP: publishRetry(retryEnvelope)
        KP->>KRT:  retry topic
        RC->>KR: ack.acknowledge()

    else Circuit breaker OPEN
        CB-->>LS: CallNotPermittedException
        LS-->>RC: null (treat as MISS)
        RC->>KP: publishRetry(retryEnvelope)
        KP->>KRT:  retry topic
        RC->>KR: ack.acknowledge()
    end
```

---

## 4. Retry Flow — GenericRetryScheduler

```mermaid
sequenceDiagram
    participant KRT as Kafka (retry topic)
    participant RS as GenericRetryScheduler
    participant LS as AerospikeLookupService
    participant ES as GenericEnrichmentService
    participant KP as KafkaPublisherService
    participant KE as Kafka (enriched topic)
    participant KRT2 as Kafka (retry topic again)
    participant KD as Kafka (DLQ topic)

    KRT->>RS: RetryEnvelope { payload, lookupKey, attempt, scheduledAt, delayMinutes }

    RS->>RS: now < scheduledAt?  wait (nack briefly) or proceed

    alt attempt >= maxRetries (3)
        RS->>KP: publishDlq(DlqEnvelope{MAX_RETRIES_EXCEEDED})
        KP->>KD: DLQ envelope
        RS->>KRT: ack.acknowledge()
        RS-->>KRT: return
    end

    RS->>LS: get(lookupKey)

    alt Cache HIT
        LS-->>RS: GenericDml (base)
        RS->>ES: enrich(base, payload)
        ES-->>RS: enriched GenericDml
        RS->>KP: publishEnriched(enriched)
        KP->>KE: enriched topic
        RS->>KRT: ack.acknowledge()

    else Cache MISS
        LS-->>RS: null
        RS->>RS: nextDelay = delayMinutes * 2 (exponential: 153060)
        RS->>RS: create new RetryEnvelope { attempt+1, scheduledAt=now+nextDelay }
        RS->>KP: publishRetry(newRetryEnvelope)
        KP->>KRT2:  retry topic (same topic, future delivery)
        RS->>KRT: ack.acknowledge()
    end
```

**Retry Schedule:**

| Attempt | Delay | Cumulative Wait |
|---------|-------|----------------|
| 1 (initial) | 15 min | 15 min |
| 2 | 30 min | 45 min |
| 3 | 60 min | 105 min (~1.75 hrs) |
| After 3 | → DLQ | — |

---

## 5. Enrichment Details — GenericEnrichmentService

### Field Mapping (configurable in yml)

```yaml
generic-message:
  dto-class-name: com.generic.dlrenricher.dto.WhatsAppMessageDto
  key-path: requestId
  raw-input-field: raw_dlr_json

  field-mappings:
    - source: status
      target: deliveryStatus
      overwrite: ALWAYS
    - source: deliveredAt
      target: deliveryTime
      overwrite: ONLY_IF_NULL
    - source: errorCode
      target: failureCode
      overwrite: ALWAYS
```

### Overwrite Strategies

| Strategy | Behaviour |
|----------|-----------|
| `ALWAYS` | Overwrites target field regardless of existing value |
| `ONLY_IF_NULL` | Only sets target if existing value is null/blank/empty |

### Enrichment Metadata (always set)

| Field | Value |
|-------|-------|
| `enrichment_timestamp` | ISO-8601 UTC timestamp of enrichment |
| `input_dto_type` | Simple class name of input DLR DTO |
| `raw_dlr_json` | Full JSON string of original DLR (if `raw-input-field` configured) |

### Nested Path Support

```
source: "metadata.requestId"  → dlr.getMetadata().getRequestId()
source: "status"              → dlr.getStatus()
```

---

## 6. Aerospike Lookup — AerospikeLookupService

```
Read Operation:
  Key key = new Key(namespace, set, lookupKey)
  Record record = client.get(null, key)
  
  if record == null:
    return null  (legitimate MISS — not an error)
  
  String json = record.getString("payload")
  return objectMapper.readValue(json, GenericDml.class)

Circuit Breaker (Resilience4j):
  Opens on: AerospikeConnectivityException (network-level failures)
  Does NOT open on: AerospikeOperationException (legitimate operation errors)
  On OPEN: returns null (treated as MISS → retry scheduled)
```

---

## 7. DLQ Error Codes

| Code | Cause | Retriable |
|------|-------|----------|
| `JSON_PARSE_ERROR` | Raw message is not valid JSON / wrong DTO structure | No |
| `MISSING_JOIN_KEY` | Lookup key field is null or blank | No |
| `MAX_RETRIES_EXCEEDED` | Cache MISS after 3 retry attempts | No — needs investigation |
| `PUBLISH_FAILURE` | Failed to publish enriched record to Kafka | Yes — depends on failure type |

---

## 8. DLQ Envelope Structure

```json
{
  "payload": { ... original DLR or GenericDml ... },
  "errorCode": "MAX_RETRIES_EXCEEDED",
  "errorMessage": "Cache MISS after 3 attempts for key: REQ-123",
  "lookupKey": "REQ-123",
  "attempt": 3,
  "timestamp": "2024-01-01T12:00:00Z",
  "sourceConsumer": "retry-consumer"
}
```

---

## 9. Supported DLR DTOs (configurable)

| DTO Class | Channel | Key fields |
|-----------|---------|-----------|
| `WhatsAppMessageDto` | WhatsApp | `requestId`, `status`, `timestamp` |
| `ApiDlrRequestDto` | Generic HTTP DLR | `requestId`, `mobile`, `status`, `deliveredAt` |
| `RcsDlrWebhookDto` | RCS | `agentId`, `requestId`, `status` |
| *(configurable)* | any | Any DTO by FQCN in config |

---

## 10. Component Map

| Class | Package | Responsibility |
|-------|---------|---------------|
| `GenericRawDlrConsumer` | consumer | Raw DLR Kafka consumer; steps 1–4 |
| `GenericRetryScheduler` | consumer | Retry topic consumer; exponential backoff |
| `GenericEnrichmentService` | service | Orchestrates enrichment: raw store + field mapping + metadata |
| `GenericFieldMapper` | service | Reflection-based get/set for source/target field paths |
| `AerospikeLookupService` | service | Circuit-breaker-protected Aerospike reads |
| `KafkaPublisherService` | service | Publishes enriched / retry / DLQ messages |
| `AnalyticsEventBuilder` | service | Builds `AnalyticsEventDTO` from enriched DLR |
| `AnalyticsEventPublisher` | service | Publishes to analytics Kafka topic |
| `GenericMessageConfig` | config | Holds DTO class, key path, field mappings |
| `FieldMappingProperties` | config | Source → target + overwrite strategy |
| `ResilienceConfig` | config | Resilience4j CircuitBreaker for Aerospike |
| `AerospikeConfig` | config | AerospikeClient initialization |
| `FailureHandlingConfig` | config | DLQ topic and retry behaviour |
| `AerospikeHealthIndicator` | health | Aerospike cluster health check |
| `KafkaHealthIndicator` | health | Kafka connectivity check |

---

## 11. Configuration Reference

| Property | Default | Description |
|----------|---------|-------------|
| `topics.raw` | `iq_channel_dlr_raw` | Input raw DLR topic |
| `topics.retry` | `dlr-retry-topic` | Internal retry scheduling topic |
| `topics.enriched` | `enriched-dlr-topic` | Enriched DLR output topic |
| `topics.dlq` | `dlr-enricher-dlq` | Dead letter queue topic |
| `topics.analytics` | `cs_raw_reporting_topic` | Analytics output topic |
| `dlr.retry.initial-delay-minutes` | `15` | Initial cache miss retry delay |
| `generic-message.dto-class-name` | — | Fully qualified DLR DTO class |
| `generic-message.key-path` | — | Dot-notation path to lookup key in DLR |
| `generic-message.raw-input-field` | — | Field on GenericDml to store raw DLR JSON |
| `generic-message.field-mappings` | — | List of source→target field mappings |
| `aerospike.host` | — | Aerospike host |
| `aerospike.namespace` | — | Aerospike namespace |
| `aerospike.set` | — | Aerospike set name |
| `kafka.bootstrap.servers` | — | Kafka broker addresses |
