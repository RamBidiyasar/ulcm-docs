# DLR State Machine

State transitions for a Delivery Report (DLR) as it moves through the DLR pipeline.

---

## DLR Enrichment State Diagram

```mermaid
stateDiagram-v2
    [*] --> RAW_RECEIVED : SMS Gateway sends DLR\n(DLR API Service)

    RAW_RECEIVED --> PUBLISHED_TO_KAFKA : Payload valid\n publish iq_channel_dlr_raw
    RAW_RECEIVED --> DROPPED : Payload invalid\n(missing required fields)

    PUBLISHED_TO_KAFKA --> ENRICHMENT_IN_PROGRESS : DLR Enricher\nconsumes raw DLR

    ENRICHMENT_IN_PROGRESS --> ENRICHED : Aerospike cache HIT\n merge fields\n publish enriched-dlr-topic

    ENRICHMENT_IN_PROGRESS --> RETRY_SCHEDULED : Aerospike cache MISS\n(original dispatch not yet cached)

    RETRY_SCHEDULED --> ENRICHMENT_IN_PROGRESS : Retry attempt\n(after 15 min delay)

    ENRICHMENT_IN_PROGRESS --> RETRY_SCHEDULED : Cache still MISS\n backoff 2×\n(30 min, then 60 min)

    RETRY_SCHEDULED --> DEAD_LETTER : Max retries (3) exhausted\n publish to DLQ

    ENRICHED --> [*]
    DEAD_LETTER --> [*]
    DROPPED --> [*]

    note right of ENRICHMENT_IN_PROGRESS
        Retry schedule:\n15 min  30 min  60 min\n(exponential backoff)
    end note

    note right of ENRICHED
        Published to:\nenriched-dlr-topic\ncs_raw_reporting_topic
    end note

    note right of DEAD_LETTER
        Sent to:\ndlr-enricher-dlq\nRequires manual recovery
    end note
    classDef active fill:#2980b9,color:#fff,stroke:#1f618d
    classDef fail fill:#e74c3c,color:#fff,stroke:#c0392b
    classDef running fill:#f39c12,color:#000,stroke:#d68910
    class RETRY_SCHEDULED active
    class DEAD_LETTER fail
    class ENRICHMENT_IN_PROGRESS running
```

---

## DLR State Transition Ownership Table

| From State | To State | Service Responsible | Mechanism |
|------------|----------|--------------------|-----------| 
| *(gateway)* | `RAW_RECEIVED` | DLR API Service | HTTP POST webhook |
| `RAW_RECEIVED` | `PUBLISHED_TO_KAFKA` | DLR API Service | Kafka producer (`acks=all`) |
| `RAW_RECEIVED` | `DROPPED` | DLR API Service | Validation rejection (no retry) |
| `PUBLISHED_TO_KAFKA` | `ENRICHMENT_IN_PROGRESS` | DLR Enricher | Kafka consumer |
| `ENRICHMENT_IN_PROGRESS` | `ENRICHED` | DLR Enricher | Aerospike cache HIT |
| `ENRICHMENT_IN_PROGRESS` | `RETRY_SCHEDULED` | DLR Enricher | Aerospike cache MISS |
| `RETRY_SCHEDULED` | `ENRICHMENT_IN_PROGRESS` | DLR Enricher | Scheduled retry after delay |
| `RETRY_SCHEDULED` | `DEAD_LETTER` | DLR Enricher | Max retries exhausted |

---

## Dispatch Record State (Aerospike Cache Loader)

```mermaid
stateDiagram-v2
    [*] --> CONSUMED : Orchestrator publishes\nto wa_main_service

    CONSUMED --> VALIDATED : Required fields present

    CONSUMED --> DLQ_SENT : Validation / parse error\n wa_main_service_dlq

    VALIDATED --> WRITTEN_TO_AEROSPIKE : Aerospike write success\n ACK Kafka

    VALIDATED --> RETRY_WRITE : Aerospike write fails

    RETRY_WRITE --> WRITTEN_TO_AEROSPIKE : Retry success\n(up to 3×, exponential backoff)

    RETRY_WRITE --> DLQ_SENT : Retry exhausted\n wa_main_service_dlq

    WRITTEN_TO_AEROSPIKE --> [*]
    DLQ_SENT --> [*]

    note right of VALIDATED
        Aerospike DOWN:\nDon't ACK Kafka\n automatic retry via Kafka
    end note
    classDef fail fill:#e74c3c,color:#fff,stroke:#c0392b
    class DLQ_SENT fail
```

---

## Orchestrator Message Lifecycle State

```mermaid
stateDiagram-v2
    [*] --> DISPATCH_RECEIVED : Validation Governance\npublishes to dispatch topic

    DISPATCH_RECEIVED --> TIME_VALIDATED : Expiry timestamp valid

    DISPATCH_RECEIVED --> TIME_EXPIRED : Expiry timestamp in past\n FailureReason.TIME_VALIDATION_FAILED

    TIME_VALIDATED --> CHANNEL_ROUTED : Channel identified\n(SMS / Email / WA / RCS / Push)

    TIME_VALIDATED --> NO_CHANNEL : No channel enabled\n FailureReason.NO_CHANNEL_ENABLED

    CHANNEL_ROUTED --> SENT_TO_PROVIDER : Provider API call success

    CHANNEL_ROUTED --> PROVIDER_FAILED : Provider API call failure\n CMS quota decremented

    SENT_TO_PROVIDER --> RESPONSE_PUBLISHED : Publish success response\n+ analytics log

    PROVIDER_FAILED --> ERROR_PUBLISHED : Publish error response\n+ analytics log

    RESPONSE_PUBLISHED --> [*]
    ERROR_PUBLISHED --> [*]
    TIME_EXPIRED --> [*]
    NO_CHANNEL --> [*]
    classDef success fill:#27ae60,color:#fff,stroke:#1e8449
    classDef fail fill:#e74c3c,color:#fff,stroke:#c0392b
    class ERROR_PUBLISHED,PROVIDER_FAILED,TIME_EXPIRED fail
    class SENT_TO_PROVIDER success
```
