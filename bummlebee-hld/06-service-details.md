# Service Details — Per-Service Deep Dive

---

## 1. uclm-rate-controller-service

> **Role:** Per-team, per-channel TPS throttle gate. Bridges upstream UCLM output with the validation pipeline.

```mermaid
flowchart LR
    UPSTREAM([upstream comms-input]) -->|Kafka consume| RC["Rate Controller\nPORT: 8081"]
    RC <-->|READ TPS config| PG[("PostgreSQL\ntenant_onboarding\nteam_onboarding")]
    RC -->|rate check\nResilience4j| TPS["TPS Limiter\n(per teamId + channel)"]
    TPS -->|permit granted| KAFKA_OUT[["event topic"]]
    TPS -->|permit denied| NACK["nack(500ms)\nKafka retries"]
    RC -->|lag monitor| LAG["Downstream\nConsumer Lag\nMonitor"]
    classDef kafka fill:#f39c12,color:#000,stroke:#d68910
    classDef svc fill:#2980b9,color:#fff,stroke:#1f618d
    classDef db fill:#27ae60,color:#fff,stroke:#1e8449
    classDef channel fill:#d35400,color:#fff,stroke:#a04000
    class TPS channel
    class PG db
    class KAFKA_OUT,NACK,UPSTREAM kafka
    class LAG,RC svc
```

| Attribute | Value |
|-----------|-------|
| Port | `8081` |
| Framework | Spring Boot · Spring Kafka · Resilience4j RateLimiter · Spring Data JPA |
| Language | Java 17 |
| DB | PostgreSQL — owns `tenant_onboarding`, `team_onboarding` |
| Kafka | Consumes `comms-input` → Produces `event` |
| Global TPS | `channel.global.tps=2000` (configurable) |
| Team TPS | `tenant.channel.tps=5` (per team, loaded from DB) |
| Lag Threshold | `downstream.max.allowed.lag=10000` |
| Downstream Groups | `valgov-consumer-group`, `orch-consumer-group` |

**Key components:**
- `KafkaEventListener` — consumes events, calls throttle processor
- `ChannelThrottleProcessor` — acquires Resilience4j RateLimiter permit
- `TeamRateLimiterManager` — creates/caches per-team rate limiters
- `TpsCache` — in-memory cache of team TPS values (reloaded periodically)
- `KafkaConsumerLagMonitor` — pauses consumer if downstream lag is too high
- `CapacityIncreaseScheduler` — schedules periodic TPS increases for capacity candidates
- `RefreshTpsScheduler` — refreshes TPS config from DB at intervals

---

## 2. uclm-validation-governance-service

> **Role:** Multi-channel validation, DLT compliance, CMS governance, and payload construction.

```mermaid
flowchart LR
    KAFKA_IN[["event topic"]] -->|Kafka consume| VG["Validation Governance\nPORT: 7777"]
    VG -->|read| LF[("Lookup Files\nraw/schema/normalized")]
    VG -->|POST DLT check - SMS only| DLT[("DLT API")]
    VG -->|POST CMS quota| CMS[("CMS API")]
    VG -->|Produce| KAFKA_OUT[["dispatch topic"]]
    VG -->|Produce| ANA[["cs_raw_reporting_topic"]]
    VG -->|Produce| EXC[["exceptions"]]
    classDef kafka fill:#f39c12,color:#000,stroke:#d68910
    classDef svc fill:#2980b9,color:#fff,stroke:#1f618d
    classDef db fill:#27ae60,color:#fff,stroke:#1e8449
    classDef ext fill:#8e44ad,color:#fff,stroke:#6c3483
    class LF db
    class CMS,DLT ext
    class ANA,KAFKA_IN,KAFKA_OUT kafka
    class VG svc
```

| Attribute | Value |
|-----------|-------|
| Port | `7777` |
| Framework | Spring Boot 3.2.4 · Spring Kafka 4.x |
| Language | Java 21 |
| DB | None (lookup files from mounted volumes) |
| Kafka | Consumes `event` → Produces `dispatch`, `cs_raw_reporting_topic`, `exceptions`, `apb-exceptions` |
| Consumer Group | `event-consumer` |
| Publish Mode | `FULL_ONLY` (configurable) |
| Supported Channels | `SMS, EMAIL, WHATSAPP, PUSH_BANK, PUSH_THANKS, D2C, FS, RCS, PUSH` |
| Concurrency | `kafka.consumer.concurrency=4` |

**Key components:**
- Schema validation using lookup files (raw/schema/normalized folders)
- DLT scrubbing for all SMS messages (regulatory compliance)
- CMS quota check before constructing payload
- Language code normalization (supports 13 languages: ENG, HIN, MAR, TEL, GUJ, BEN, KAN, TAM, PUN, ODI, MAL, ASS, URD)
- Category validation: PROMOTIONAL, SERVICE_IMPLICIT, TRANSACTIONAL, SERVICE_EXPLICIT, SERVICE, PROMOTION
- Event type validation: NRT, SCHEDULE, ONETIME, EVENT, RECURRING

---

## 3. uclm-orchestrator-service

> **Role:** Multi-channel dispatch engine. Routes payloads to SMS, Email, WhatsApp, RCS, and Push providers.

```mermaid
flowchart LR
    KAFKA_IN[["dispatch topic"]] -->|Kafka consume| ORCH["Orchestrator\n(Kafka-driven)"]
    ORCH --> TV["TimeValidationService\n(expiry check)"]
    TV --> ROUTE["Channel Router\nSMS  Email  WA  RCS  Push"]
    ROUTE --> BASE["BaseChannelService\n(Template Pattern)"]
    BASE --> SMS["SmsService\n SmsClient (Feign)"]
    BASE --> WA["WhatsappService\n WhatsappClient (Feign)"]
    BASE --> PUSH["PushService\n PushClient (Feign)"]
    BASE --> EMAIL["EmailService\n NetcoreClient (Feign)\n SMTP fallback"]
    BASE --> RCS["RcsService\n RcsClient (Feign)"]
    BASE --> CMS["CmsService\n CmsClient (Feign)\nquota decrement"]
    ORCH -->|Produce| RESP[["dispatch-response\nchannel_partner_*_succ"]]
    ORCH -->|Produce| ERR[["channel_partner_*_err"]]
    ORCH -->|Produce| ANA[["cs_raw_reporting_topic"]]
    ORCH -->|Produce| CACHE[["wa_main_service\n(DLR correlation)"]]
    classDef kafka fill:#f39c12,color:#000,stroke:#d68910
    classDef svc fill:#2980b9,color:#fff,stroke:#1f618d
    classDef db fill:#27ae60,color:#fff,stroke:#1e8449
    classDef ext fill:#8e44ad,color:#fff,stroke:#6c3483
    classDef channel fill:#d35400,color:#fff,stroke:#a04000
    classDef user fill:#34495e,color:#fff,stroke:#2c3e50
    class BASE,ERR,RCS,RESP,ROUTE,WA channel
    class CACHE db
    class CMS,EMAIL ext
    class ANA,KAFKA_IN,ORCH kafka
    class TV svc
    class PUSH,SMS user
```

| Attribute | Value |
|-----------|-------|
| Port | N/A (no REST controllers) |
| Framework | Spring Boot 3.3.4 · Spring Cloud OpenFeign · Spring Kafka 4.x |
| Language | Java 21 |
| DB | None |
| Kafka | Consumes `dispatch` → Produces response, error, analytics, and `wa_main_service` topics |
| Consumer Group | `dispatch-request-consumer-group` |
| Design Pattern | Template Method (BaseChannelService) |
| Provider Priority | SMS → Email → WhatsApp → RCS → Push |

**Failure Reasons (enum):**
| Code | Description |
|------|-------------|
| `TIME_VALIDATION_FAILED` | Message expiry timestamp in the past |
| `PROVIDER_FAILED` | Channel provider API returned error |
| `NO_CHANNEL_ENABLED` | No channel enabled in configuration |
| `INVALID_PAYLOAD` | Payload structure invalid |
| `CMS_DECREMENT_FAILED` | CMS quota decrement failed |
| `PARSE_ERROR` | Message could not be parsed |

---

## 4. uclm-dlr-api-service

> **Role:** Stateless webhook receiver for SMS Delivery Reports. Validates and publishes to Kafka.

```mermaid
flowchart LR
    GW[("SMS Gateway\nAirtel IQ")] -->|"POST /channel/dlr/status"| LB["Load Balancer\nALB / K8s Ingress"]
    LB --> DLR1["DLR API\nInstance 1"]
    LB --> DLR2["DLR API\nInstance 2"]
    LB --> DLR3["DLR API\nInstance 3"]
    DLR1 & DLR2 & DLR3 -->|"Produce\nacks=all\nidempotent"| KAFKA[["iq_channel_dlr_raw"]]
    classDef svc fill:#2980b9,color:#fff,stroke:#1f618d
    classDef ext fill:#8e44ad,color:#fff,stroke:#6c3483
    classDef channel fill:#d35400,color:#fff,stroke:#a04000
    class KAFKA channel
    class GW ext
    class DLR1,DLR2,DLR3 svc
```

| Attribute | Value |
|-----------|-------|
| Port | `8080` |
| Framework | Spring Boot 3.2.0 · Spring Kafka · Jackson |
| Language | Java 17 |
| DB | None |
| Kafka | Produces → `iq_channel_dlr_raw` |
| Reliability | `acks=all`, `enable.idempotence=true`, retries configured |
| Auth | Kerberos GSSAPI (UAT/Prod), plaintext (DEV) |

**Endpoints:**

| Method | Path | Description |
|--------|------|-------------|
| `POST` | `/channel/dlr/status` | Receive DLR webhook from SMS gateway |
| `GET` | `/actuator/health/liveness` | Kubernetes liveness probe |
| `GET` | `/actuator/health/readiness` | Kubernetes readiness probe |
| `GET` | `/actuator/health` | Full health status |

---

## 5. uclm-dlr-aerospike-cache-loader

> **Role:** Stores dispatched message records in Aerospike so DLR Enricher can correlate DLRs.

```mermaid
flowchart LR
    KAFKA[["wa_main_service"]] -->|Kafka consume\nmanual offset| LOADER["Aerospike Cache Loader"]
    LOADER --> VAL["Validate required fields"]
    VAL -->|valid| AS[("Aerospike\nCluster")]
    VAL -->|invalid| DLQ1[["wa_main_service_dlq"]]
    AS -->|write success| ACK["ACK Kafka offset"]
    AS -->|write fail, retry 3×| RETRY["Exponential Backoff\nRetry"]
    RETRY -->|max retries| DLQ1
    AS -->|Aerospike DOWN| NOACK["Don't ACK\n Kafka retries"]
    classDef kafka fill:#f39c12,color:#000,stroke:#d68910
    classDef db fill:#27ae60,color:#fff,stroke:#1e8449
    classDef user fill:#34495e,color:#fff,stroke:#2c3e50
    classDef stop fill:#e74c3c,color:#fff,stroke:#c0392b
    class AS,LOADER db
    class ACK,KAFKA,NOACK,RETRY kafka
    class DLQ1 stop
    class VAL user
```

| Attribute | Value |
|-----------|-------|
| Port | N/A |
| Framework | Spring Boot 3.2.0 · Spring Kafka · Aerospike Client 7.1.0 · Spring Retry |
| Language | Java 17 |
| DB | Aerospike (write) |
| Kafka | Consumes `wa_main_service` → DLQ Producer `wa_main_service_dlq` |
| Retry | 3 attempts with exponential backoff |
| Auth | Kerberos GSSAPI (UAT/Prod) |
| Primary Key | Configurable (`dynamic.primary.key` property) |

---

## 6. uclm-dlr-enricher

> **Role:** Correlates raw DLRs with original dispatch records from Aerospike cache, produces enriched DLRs.

```mermaid
flowchart LR
    KAFKA[["iq_channel_dlr_raw"]] -->|Kafka consume| ENR["DLR Enricher"]
    ENR -->|extract lookup key| KEY["Lookup Key\nrequestId / UUID"]
    KEY -->|query| AS[("Aerospike\nCluster")]
    AS -->|HIT| MERGE["Merge fields\nDLR + dispatch record"]
    MERGE -->|Produce| OUT[["enriched-dlr-topic"]]
    AS -->|MISS| RETRY_T[["dlr-retry-topic\n(15 min delay)"]]
    RETRY_T -->|re-attempt| ENR
    RETRY_T -->|max 3 retries - 15m30m60m| DLQ[["dlr-enricher-dlq"]]
    classDef kafka fill:#f39c12,color:#000,stroke:#d68910
    classDef svc fill:#2980b9,color:#fff,stroke:#1f618d
    classDef db fill:#27ae60,color:#fff,stroke:#1e8449
    classDef channel fill:#d35400,color:#fff,stroke:#a04000
    classDef stop fill:#e74c3c,color:#fff,stroke:#c0392b
    class KAFKA channel
    class AS,KEY db
    class MERGE,OUT,RETRY_T kafka
    class DLQ stop
    class ENR svc
```

| Attribute | Value |
|-----------|-------|
| Port | N/A |
| Framework | Spring Boot 3.2.0 · Spring Kafka · Aerospike Client 7.1.0 · Resilience4j |
| Language | Java 17 |
| DB | Aerospike (read) |
| Kafka | Consumes `iq_channel_dlr_raw`, `dlr-retry-topic` → Produces `enriched-dlr-topic`, DLQ |
| Retry Schedule | 15 min → 30 min → 60 min (exponential backoff, max 3 attempts) |
| Resilience | Circuit Breaker on Aerospike, manual Kafka offset management |
| Auth | Kerberos GSSAPI (UAT/Prod) |

**Field Mapping Strategy:**
- All fields from the original dispatch record are merged into the DLR event
- Configurable overwrite strategy (DLR fields can overwrite or preserve dispatch fields)
- Null/blank fields in Aerospike record are preserved as-is
