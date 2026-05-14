# Service Details — Per-Service Deep Dive

---

## 1. uclm-campaign-manager

> **Role:** Owns all campaign domain data. Central CRUD API. Source of truth for state.

```mermaid
flowchart LR
    UI([User / UI]) -->|REST| CM["Campaign Manager\nPORT: 80"]
    CM --> DB[("Oracle DB\ncampaign_master\ncampaign_details\ngoal · subgoal\ncg · whitelist\nfrequency_capping\ngovernance · exclusion")]

    CM --- API["APIs:\n/api/v1/campaigns (CRUD)\n/publish\n/lifecycle\n/cg-rules\n/exclusion\n/governance\n/goals · /subgoals\n/frequency-capping"]
    classDef svc fill:#2980b9,color:#fff,stroke:#1f618d
    classDef db fill:#27ae60,color:#fff,stroke:#1e8449
    classDef user fill:#34495e,color:#fff,stroke:#2c3e50
    class DB db
    class API,CM svc
    class UI user
```

| Attribute | Value |
|-----------|-------|
| Port | `80` (K8s cluster) |
| Framework | Spring Boot 3.5.6 · Spring Data JPA · Caffeine Cache · Resilience4j 2.3.0 |
| Database | Oracle / MySQL — **owns all tables** |
| Kafka | Producer — `uclm_analytics` (state change events) · `orchestrator.request` (approval email notifications) |
| External Calls | None |
| Special | OpenAPI/Swagger UI enabled |

---

## 2. uclm-campaign-audience-push

> **Role:** Triggered by Airflow DAG every 15 min. Scans for eligible `CampaignDetails`, calls Audience Manager to trigger file creation, and manages `PUBLISHED → AUDIENCE_PUSHED` state transitions.

```mermaid
flowchart TD
    AIRFLOW(["Airflow DAG\naudience_push_trigger_scheduler\n(every 15 min)"])
    UI([Manual Trigger])

    AIRFLOW -->|"POST /audience/trigger\n(no body)"| AP["Audience Push\nPORT: 8095"]
    UI -->|"POST /audience/push\n(campaignId in body)"| AP

    AP -->|"GET tenant timezone"| AUTH[("Auth Manager")]
    AP -->|"Query: scheduleType IN (ONETIME,RECURRING)\nstate=PUBLISHED\nactualScheduleTs within today (tenant TZ)"| DB[("Oracle DB\nCampaignDetails")]
    AP -->|"POST /audience/v1/fetch\n(get CL_ delivery attrs)"| AM[("Audience Manager")]
    AP -->|"POST /push/file/v1/create\n(trigger file creation)"| AM
    AP -->|"Write state changes"| DB

    AP --- STATES["Per-tenant, per-campaign-group:\nPUBLISHED  AUDIENCE_REQUESTED\n AUDIENCE_PUSHED\n(or rollback  PUBLISHED on AM failure)"]
    classDef svc fill:#2980b9,color:#fff,stroke:#1f618d
    classDef db fill:#27ae60,color:#fff,stroke:#1e8449
    classDef ext fill:#8e44ad,color:#fff,stroke:#6c3483
    classDef af fill:#1abc9c,color:#000,stroke:#148f77
    classDef user fill:#34495e,color:#fff,stroke:#2c3e50
    classDef stop fill:#e74c3c,color:#fff,stroke:#c0392b
    class AIRFLOW af
    class DB db
    class AM ext
    class STATES stop
    class AP,AUTH svc
    class UI user
```

| Attribute | Value |
|-----------|-------|
| Port | `8095` |
| Framework | Spring Boot · RestTemplate · Resilience4j · OpenTelemetry |
| Triggered by | Airflow DAG `audience_push_trigger_scheduler` (every 15 min) OR manual `POST /audience/push` |
| Eligible campaigns | `scheduleType IN (ONETIME, RECURRING)` · `state = PUBLISHED` · `actualScheduleTs` within today in tenant timezone |
| Tenant handling | Iterates all configured tenant IDs; each tenant gets its own timezone-aware day window |
| AM calls per campaign | ① `POST /audience/v1/fetch` → extract `CL_` identifiers ② `POST /push/file/v1/create` → trigger file + get `transactionId` |
| On AM failure | Detail states rolled back to `PUBLISHED` → retried on next DAG run |
| CL_* attributes | Extracted from delivery response and added to AM file-create request `targeting` map |

---

## 3. uclm-campaign-processor

> **Role:** Receives async callback from Audience Manager, parses control file, publishes to Kafka.

```mermaid
flowchart LR
    AM[("Audience Manager")] -->|"POST /campaign-processor/\napi/v1/callback"| PROC["Campaign Processor\nPORT: 8080"]
    PROC -->|"GET .ctrl.gz\n(HTTP, timeout 300s)"| AM
    PROC -->|R/W state| DB[("Oracle DB")]
    PROC -->|"Produce: control_file_request"| KAFKA[["Kafka"]]

    PROC --- FLOW["Flow:\n1. Validate callback\n2. Extract audience_id from URL\n3. Lookup campaign by audience_id\n4. Download + decompress .ctrl.gz\n5. Parse: part URLs, delimiters, attrs\n6. Update DB state\n7. Produce to Kafka"]
    classDef kafka fill:#f39c12,color:#000,stroke:#d68910
    classDef svc fill:#2980b9,color:#fff,stroke:#1f618d
    classDef db fill:#27ae60,color:#fff,stroke:#1e8449
    classDef ext fill:#8e44ad,color:#fff,stroke:#6c3483
    class DB,FLOW db
    class AM ext
    class KAFKA kafka
    class PROC svc
```

| Attribute | Value |
|-----------|-------|
| Port | `8080` |
| Framework | Spring Boot 3.5.6 · RestTemplate · Resilience4j · OpenAPI |
| Kafka | Produces → `control_file_request` |
| Max File | 500 MB (configurable) |
| GZIP | Multi-layer decompression supported |
| Security | NONE / KERBEROS / SCRAM / PLAIN |

---

## 4. uclm-campaign-data-file-download

> **Role:** Downloads audience part files in parallel from AM URLs. Stores to local/S3/in-memory.

```mermaid
flowchart LR
    KAFKA[["Kafka\ncontrol_file_request"]] -->|Consume| DFD["Data File Download\nPORT: 8070"]
    AUTH[("Auth Manager")] -->|tenant timezone| DFD
    AM[("Audience Manager")] -->|"GET part file × N\n(parallel, WebClient)"| DFD

    DFD -->|"storage=local"| LOCAL[(" Local FS\n/data/comms_planner/\nAUD_DATA/{date}/")]
    DFD -->|"storage=s3"| S3[(" AWS S3\n{bucket}/AUD_DATA/{date}/")]
    DFD -->|"storage=in-memory"| MEM[(" ConcurrentHashMap\n≤ 200MB")]
    DFD -->|"UPDATE state"| DB[("Oracle DB")]
    classDef kafka fill:#f39c12,color:#000,stroke:#d68910
    classDef svc fill:#2980b9,color:#fff,stroke:#1f618d
    classDef db fill:#27ae60,color:#fff,stroke:#1e8449
    classDef ext fill:#8e44ad,color:#fff,stroke:#6c3483
    classDef user fill:#34495e,color:#fff,stroke:#2c3e50
    class DB,S3 db
    class AM ext
    class KAFKA kafka
    class AUTH,DFD svc
    class n user
```

| Attribute | Value |
|-----------|-------|
| Port | `8070` |
| Framework | Spring Boot **WebFlux** · AWS S3 SDK · Resilience4j |
| Kafka | Consumes ← `control_file_request` · Consumer Group: `control_file_request` |
| Parallel Downloads | Up to 10 threads (configurable) |
| Validation | Samples first 1000 rows per file for column count check |
| Storage Modes | `local` / `s3` / `in-memory` |
| Timeout | 600 minutes (configurable) |

---

## 5. uclm-campaign-manager-event-enrichment

> **Role:** Enriches raw events — fetches audience attributes, resolves dynamic content, checks exclusions.

```mermaid
flowchart TD
    KIN[["Kafka: event-enrichment"]] -->|Consume| EE["Event Enrichment\nPORT: 8091"]

    EE -->|"GET audience attributes"| AM[("Audience Manager")]
    EE -->|"GET campaign metadata"| CM["Campaign Manager"]
    EE -->|"GET template"| CONT[("Content Manager")]
    EE -->|"POST excludecg"| CGX["CG Exclusion"]
    EE -->|"POST exclusionscan"| ESC["Exclusion Scan"]
    EE -->|"READ"| DB[("Oracle DB")]

    EE -->|"Produce enriched events"| KOUT[["Kafka: enriched-events"]]

    EE --- PIPELINE["Pipeline:\n1. Validate event schema\n2. Fetch audience attrs from AM\n3. Resolve {{1}} {{2}} {{3}} params\n4. CG/TG rule check\n5. Exclusion scan\n6. Publish enriched event"]
    classDef kafka fill:#f39c12,color:#000,stroke:#d68910
    classDef svc fill:#2980b9,color:#fff,stroke:#1f618d
    classDef db fill:#27ae60,color:#fff,stroke:#1e8449
    classDef ext fill:#8e44ad,color:#fff,stroke:#6c3483
    class DB db
    class AM ext
    class EE,KIN,KOUT,PIPELINE kafka
    class CGX,CM,CONT,ESC svc
```

| Attribute | Value |
|-----------|-------|
| Port | `8091` |
| Framework | Spring Boot 3.5.6 · OpenFeign · Caffeine · Resilience4j · OpenTelemetry |
| Kafka | Consumes ← `event-enrichment` · Produces → `enriched-events` |
| Dynamic Params | Resolves `{{1}}`, `{{2}}`, `{{3}}` from audience attrs / event data / constants |
| HTTP Client | OpenFeign (declarative) |

---

## 6. uclm-campaign-time-validation

> **Role:** Final 7-stage validation pipeline. Builds channel payloads. Dispatches to channel Kafka topics.

```mermaid
flowchart TD
    KIN[["Kafka: enriched-events"]] -->|Consume| TV["Time Validation\nPORT: 8091"]

    TV -->|Stage 2: CG/TG| CGX["CG Exclusion"]
    TV -->|Stage 3: Exclusion| ESC["Exclusion Scan"]
    TV -->|Stage 5: Template| CONT[("Content Manager")]
    TV -->|Stage: Goals| CM["Campaign Manager"]

    TV -->|"SMS"| K1[["sms_nrt_svc_valgov"]]
    TV -->|"Email"| K2[["eml_nrt_svc_valgov"]]
    TV -->|"WhatsApp"| K3[["wa_nrt_svc_valgov"]]
    TV -->|"RCS"| K4[["rcs_nrt_svc_valgov"]]
    TV -->|"Push"| K5[["push_nrt_svc_valgov"]]
    TV -->|"Analytics"| K6[["comms_analytics_logs"]]

    TV --- STAGES["7-Stage Pipeline:\n1. VALIDATION (field-level)\n2. TGCG (group assignment)\n3. EXCLUSION (filter)\n4. GOVERNANCE (compliance)\n5. TEMPLATE (render)\n6. PAYLOAD (build channel msg)\n7. KAFKA (dispatch)"]
    classDef kafka fill:#f39c12,color:#000,stroke:#d68910
    classDef svc fill:#2980b9,color:#fff,stroke:#1f618d
    classDef channel fill:#d35400,color:#fff,stroke:#a04000
    class K4,STAGES channel
    class KIN kafka
    class CGX,CM,CONT,ESC,K6,TV svc
```

| Attribute | Value |
|-----------|-------|
| Port | `8091` |
| Framework | Spring Boot 3.5.6 · OpenFeign · Virtual Threads (Project Loom) · Resilience4j |
| Kafka | Consumes ← `enriched-events` · Produces → 5 channel topics + analytics |
| Channels | SMS · Email · WhatsApp · RCS · Push |
| Special | RCS supports rich cards, buttons, carousels, deep linking |

---

## 7. uclm-campaign-exclusion-scan

> **Role:** Fast in-memory mobile number exclusion lookup from CSV lists (employee, VIP, retailer).

```mermaid
flowchart LR
    CSV[(" CSV Files\n/data/exclusion/*.csv\nemployee.csv · vip.csv\nretailer.csv")] -->|"Load at startup\nReload daily @ 00:30"| ESC["Exclusion Scan\nPORT: 8080\nConcurrentHashMap"]

    CALLER["Event Enrichment\nTime Validation"] -->|"POST /api/v1/internal/exclusionscan\n{mobile_number, exclusion_type[]}"| ESC
    ESC -->|"{ eligible: false/true }"| CALLER
    ESC -->|"X-Trace-Id header"| CALLER
    classDef kafka fill:#f39c12,color:#000,stroke:#d68910
    classDef svc fill:#2980b9,color:#fff,stroke:#1f618d
    class CALLER kafka
    class ESC svc
```

| Attribute | Value |
|-----------|-------|
| Port | `8080` |
| Framework | Spring Boot 4.0.0 · Jakarta Validation · OpenTelemetry |
| Storage | In-memory `ConcurrentHashMap` — O(1) lookup |
| Reload | Daily at 00:30 AM (no restart needed) |
| Trace | `X-Trace-Id` propagated in response header |

---

## 8. uclm-campaign-cg-exclusion

> **Role:** SpEL-based Control Group / Target Group rule evaluation from database.

```mermaid
flowchart LR
    DB[("Oracle DB\ncg_rules")] --> CGX["CG Exclusion\nPORT: 8080\nSpEL Engine"]

    CALLER["Event Enrichment\nTime Validation"] -->|"POST /api/v1/internal/excludecg\n{mobile_number, cg_group, kpi}"| CGX
    CGX -->|"Evaluate SpEL expression"| CGX
    CGX -->|"{ exclude: true/false }"| CALLER

    CGX --- RULES["SpEL Operators:\n>, <, >=, <=, ==\nand, or, ()\nExample: kpi.value > 500 and kpi.name == 'revenue'"]
    classDef kafka fill:#f39c12,color:#000,stroke:#d68910
    classDef svc fill:#2980b9,color:#fff,stroke:#1f618d
    classDef db fill:#27ae60,color:#fff,stroke:#1e8449
    class DB db
    class CALLER kafka
    class CGX svc
```

| Attribute | Value |
|-----------|-------|
| Port | `8080` |
| Framework | Spring Boot 4.0.1 · Spring Data JPA · SpEL |
| Rule Engine | Spring Expression Language (SpEL) |
| Rules stored | Oracle DB (loaded per request) |

---

## 9. uclm-test-campaign

> **Role:** Identical mirror of `uclm-campaign-time-validation` for pre-production / A-B testing.

```mermaid
flowchart LR
    PROD["Time Validation\n(Production)"] -.->|"identical code"| TC["Test Campaign\n(Pre-prod)"]

    KIN[["Kafka: enriched-events\n(or test topic)"]] -->|Consume| TC
    TC -->|"Produce (test topics)"| KTEST[["Test Channel Topics"]]

    TC --- DIFF["Differences vs Time Validation:\n- Separate K8s deployment\n- May use different Kafka topics\n- Profiles: oracle / prepod\n- Used for feature validation\nwithout affecting production"]
    classDef kafka fill:#f39c12,color:#000,stroke:#d68910
    classDef svc fill:#2980b9,color:#fff,stroke:#1f618d
    classDef db fill:#27ae60,color:#fff,stroke:#1e8449
    classDef channel fill:#d35400,color:#fff,stroke:#a04000
    class KTEST channel
    class DIFF db
    class KIN kafka
    class PROD,TC svc
```

| Attribute | Value |
|-----------|-------|
| Framework | Identical to `uclm-campaign-time-validation` |
| Purpose | Pre-prod validation, A/B testing, safe rollout |
| Config | `application-oracle.yml`, `application-prepod.yml` |
