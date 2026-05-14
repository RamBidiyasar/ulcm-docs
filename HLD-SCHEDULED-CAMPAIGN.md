# HLD — Scheduled Campaign Flow (ONETIME / RECURRING)

**Role:** End-to-end execution path for time-scheduled campaigns — from Airflow DAG trigger through audience file acquisition, CSV streaming, 7-stage per-record validation, and channel dispatch into Bummlebee.

---

## 1. Purpose & Responsibilities

| Responsibility | Detail |
|---|---|
| State lifecycle management | Campaign Manager owns all transitions: `DRAFT → PUBLISHED → IN_PROGRESS → COMPLETED` |
| Airflow-driven triggers | Three DAGs fire every 15 min to advance state and execute campaigns |
| Audience file acquisition | Audience Push calls Audience Manager to create an audience file; Campaign Processor downloads the `.ctrl.gz` control file and extracts part-file URLs |
| Parallel part-file download | Data File Download fetches all audience CSV part-files concurrently (up to 10 threads), validates first 1000 rows, stores to disk / S3 |
| Per-record 7-stage pipeline | Time Validation streams CSVs, runs whitelist → TGCG → exclusion scan → A/B test → template resolve → payload build → Kafka publish per record |
| State machine ownership | Campaign Processor and Data File Download drive sub-states; Time Validation sets `PROCESSING_START → DELIVERED_TO_CHANNEL_PARTNER` |
| Bummlebee handoff | Time Validation publishes `CommonDispatchPayload` to `channel_partner_*_nrt_svc_valgov` — the shared entry into Bummlebee's Rate Controller |
| Analytics | Every record outcome fires to `cs_raw_reporting_topic` (fire-and-forget) |
| Schedule types covered | `ONETIME`, `RECURRING` — EVENT campaigns do **not** use this path |

---

## 2. High-Level Architecture

```mermaid
flowchart TD
    subgraph AF["Apache Airflow — every 15 min"]
        DAG1["DAG 1\ncampaign_state_transition_scheduler"]
        DAG2["DAG 2\naudience_push_trigger_scheduler"]
        DAG3["DAG 3\ncampaign_execution_trigger_scheduler"]
    end

    CM["uclm-campaign-manager\n:80\nPUBLISHED to IN_PROGRESS\nIN_PROGRESS to COMPLETED\nbulk DB UPDATE"]

    AP["uclm-campaign-audience-push\n:8095\n1. Auth Manager timezone\n2. AM /audience/fetch\n3. AM /push/file/create\n4. state to AUDIENCE_PUSHED"]

    PROC["uclm-campaign-processor\n:8080\nPOST /api/v1/callback\ndownload .ctrl.gz\nextract partFileUrls\nUPDATE CONTROL_FILE_DOWNLOADED\nproduce: control_file_request"]

    DFD["uclm-campaign-data-file-download\n:8070\nparallel download, up to 10 threads\nvalidate first 1000 rows\nstore: disk / S3 / in-memory\nUPDATE DATA_FILE_DOWNLOADED"]

    TV["uclm-campaign-time-validation\n:8091\n1. findEligible: ONETIME/RECURRING, DATA_FILE_DOWNLOADED, time-window\n2. UPDATE PROCESSING_START\n3. CampaignBootstrapService: goals, templates, CG rules\n4. CsvStreamService: batch 2000, Semaphore 100\n5. Per-record 7-stage pipeline\n6. UPDATE DELIVERED_TO_CHANNEL_PARTNER"]

    CFK[/"Kafka\ncontrol_file_request"/]
    UKP[/"Kafka\nchannel_partner_nrt_svc_valgov"/]

    subgraph BB["BUMMLEBEE"]
        direction LR
        RC["Rate Controller\n:8081"] --> VG["Validation Governance\n:7777"] --> ORC["Orchestrator"] --> CP(["SMS / Email / WA / RCS / Push"])
    end

    ODB[("Oracle DB")]
    AM(["Audience Manager\naudience file create / fetch"])
    AUTH(["uclm-auth-manager\ntenant timezone"])
    CONT(["uclm-contentmgmt\ntemplates and media"])
    EXSVC(["uclm-campaign-cg-exclusion\nuclm-campaign-exclusion-scan"])

    DAG1 -->|"POST /trigger/state-transition"| CM
    DAG2 -->|"POST /audience/trigger"| AP
    DAG3 -->|"POST /execution/trigger"| TV

    CM <-->|"bulk UPDATE"| ODB

    AP <-->|"tenant timezone"| AUTH
    AP <-->|"audience fetch + file create"| AM
    AP <-->|"UPDATE state"| ODB

    AM -->|"async callback POST /api/v1/callback"| PROC
    PROC <-->|"download .ctrl.gz"| AM
    PROC <-->|"UPDATE state"| ODB
    PROC -->|"produce"| CFK

    CFK -->|"consume"| DFD
    DFD <-->|"download part files"| AM
    DFD <-->|"UPDATE state"| ODB

    TV <-->|"tenant timezone"| AUTH
    TV <-->|"SELECT eligible + UPDATE state"| ODB
    TV <-->|"Feign goals + subgoals"| CM
    TV <-->|"RestTemplate templates"| CONT
    TV <-->|"Feign CG eval + exclusion scan"| EXSVC
    TV -->|"publish per record"| UKP

    UKP --> BB

    classDef kafka fill:#f39c12,color:#000,stroke:#d68910
    classDef svc fill:#2980b9,color:#fff,stroke:#1f618d
    classDef db fill:#27ae60,color:#fff,stroke:#1e8449
    classDef ext fill:#8e44ad,color:#fff,stroke:#6c3483
    classDef af fill:#1abc9c,color:#000,stroke:#148f77

    class UKP,CFK kafka
    class CM,AP,TV,PROC,DFD svc
    class ODB db
    class AM,AUTH,CONT,EXSVC ext
    class DAG1,DAG2,DAG3 af
```

---

## 3. Detailed Processing Flow

### 3a. Campaign Creation → Approval → PUBLISHED

```mermaid
sequenceDiagram
    autonumber
    participant User
    participant CM as uclm-campaign-manager
    participant DB as Oracle DB
    participant K as Kafka

    User->>CM: POST /api/v1/campaigns {name, channel, scheduleType: ONETIME, startTs, endTs}
    CM->>DB: INSERT CAMPAIGN_MASTER (state=DRAFT)
    CM-->>User: 201 {campaignId}

    User->>CM: POST /campaigns/{id}/publish
    CM->>DB: UPDATE state=APPROVAL_PENDING
    CM->>K: Produce → uclm_analytics (campaign_published)
    CM->>K: Produce → orchestrator.request (approval email)

    User->>CM: POST /campaigns/{id}/approvals {action=APPROVE}
    CM->>DB: INSERT CampaignDetails rows (goals, CG config, actualScheduleTs)
    CM->>DB: UPDATE state=PUBLISHED
    CM->>K: Produce → uclm_analytics (campaign_approved)
```

### 3b. Airflow DAG 1 — State Transitions

```mermaid
sequenceDiagram
    autonumber
    participant Airflow
    participant CM as uclm-campaign-manager
    participant DB as Oracle DB

    Airflow->>CM: POST /trigger/state-transition (every 15 min)
    CM->>DB: BATCH UPDATE state=IN_PROGRESS\nWHERE scheduleType!=EVENT AND state=PUBLISHED AND startTs<=now()
    CM->>DB: BATCH UPDATE state=COMPLETED\nWHERE scheduleType!=EVENT AND state=IN_PROGRESS AND endTs<=now()
    CM->>DB: BATCH UPDATE state=IN_PROGRESS\nWHERE scheduleType=EVENT AND state=PUBLISHED (immediate)
    CM-->>Airflow: 200 {markSchedule: N, markCompleted: N, markEvent: N}
```

### 3c. Airflow DAG 2 — Audience Push → Audience Manager → Campaign Processor

```mermaid
sequenceDiagram
    autonumber
    participant Airflow
    participant AP as uclm-campaign-audience-push
    participant AUTH as uclm-auth-manager
    participant DB as Oracle DB
    participant AM as Audience Manager
    participant PROC as uclm-campaign-processor
    participant KAfka as Kafka

    Airflow->>AP: POST /audience/trigger (every 15 min)

    loop For each configured tenantId
        AP->>AUTH: GET /tenant/config/{tenantId} → timezone
        AP->>DB: SELECT CampaignDetails\nWHERE scheduleType IN (ONETIME,RECURRING)\nAND state=PUBLISHED\nAND actualScheduleTs BETWEEN startOfDay AND 23:30 (tenant TZ)

        loop For each eligible campaign group
            AP->>DB: UPDATE CampaignDetails state=AUDIENCE_REQUESTED
            AP->>AM: POST /audience-manager/audience/v1/fetch {campaignId, tenantId}
            AM-->>AP: CL_ delivery attributes
            AP->>AM: POST /audience-manager/push/file/v1/create {campaignId, audienceDef}
            alt AM success
                AM-->>AP: 202 {transactionId}
                AP->>DB: UPDATE state=AUDIENCE_PUSHED, store transactionId
            else AM failure
                AP->>DB: ROLLBACK state=PUBLISHED  (retry next DAG run)
            end
        end
    end

    Note over AM,PROC: Audience Manager prepares files asynchronously
    AM->>PROC: POST /campaign-processor/api/v1/callback {campaignId, ctrlFileUrl}
    PROC->>DB: UPDATE state=CALLBACK_RECEIVED
    PROC->>AM: GET {ctrlFileUrl} → download .ctrl.gz
    PROC->>PROC: Decompress → extract partFileUrls[], delimiter, attributeList
    PROC->>DB: UPDATE state=CONTROL_FILE_DOWNLOADED, save partFileUrls
    PROC->>KAfka: PRODUCE control_file_request {campaignId, partFileUrls, delimiter}
    PROC->>DB: UPDATE state=CONTROL_FILE_PUBLISHED
```

### 3d. Data File Download (Parallel)

```mermaid
sequenceDiagram
    autonumber
    participant Kafka
    participant DFD as uclm-campaign-data-file-download
    participant AUTH as uclm-auth-manager
    participant AM as Audience Manager
    participant DB as Oracle DB

    Kafka->>DFD: CONSUME control_file_request {campaignId, partFileUrls[]}
    DFD->>AUTH: GET /tenant/config/{tenantId} → timezone

    par Download all part files simultaneously (up to 10 threads)
        DFD->>AM: GET partFileUrls[0]
        DFD->>AM: GET partFileUrls[1]
        DFD->>AM: GET partFileUrls[N]
    end

    DFD->>DFD: Validate first 1000 rows per file (column integrity check)
    DFD->>DFD: Store to disk / AWS S3 / in-memory
    DFD->>DB: UPDATE state=DATA_FILE_DOWNLOADED
```

### 3e. Airflow DAG 3 — Time Validation: CSV Streaming + 7-Stage Per-Record Pipeline

```mermaid
sequenceDiagram
    autonumber
    participant Airflow
    participant TV as uclm-campaign-time-validation
    participant DB as Oracle DB
    participant AUTH as uclm-auth-manager
    participant CM as uclm-campaign-manager (Feign)
    participant CONT as uclm-contentmgmt (RestTemplate)
    participant TGCG as uclm-campaign-cg-exclusion
    participant EXCL as uclm-campaign-exclusion-scan
    participant Kafka as Kafka (UnifiedKafkaProducer)
    participant ANA as cs_raw_reporting_topic

    Airflow->>TV: POST /api/v1/campaign/execution/trigger (every 15 min)
    TV-->>Airflow: 204 No Content (async, non-blocking)

    loop For each tenantId
        TV->>AUTH: GET /tenant/config/{tenantId} → timezone
        TV->>DB: SELECT CampaignDetails\nWHERE scheduleType IN (ONETIME,RECURRING)\nAND state=DATA_FILE_DOWNLOADED\nAND actualScheduleTs BETWEEN startOfDay AND now (≤23:30 tenant TZ)
        TV->>DB: UPDATE state=PROCESSING_START

        Note over TV: Bootstrap Phase (once per campaign)
        TV->>CM: Feign GET /goals/{id}, /goals/{id}/subgoals
        TV->>CONT: RestTemplate GET /template/{id} or /bundle/{id} → Caffeine cache
        TV->>DB: SELECT * FROM cg → compile SpEL predicates → Caffeine (LocalTgcgService)

        Note over TV: CSV Streaming Phase
        loop For each CSV file → batches of 2000 records [Semaphore(100)]
            loop parallelStream — each record
                TV->>TV: [1] WhitelistService.isAllowed(mobile/email)
                TV->>TV: [2] LocalTgcgService.evaluate() (Caffeine SpEL, zero I/O)
                TV->>TGCG: [3] POST /api/v1/excludecg (Feign, Resilience4j retry+CB)
                TGCG-->>TV: {exclude: true/false}
                TV->>EXCL: [4] POST /api/v1/exclusionscan (Feign, Resilience4j)
                EXCL-->>TV: {eligible: true/false}
                TV->>TV: [5] A/B split → resolve child CampaignDetails
                TV->>TV: [6] ContentManagerService.buildChannelContent() (from cache)
                TV->>TV: [7] ChannelPayloadFactory.build(channel, record, content)
                TV->>TV: TemplateParamValidator.validate() — check unresolved {{n}} tokens
                TV->>Kafka: UnifiedKafkaProducer.publish(channel, siId, payload) — sync 3 retries
                TV->>ANA: AnalyticsLogPublisher (fire-and-forget)
            end
        end

        alt publishedCount > 0
            TV->>DB: UPDATE state=DELIVERED_TO_CHANNEL_PARTNER
            TV->>TV: cleanupScheduler.schedule(deleteFile, +15 min)
        else publishedCount == 0
            TV->>DB: UPDATE state=FAILED_TO_DELIVER
        end
        TV->>DB: UPDATE master.lastExecutedTimestamp
    end
```

---

## 4. Key Business Logic

### Campaign State Machine

```mermaid
stateDiagram-v2
    classDef init fill:#95a5a6,color:#000,stroke:#7f8c8d
    classDef active fill:#2980b9,color:#fff,stroke:#1f618d
    classDef success fill:#27ae60,color:#fff,stroke:#1e8449
    classDef fail fill:#e74c3c,color:#fff,stroke:#c0392b

    [*] --> DRAFT

    DRAFT --> APPROVAL_PENDING : publish()

    APPROVAL_PENDING --> PUBLISHED : approve
    APPROVAL_PENDING --> REJECTED : reject

    PUBLISHED --> IN_PROGRESS : DAG 1
    PUBLISHED --> AUDIENCE_REQUESTED : DAG 2

    IN_PROGRESS --> COMPLETED : DAG 1 endTs passed

    AUDIENCE_REQUESTED --> PUBLISHED : rollback
    AUDIENCE_REQUESTED --> AUDIENCE_PUSHED : AM success

    AUDIENCE_PUSHED --> CALLBACK_RECEIVED : AM callback

    CALLBACK_RECEIVED --> CONTROL_FILE_DOWNLOADED : ctrl.gz parsed

    CONTROL_FILE_DOWNLOADED --> PUBLISHED : rollback
    CONTROL_FILE_DOWNLOADED --> CONTROL_FILE_PUBLISHED : Kafka publish OK

    CONTROL_FILE_PUBLISHED --> DATA_FILE_DOWNLOADED : all part files downloaded

    DATA_FILE_DOWNLOADED --> PROCESSING_START : DAG 3 trigger

    PROCESSING_START --> DELIVERED_TO_CHANNEL_PARTNER : success
    PROCESSING_START --> FAILED_TO_DELIVER : failure

    COMPLETED --> [*]
    REJECTED --> [*]
    DELIVERED_TO_CHANNEL_PARTNER --> [*]
    FAILED_TO_DELIVER --> [*]

    class DRAFT init
    class APPROVAL_PENDING,PUBLISHED,IN_PROGRESS,AUDIENCE_REQUESTED,AUDIENCE_PUSHED,CALLBACK_RECEIVED,CONTROL_FILE_DOWNLOADED,CONTROL_FILE_PUBLISHED,DATA_FILE_DOWNLOADED,PROCESSING_START active
    class COMPLETED,DELIVERED_TO_CHANNEL_PARTNER success
    class REJECTED,FAILED_TO_DELIVER fail
```

### DAG 2 — Audience Push Eligibility Query

```sql
SELECT cd FROM CampaignDetails cd
JOIN cd.campaignMaster cm
WHERE LOWER(cm.scheduleType) IN ('onetime', 'recurring')
  AND LOWER(cd.state) = 'published'
  AND cd.actualScheduleTs >= :startOfDay       -- 00:00 in tenant TZ
  AND cd.actualScheduleTs < :endOfDay          -- 23:30 in tenant TZ
  AND cm.tenantId = :tenantId
ORDER BY cd.actualScheduleTs ASC
```

### DAG 3 — Time Validation Time-Window

```
startOfDay  = ZonedDateTime.now(tenantTZ).truncatedTo(DAYS)
currentTime = ZonedDateTime.now(tenantTZ)
if currentTime.toLocalTime() > 23:30 → cap at today@23:30

eligible if: startOfDay ≤ actualScheduleTs ≤ currentTime
             AND state = DATA_FILE_DOWNLOADED
             AND scheduleType IN (ONETIME, RECURRING)
```

### LocalTgcgService Cache (Time Validation)

```
Type:    Caffeine (in-process)
Key:     campaignId + cgId
TTL:     loaded once per bootstrap phase (not time-based)
Purpose: zero I/O per-record CG evaluation during CSV streaming
         avoids DB round-trip for every one of potentially millions of records
```

---

## 5. Kafka Topics

| Topic | Producer | Consumer | Payload |
|-------|----------|----------|---------|
| `uclm_analytics` | Campaign Manager | Analytics Reporting | Campaign created / approved events |
| `orchestrator.request` | Campaign Manager | Orchestrator (email) | Approval notification emails |
| `control_file_request` | Campaign Processor | Data File Download | ctrl.gz metadata + part-file URLs |
| `channel_partner_sms_nrt_svc_valgov` | Time Validation | Rate Controller (Bummlebee) | SMS CommonDispatchPayload |
| `channel_partner_wa_nrt_svc_valgov` | Time Validation | Rate Controller (Bummlebee) | WhatsApp CommonDispatchPayload |
| `channel_partner_eml_nrt_svc_valgov` | Time Validation | Rate Controller (Bummlebee) | Email CommonDispatchPayload |
| `channel_partner_push_nrt_svc_valgov` | Time Validation | Rate Controller (Bummlebee) | Push CommonDispatchPayload |
| `channel_partner_rcs_nrt_svc_valgov` | Time Validation | Rate Controller (Bummlebee) | RCS CommonDispatchPayload |
| `cs_raw_reporting_topic` | Time Validation | Analytics Reporting | Per-record outcome analytics event |

---

## 6. External Dependencies

| System | Called By | Method | Purpose |
|--------|-----------|--------|---------|
| Audience Manager | Audience Push, Data File Download | REST / WebClient | Audience file creation, part-file download |
| `uclm-auth-manager` | Audience Push, Data File Download, Time Validation | Feign | Tenant timezone resolution |
| `uclm-contentmgmt` | Time Validation | RestTemplate (cached) | Template / bundle fetch |
| `uclm-campaign-cg-exclusion` | Time Validation | Feign (Resilience4j) | SpEL CG rule evaluation |
| `uclm-campaign-exclusion-scan` | Time Validation | Feign (Resilience4j) | Employee / VIP / retailer exclusion lookup |
| `uclm-campaign-manager` | Time Validation | Feign | Goal + subgoal resolution for governance |
| Apache Airflow | — | HTTP POST (DAGs) | Scheduled trigger source for all three execution phases |
| AWS S3 | Data File Download | SDK | Audience part-file storage (when S3 profile active) |
