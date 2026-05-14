# HLD — NRT Campaign Flow (EVENT / Near Real-Time)

**Role:** Real-time event-triggered campaign execution path — an external system event fires a Kafka message that is immediately consumed by `uclm-campaign-manager-event-enrichment`, which validates, enriches, excludes, and dispatches per-subscriber without any audience file, batch download, or Airflow scheduling.

---

## 1. Purpose & Responsibilities

| Responsibility | Detail |
|---|---|
| Real-time event consumption | `EventProcessingConsumer` listens on `event_enrichment_prod`; processes each message end-to-end in-band |
| Campaign DB validation | Confirms a PUBLISHED or IN_PROGRESS campaign with matching `eventId` exists before any enrichment |
| Subscriber identity resolution | LOB-branched logic maps `si` / `rtn` / `lob` fields to a typed `PrimaryId` (SI or RTN) for Audience Manager lookup |
| Per-subscriber real-time enrichment | Feign call to Audience Manager API per campaign — fetches profile attributes, audience segment IDs, KPI values, language preference |
| Audience membership filter | Subscriber must belong to the campaign's configured `audienceId`; otherwise dropped |
| CG / TG exclusion | `LocalTgcgService` (Caffeine cache, 24h TTL, loaded from Oracle) evaluates SpEL expressions without a DB round-trip per message |
| Exclusion scan | Feign call to `uclm-campaign-exclusion-scan` for EMPLOYEE / VIP / RETAILER list check |
| A/B test split | `AudienceDecisionServiceImpl` applies percentage split from campaign config |
| Content fetch | Feign call to `uclm-contentmgmt` for template or bundle; language-aware |
| Channel payload build | `ChannelPayloadFactory` strategy pattern per channel (SMS / EMAIL / PUSH / WA / RCS) |
| Kafka dispatch | `UnifiedKafkaProducer` publishes `CommonDispatchPayload` to `channel_partner_*_nrt_svc_valgov`; buffered flush at 50; soft-fail |
| Analytics | `AnalyticsLogPublisher` → `cs_raw_reporting_topic` fire-and-forget at every gate |
| Whitelist gate | Optional in-memory `Set<String>` (refreshed on schedule); controlled by `whitelist.enabled` |
| OTel tracing | Correlation ID = OTel `traceId` if available, else random UUID; attached to MDC per message |
| No Airflow DAGs | `EVENT` campaigns transition `PUBLISHED → IN_PROGRESS` immediately via DAG 1 — no audience push, no ctrl file, no DAG 2 or DAG 3 |

---

## 2. High-Level Architecture

```mermaid
flowchart TD
    ESRC(["External Event Source\nnetwork / telco trigger"])
    KIN[/"Kafka\nevent_enrichment_prod"/]

    EPC["EventProcessingConsumer\n@KafkaListener, groupId: event-enrichment-group\nport :8095, context-path: /event-enrichment"]

    subgraph PIPE["Per-Message Pipeline"]
        direction TB
        P0["Step 0  Resolve correlationId — OTel traceId or UUID — into MDC"]
        P1["Step 1  Safety check — skip null / blank / non-JSON"]
        P2["Step 2  Parse JSON — eventId, si, rtn last 10 digits, lob, src, meta"]
        P3["Step 3  Timestamp guard — null start to Instant.now, null expire to 23:59:59, discard if past"]
        P4["Step 4  EventValidationService — DB COUNT check on CAMPAIGN_MASTER"]
        P5["Step 5  CampaignService.getCampaignsByEventId — List of CampaignMaster"]
        P6["Step 6  LOB normalisation — PREPAID / POSTPAID / APB to mobility"]
        P7["Step 7  PrimaryId resolution — LOB-branched SI or RTN"]
        P8["Step 8  AudienceService — POST /audience-manager/user/audiences, Feign, per message"]
        P9["Step 9  WhitelistService gate — optional, if whitelist.enabled=true"]
        P0 --> P1 --> P2 --> P3 --> P4 --> P5 --> P6 --> P7 --> P8 --> P9
    end

    subgraph CEO["CampaignExecutionOrchestrator — per CampaignMaster, async with retry"]
        direction TB
        GS["GovernanceService\nFeign — Goal + SubGoal from Campaign Manager"]
        TGCG["LocalTgcgService\nCaffeine 24h TTL, loaded from Oracle cg table\nSpEL kpi expression evaluated on cache HIT"]
        EXCL["ExclusionClient\nFeign POST /exclusionscan\nEMPLOYEE, VIP, RETAILER CSV lists"]
        AB["AudienceDecisionService\nA/B split"]
        CPF["ChannelPayloadFactory — strategy per channel\nCommonPayloadAssembler — CommonDispatchPayload"]
        GS --> TGCG --> EXCL --> AB --> CPF
    end

    UKP[/"Kafka\nchannel_partner_nrt_svc_valgov"/]
    ALP[/"Kafka\ncs_raw_reporting_topic"/]

    subgraph BB["BUMMLEBEE"]
        direction LR
        RC["Rate Controller\n:8081"] --> VG["Validation Governance\n:7777"] --> ORC["Orchestrator"] --> CP(["SMS / Email / WA / RCS / Push"])
    end

    ODB[("Oracle DB\nCAMPAIGN_MASTER, campaign_details, cg table")]
    AM(["Audience Manager\nPOST /user/audiences\nper subscriber"])
    CONT(["uclm-contentmgmt\nFeign GET template or bundle"])
    ESCAN(["uclm-campaign-exclusion-scan\nFeign POST /exclusionscan"])

    ESRC -->|produce| KIN
    KIN -->|consume| EPC
    EPC --> PIPE
    P4 <-->|JPA| ODB
    P5 <-->|JPA| ODB
    P8 <-->|Feign HTTPS| AM
    PIPE -->|for each CampaignMaster| CEO
    GS <-->|Feign REST| ODB
    TGCG <-.->|cache MISS only| ODB
    EXCL <-->|Feign| ESCAN
    AB <-->|Feign| CONT
    CPF --> UKP
    CEO -->|fire-and-forget| ALP
    UKP --> BB

    classDef kafka fill:#f39c12,color:#000,stroke:#d68910
    classDef svc fill:#2980b9,color:#fff,stroke:#1f618d
    classDef db fill:#27ae60,color:#fff,stroke:#1e8449
    classDef ext fill:#8e44ad,color:#fff,stroke:#6c3483

    class KIN,UKP,ALP kafka
    class EPC,GS,TGCG,EXCL,AB,CPF svc
    class ODB db
    class ESRC,AM,CONT,ESCAN ext
```

---

## 3. Detailed Processing Flow

### 3a. Full Per-Message Pipeline

```mermaid
sequenceDiagram
    autonumber
    participant ESRC as External Event Source
    participant KIN as Kafka (event_enrichment_prod)
    participant EPC as EventProcessingConsumer
    participant EVS as EventValidationService
    participant CS as CampaignService
    participant AS as AudienceService
    participant AM as Audience Manager API
    participant CEO as CampaignExecutionOrchestrator
    participant GS as GovernanceService
    participant TGCG as LocalTgcgService (Caffeine)
    participant EXCL as ExclusionClient (Feign)
    participant CONT as uclm-contentmgmt
    participant CPF as ChannelPayloadFactory
    participant UKP as UnifiedKafkaProducer
    participant ALP as AnalyticsLogPublisher
    participant DB as Oracle DB

    ESRC->>KIN: Produce event JSON\n{eventId, si, rtn, lob, src, meta,\nevent_start_timestamp, event_expire_timestamp}

    KIN->>EPC: CONSUME raw string

    EPC->>EPC: Step 0 — correlationId (OTel traceId or UUID → MDC)
    EPC->>EPC: Step 1 — skip null / blank / non-JSON
    EPC->>EPC: Step 2 — parse: eventId, si, rtn(last 10), lob, src, meta
    EPC->>EPC: Step 3 — timestamp guard\n(null start→now, null expire→23:59:59, discard if expired)

    EPC->>EVS: Step 4 — validateEvent(eventId)
    EVS->>DB: SELECT COUNT(*) FROM CAMPAIGN_MASTER\nWHERE EVENT_ID=? AND UPPER(STATE) IN ('PUBLISHED','IN_PROGRESS')
    alt count == 0
        EVS-->>EPC: invalid
        EPC->>ALP: publishAnalytics(DROPPED, NO_CAMPAIGN)
    else count > 0
        EVS-->>EPC: valid
    end

    EPC->>CS: Step 5 — getCampaignsByEventId(eventId)
    CS->>DB: SELECT * FROM CAMPAIGN_MASTER WHERE EVENT_ID=? AND STATE IN (...)
    CS-->>EPC: List<CampaignMaster>

    EPC->>EPC: Step 6 — LOB normalisation\nMOBILITY/PREPAID/POSTPAID/APB → "mobility"
    EPC->>EPC: Step 7 — PrimaryId resolution (LOB-branched, §3b)
    EPC->>EPC: Step 8 — build base AudienceRecord\n(mobileNumber, siId, si_lob, timestamps, circleId, meta attrs)

    loop For each CampaignMaster
        EPC->>EPC: clone base AudienceRecord (prevent cross-campaign leakage)
        EPC->>AS: sendAudienceData(primaryIds, tenantId, workspaceId)
        AS->>AM: POST /audience-manager/user/audiences\n{primaryId, type, lob, source=CLM_PUSH}
        AM-->>AS: {profile, audienceSegments[], kpiValues{}, languagePreference}
        AS-->>EPC: enriched AudienceData
        EPC->>EPC: Populate AudienceRecord (attributes, segments, kpi, language)
        EPC->>EPC: Filter: subscriber NOT in campaign.audienceId → SKIP

        EPC->>CEO: execute(campaignId, audienceRecord)

        CEO->>GS: resolveGoals(campaignMaster)
        GS->>DB: Feign GET /campaign-manager/goals/{id}
        GS->>DB: Feign GET /campaign-manager/goals/{id}/subgoals
        GS-->>CEO: GovernanceContext

        CEO->>TGCG: LocalTgcgService.check(campaignId, cgId, kpiValues)
        TGCG->>DB: JPA SELECT FROM cg (on cache MISS only, Caffeine 24h TTL)
        TGCG->>TGCG: evaluate SpEL expression (zero I/O on cache HIT)
        alt CG EXCLUDE
            TGCG-->>CEO: EXCLUDE
            CEO->>ALP: publishAnalytics(EXCLUDED_CG) → cs_raw_reporting_topic
        else CG INCLUDE
            TGCG-->>CEO: INCLUDE

            CEO->>EXCL: Feign POST /api/v1/internal/exclusionscan\n{mobile, exclusionType}
            EXCL-->>CEO: {eligible: true/false}
            alt not eligible
                CEO->>ALP: publishAnalytics(EXCLUDED_SCAN)
            else eligible
                CEO->>CEO: AudienceDecisionService A/B split
                alt A/B EXCLUDE
                    CEO->>ALP: publishAnalytics(EXCLUDED_AB)
                else A/B INCLUDE
                    CEO->>CONT: Feign GET /content-manager/api/v1/template/{id}\n?language={languagePreference}
                    CONT-->>CEO: ContentPayload{message, params, senderConfig}

                    CEO->>CPF: ChannelPayloadFactory.get(channel).build(ChannelContext)
                    CPF-->>CEO: CommonDispatchPayload

                    CEO->>UKP: publish(channel, siId, payload)\n[buffer flush at 50, soft-fail]
                    UKP->>KIN: PRODUCE channel_partner_{channel}_nrt_svc_valgov

                    CEO->>ALP: publishAnalytics(DISPATCHED) → cs_raw_reporting_topic
                end
            end
        end
    end
```

### 3b. LOB-based PrimaryId Resolution

```mermaid
flowchart TD
    START(["Event arrives\n{si, rtn, lob}"])
    NORM["UPPER(lob)"]
    START --> NORM

    NORM --> LOB{lob?}

    LOB -->|"MOBILITY / PREPAID\nPOSTPAID / APB\nnull / empty"| MOB{si present?}
    MOB -->|"length 12\nstarts with 91"| A["type=SI, id=si"]
    MOB -->|"length 10"| B["type=SI, id='91'+si"]
    MOB -->|"si absent, rtn present"| C["type=RTN, id=last-10(rtn)"]
    MOB -->|"neither"| R1(["REJECT"])

    LOB -->|"BROADBAND"| BB["type=RTN, lob=telemedia\nuse both si + rtn"]

    LOB -->|"FLVOICE / FLDSL\nTELEMEDIA"| FL{si present?}
    FL -->|yes| D["type=SI, productCode=lob"]
    FL -->|no| E["type=RTN, lob=telemedia"]

    LOB -->|"DTH"| DTH{si present?}
    DTH -->|yes| F["type=SI, productCode=DTH"]
    DTH -->|no| G["type=RTN, lob=telemedia"]

    LOB -->|"unrecognised"| R2(["REJECT"])

    classDef ok fill:#27ae60,color:#fff,stroke:#1e8449
    classDef reject fill:#e74c3c,color:#fff,stroke:#c0392b
    class A,B,C,BB,D,E,F,G ok
    class R1,R2 reject
```

---

## 4. Key Business Logic

### Per-Campaign Execution Stages

```mermaid
flowchart TD
    START(["clone AudienceRecord"])

    S0["Stage 0 — TENANT CONTEXT\nset TenantContextHolder"]

    S1["Stage 1 — GOVERNANCE\nGovernanceServiceImpl\nFeign GET goals + subgoals from Campaign Manager"]

    S2["Stage 2 — CG / TG EXCLUSION\nLocalTgcgService\nCaffeine 24h TTL, max 10000 entries\nOn cache MISS: JPA SELECT FROM cg table\nSpEL kpi expression evaluated on cache HIT"]

    S2_STOP(["STOP\nanalytics: EXCLUDED_CG"])

    S3["Stage 3 — EXCLUSION SCAN\nExclusionClient, Feign + Resilience4j retry/CB\nPOST /api/v1/internal/exclusionscan\nChecks: EMPLOYEE, VIP, RETAILER CSV lists"]

    S3_STOP(["STOP\nanalytics: EXCLUDED_SCAN"])

    S4["Stage 4 — A/B TEST\nAudienceDecisionServiceImpl\nApply campaign A/B percentage split"]

    S4_STOP(["STOP\nanalytics: EXCLUDED_AB"])

    S5["Stage 5 — LANGUAGE RESOLUTION\nCampaignDetails.language\nor AudienceRecord.languagePreference\nFallback: ENGLISH"]

    S6["Stage 6 — CONTENT FETCH\nContentManagerServiceImpl, Feign\nGET /content-manager/api/v1/template/id?lang=X\nor GET /bundle/bundleId"]

    S7["Stage 7 — CHANNEL PAYLOAD BUILD\nChannelPayloadFactory, strategy pattern\nCommonPayloadAssembler\nCommonDispatchPayload"]

    S8["Stage 8 — DISPATCH\nUnifiedKafkaProducer.publish(channel, siId, payload)\nBuffer flush at 50, soft-fail\nProduces: channel_partner_nrt_svc_valgov\nAnalyticsLogPublisher: cs_raw_reporting_topic"]

    START --> S0 --> S1 --> S2
    S2 -->|EXCLUDE| S2_STOP
    S2 -->|INCLUDE| S3
    S3 -->|not eligible| S3_STOP
    S3 -->|eligible| S4
    S4 -->|EXCLUDE_AB| S4_STOP
    S4 -->|INCLUDE| S5
    S5 --> S6 --> S7 --> S8

    classDef stop fill:#e74c3c,color:#fff,stroke:#c0392b
    classDef stage fill:#2980b9,color:#fff,stroke:#1f618d
    classDef startend fill:#27ae60,color:#fff,stroke:#1e8449

    class S2_STOP,S3_STOP,S4_STOP stop
    class S0,S1,S2,S3,S4,S5,S6,S7,S8 stage
    class START startend
```

### LocalTgcgService Cache Design

```
Cache type:     Caffeine (in-process, JVM-local)
Maximum size:   10,000 entries
TTL:            expireAfterWrite = 24 hours
Cache key:      campaignId + cgId
On cache MISS:  JPA SELECT FROM Oracle cg table
On cache HIT:   SpEL expression evaluated with zero I/O
                (critical: EVENT campaigns can receive thousands of events/minute)
Remote TGCG:    uclm-campaign-cg-exclusion service is NOT called in the NRT flow
                LocalTgcgService replaces it entirely for performance
```

### Whitelist Gate (when enabled)

```
whitelist.enabled = true  (production env)
WhitelistService holds Set<String> of allowed si / rtn values
Refreshed on configurable schedule (e.g. daily)
Subscriber NOT in set → DISCARD before enrichment (before AM API call)
```

### Kafka Producer Buffer

```
UnifiedKafkaProducer internal buffer: List<CommonDispatchPayload>
Flush condition: buffer.size() >= 50 OR end of campaign loop
Failure handling: soft-fail — error is logged, flow continues
                  never blocks or fails the Kafka consumer
```

---

## 5. Campaign State for EVENT Type

EVENT campaigns use a simpler state path — no audience file states:

```mermaid
stateDiagram-v2
    classDef init fill:#95a5a6,color:#000,stroke:#7f8c8d
    classDef active fill:#2980b9,color:#fff,stroke:#1f618d
    classDef running fill:#f39c12,color:#000,stroke:#d68910
    classDef terminal fill:#e74c3c,color:#fff,stroke:#c0392b

    [*] --> DRAFT

    DRAFT --> APPROVAL_PENDING : publish()

    APPROVAL_PENDING --> PUBLISHED : approve

    PUBLISHED --> IN_PROGRESS : DAG 1, immediate, no startTs check

    IN_PROGRESS --> KILLED : Manual kill

    KILLED --> [*]

    note right of IN_PROGRESS
        EVENT campaigns stay IN_PROGRESS until manually KILLED.
        No COMPLETED transition and no endTs check.
        States AUDIENCE_REQUESTED, AUDIENCE_PUSHED,
        CONTROL_FILE_*, DATA_FILE_DOWNLOADED and
        PROCESSING_START do not apply to EVENT campaigns.
    end note

    class DRAFT init
    class APPROVAL_PENDING,PUBLISHED active
    class IN_PROGRESS running
    class KILLED terminal
```

---

## 6. Kafka Topics

| Topic | Direction | Consumer Group | Payload |
|-------|-----------|---------------|---------|
| `event_enrichment_prod` | CONSUME | `event-enrichment-group` | Raw trigger event JSON (`event_enrichment` in DEV) |
| `channel_partner_sms_nrt_svc_valgov` | PRODUCE | Rate Controller (Bummlebee) | SMS `CommonDispatchPayload` |
| `channel_partner_wa_nrt_svc_valgov` | PRODUCE | Rate Controller (Bummlebee) | WhatsApp `CommonDispatchPayload` |
| `channel_partner_eml_nrt_svc_valgov` | PRODUCE | Rate Controller (Bummlebee) | Email `CommonDispatchPayload` |
| `channel_partner_push_nrt_svc_valgov` | PRODUCE | Rate Controller (Bummlebee) | Push `CommonDispatchPayload` |
| `channel_partner_rcs_nrt_svc_valgov` | PRODUCE | Rate Controller (Bummlebee) | RCS `CommonDispatchPayload` |
| `cs_raw_reporting_topic` | PRODUCE | Analytics Reporting | `AnalyticsLogEvent` (fire-and-forget per gate) |

---

## 7. External Dependencies

| System | Type | Base URL | Purpose |
|--------|------|----------|---------|
| Audience Manager | REST / Feign HTTPS | `https://uclm-audience-manager.apps...` | Per-subscriber profile enrichment + segment membership |
| `uclm-contentmgmt` | REST / Feign HTTP | `http://contentmanager-deployment...:7002/content-manager` | Template / bundle fetch per campaign |
| `uclm-campaign-manager` | REST / Feign HTTP | `http://campaign-manager-uclm...:80` | Goal + SubGoal resolution for governance |
| `uclm-campaign-exclusion-scan` | REST / Feign HTTP | `http://exclusion-scan-service...:8080` | Employee / VIP / retailer exclusion check |
| Oracle DB | JPA / JDBC | `jdbc:oracle:thin:@...` | Campaign master, details, CG rules, whitelist |
| Kafka Cluster | SASL_PLAINTEXT (Kerberos GSSAPI) | `10.92.36.48:9092,...` (dev) | Inbound events + outbound dispatch + analytics |

---

## 8. Configuration Reference

| Property | Default / Dev Value | Description |
|----------|---------------------|-------------|
| `server.port` | `8095` | Service HTTP port |
| `server.servlet.context-path` | `/event-enrichment` | Context path |
| `spring.kafka.consumer.group-id` | `event-enrichment-group` | Kafka consumer group |
| `kafka.topics.inbound` | `event_enrichment` (dev) / `event_enrichment_prod` (prod) | Topic consumed |
| `kafka.campaign.topics.SMS` | `channel_partner_sms_nrt_svc_valgov` | SMS outbound topic |
| `kafka.campaign.topics.WHATSAPP` | `channel_partner_wa_nrt_svc_valgov` | WA outbound topic |
| `kafka.campaign.topics.EMAIL` | `channel_partner_eml_nrt_svc_valgov` | Email outbound topic |
| `kafka.campaign.topics.PUSH` | `channel_partner_push_nrt_svc_valgov` | Push outbound topic |
| `kafka.campaign.topics.RCS` | `channel_partner_rcs_nrt_svc_valgov` | RCS outbound topic |
| `kafka.logs.analytics-topic` | `cs_raw_reporting_topic` | Analytics topic |
| `whitelist.enabled` | `false` | Enable subscriber whitelist gate |
| `audience.bb_to_comms` | `N` | Enable BROADBAND LOB processing when `Y` |
| `resilience4j.retry.maxAttempts` | `3` | Retry attempts for downstream Feign calls |
| `resilience4j.retry.waitDuration` | `500ms` | Wait between retries |
