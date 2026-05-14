# UCLM Platform — Detailed End-to-End Flow

> This document traces every user action and system event through all 18 services, from the first login to message delivery and analytics.  
> Read in conjunction with `HLD-FULL-SYSTEM.md` for architecture context.  
> Last updated: 2026-05-13

---

## Table of Contents

1. [Flow Overview](#1-flow-overview)
2. [Phase 0 — Authentication & Identity](#2-phase-0--authentication--identity)
3. [Phase 1 — Content Setup](#3-phase-1--content-setup)
4. [Phase 2 — Campaign Creation & Approval](#4-phase-2--campaign-creation--approval)
5. [Phase 3 — Audience Acquisition](#5-phase-3--audience-acquisition)
6. [Phase 4 — Event Enrichment](#6-phase-4--event-enrichment)
7. [Phase 5 — Time Validation & Channel Dispatch](#7-phase-5--time-validation--channel-dispatch)
8. [Phase 6 — Bummlebee Delivery Pipeline](#8-phase-6--bummlebee-delivery-pipeline)
9. [Phase 7 — DLR (Delivery Report) Tracking](#9-phase-7--dlr-delivery-report-tracking)
10. [Phase 8 — Analytics & Reporting](#10-phase-8--analytics--reporting)
11. [Campaign State Machine Reference](#11-campaign-state-machine-reference)
12. [Kafka Topic Reference](#12-kafka-topic-reference)
13. [Error & Compensation Flows](#13-error--compensation-flows)
14. [IAM Header Contract](#14-iam-header-contract)

---

## 1. Flow Overview

```mermaid
flowchart TD
    P0["Phase 0\nAuth & Identity\n(SAML SSO → JWT)"]
    P1["Phase 1\nContent Setup\n(Templates & Media)"]
    P2["Phase 2\nCampaign Creation\n(CRUD + Approval)"]
    P3["Phase 3\nAudience Acquisition\n(AM → ctrl file → part files)"]
    P4["Phase 4\nEvent Enrichment\n(Enrich per subscriber)"]
    P5["Phase 5\nTime Validation\n(7-Stage → Channel Kafka)"]
    P6["Phase 6\nBummlebee Delivery\n(Rate → Govern → Dispatch)"]
    P7["Phase 7\nDLR Tracking\n(Webhook → Enrich → Publish)"]
    P8["Phase 8\nAnalytics\n(Druid + Hazelcast)"]

    P0 --> P1 --> P2 --> P3 --> P4 --> P5 --> P6 --> P7 --> P8
    P2 -.->|"parallel dimension events"| P8

    classDef auth fill:#34495e,color:#fff,stroke:#2c3e50
    classDef setup fill:#2980b9,color:#fff,stroke:#1f618d
    classDef exec fill:#f39c12,color:#000,stroke:#d68910
    classDef delivery fill:#e74c3c,color:#fff,stroke:#c0392b
    classDef reporting fill:#27ae60,color:#fff,stroke:#1e8449

    class P0 auth
    class P1,P2 setup
    class P3,P4,P5 exec
    class P6 delivery
    class P7,P8 reporting
```

---

## 2. Phase 0 — Authentication & Identity

**Services involved:** `uclm-auth-manager`  
**Databases:** Oracle DB, Aerospike  
**External:** SAML IdP (AD FS)

### 2.1 SSO Login Sequence

```mermaid
sequenceDiagram
    autonumber
    participant Browser
    participant AuthMgr as uclm-auth-manager
    participant Oracle as Oracle DB
    participant Aerospike
    participant IdP as SAML IdP (AD FS)
    participant UI as Frontend UI

    Browser->>AuthMgr: POST /auth-manager/api/v1/ssoLogin\n{userName: "user@corp.com"}
    AuthMgr->>Oracle: findByUsername("user@corp.com") → User{tenantId}
    Oracle-->>AuthMgr: User entity
    AuthMgr->>Oracle: findByTenantId(tenantId) → TenantConfig{SAML config}
    Oracle-->>AuthMgr: loginUrl, entityId, acsUrl, certificate
    AuthMgr->>AuthMgr: Build OpenSAML AuthnRequest\nDeflate → Base64 → URLEncode
    AuthMgr-->>Browser: {ssoRedirectUrl: "https://idp.corp.com/saml?SAMLRequest=..."}

    Note over Browser,IdP: ── Corporate authentication ──
    Browser->>IdP: Navigate to ssoRedirectUrl
    IdP->>Browser: Present login form
    Browser->>IdP: Submit credentials
    IdP->>IdP: Validate + build SAML Assertion
    IdP-->>Browser: HTTP POST form → ACS URL (POST SAMLResponse)

    Browser->>AuthMgr: POST /auth-manager/api/v1/user/saml/response\n(form: SAMLResponse=base64...)
    AuthMgr->>AuthMgr: Decode SAMLResponse (OpenSAML)\nExtract email from assertion attribute
    AuthMgr->>Oracle: findByUsername(email) → User{tenantId, userId}
    AuthMgr->>Oracle: findByTenantId(tenantId) → TenantConfig (validate assertion)
    AuthMgr->>AuthMgr: validateSAMLLoginResponse()\nCheck signature, timing, audience, conditions
    AuthMgr->>Oracle: findById(tenantId) → OrgParentHierarchy root node
    AuthMgr->>AuthMgr: generateToken(user, orgNode, accessList, sessionId)\nHS256 JWT claims: sub, x-user-id, x-tenant-id,\nx-workspace-id, x-user-hierarchy, x-session-id, roles
    AuthMgr->>Aerospike: put(key=username, TTL=300s)\nbins: user_id, session_id, idp_ses_idx, created_on, expires_on
    AuthMgr-->>Browser: HTTP 302 → uiRedirectUrl?loginResponse=base64JWT

    Note over Browser,UI: ── Authenticated session begins ──
    Browser->>UI: Navigate with JWT in query param
    UI->>UI: Parse JWT → extract IAM headers\nStore token for all subsequent API calls
```

### 2.2 Workspace Switch

```mermaid
sequenceDiagram
    participant Client
    participant AuthMgr as uclm-auth-manager
    participant Oracle as Oracle DB

    Client->>AuthMgr: POST /auth-manager/api/v1/switch/workspace?workspaceId=7\nAuthorization: Bearer <existing-JWT>
    AuthMgr->>AuthMgr: Extract username, tenantId, currentWorkspaceId from JWT
    alt currentWorkspaceId == 7
        AuthMgr-->>Client: {accessToken: <same JWT>}
    else switching to new workspace
        AuthMgr->>Oracle: findByUser_UsernameAndNode_IdAndTenantId(username, 7, tenantId)
        Oracle-->>AuthMgr: UserAccess{role, node}
        AuthMgr->>Oracle: findById(tenantId) → OrgParentHierarchy root
        Oracle-->>AuthMgr: root org node
        AuthMgr->>AuthMgr: generateToken(user, rootNode, newAccess, existingSessionId)
        AuthMgr-->>Client: {accessToken: <new JWT scoped to workspaceId=7>}
    end
```

---

## 3. Phase 1 — Content Setup

**Services involved:** `uclm-contentmgmt`  
**Databases:** MongoDB, GCS/S3  
**External:** WhatsApp IQ Platform, RCS IQ Platform, Kafka

### Template Lifecycle

```mermaid
sequenceDiagram
    autonumber
    participant UI as Frontend UI
    participant Content as uclm-contentmgmt
    participant MongoDB
    participant Storage as GCS / S3
    participant ExtPlatform as WhatsApp / RCS IQ

    UI->>Content: POST /content-manager/api/v1/template\nHeaders: x-tenant-id, x-workspace-id, x-user-id\nBody: {templateName, channel: "SMS", message, ...}
    Content->>Content: AppFilter: validate IAM headers, set RequestContext
    Content->>Content: Channel-specific validator\nNormalize params, set DLT fields for SMS\nSet state=L1_PENDING, variant="v1"
    Content->>MongoDB: save(TemplateDAO{templateId, state=L1_PENDING})
    Content-->>UI: TemplateDAO

    Note over UI,Content: ── L1 Approval ──
    UI->>Content: PUT /template/approve/l1/{templateId}
    Content->>Content: WorkflowEngine: L1_PENDING → L2_PENDING
    Content->>MongoDB: update state=L2_PENDING
    Content-->>UI: Updated TemplateDAO

    Note over UI,Content: ── L2 Approval ──
    UI->>Content: PUT /template/approve/l2/{templateId}
    Content->>Content: WorkflowEngine: L2_PENDING → CHANNEL_REG_PENDING\n(for WA/RCS) or APPROVED (for SMS/Email/Push)
    Content->>MongoDB: update state

    alt Channel is WhatsApp or RCS
        Content->>ExtPlatform: POST /register-template
        ExtPlatform-->>Content: {registrationId}
        Content->>MongoDB: update state=CHANNEL_REG_SUBMITTED
        Note over Content,ExtPlatform: Polling until APPROVED or REJECTED
        Content->>ExtPlatform: GET /template-status/{registrationId}
        ExtPlatform-->>Content: {status: APPROVED}
        Content->>MongoDB: update state=APPROVED
    end
    Content-->>UI: Updated TemplateDAO (APPROVED)

    Note over UI,Content: ── Media Upload ──
    UI->>Content: POST /content-manager/api/v1/media\nBody: {file, channel, mediaType}
    Content->>Storage: Upload file to GCS/S3
    Storage-->>Content: {mediaUrl, objectKey}
    Content->>MongoDB: save(MediaDAO{mediaId, state=L1_PENDING, url})
    Content-->>UI: MediaDAO
```

---

## 4. Phase 2 — Campaign Creation & Approval

**Services involved:** `uclm-campaign-manager`  
**Databases:** Oracle/MySQL  
**External:** Apache Airflow (DAGs), SMTP

```mermaid
sequenceDiagram
    autonumber
    participant User
    participant CM as uclm-campaign-manager
    participant Oracle as Oracle / MySQL DB
    participant Airflow as Apache Airflow

    User->>CM: POST /api/v1/campaigns\n{name, channel, startTimestamp, endTimestamp,\ngoals, subgoals, controlGroup, ...}
    CM->>Oracle: INSERT campaign(state=DRAFT, tenantId, workspaceId)
    CM-->>User: Campaign{campaignId, state=DRAFT}

    User->>CM: PUT /api/v1/campaigns/{id}/submit
    CM->>Oracle: UPDATE state=APPROVAL_PENDING
    CM-->>User: state=APPROVAL_PENDING

    User->>CM: PUT /api/v1/campaigns/{id}/approve
    CM->>Oracle: INSERT CampaignDetails rows\n(goals, subgoals, CG config)
    CM->>Oracle: UPDATE state=PUBLISHED
    CM-->>User: state=PUBLISHED

    Note over Airflow,CM: ── Airflow DAGs run every 15 min ──
    Airflow->>CM: Trigger campaign_state_transition_scheduler DAG
    CM->>Oracle: SELECT campaigns WHERE state=PUBLISHED AND startTimestamp <= now()
    CM->>Oracle: UPDATE state=IN_PROGRESS (ONETIME/RECURRING campaigns)
    CM->>Oracle: UPDATE state=IN_PROGRESS immediately (EVENT campaigns)

    Airflow->>CM: Trigger campaign_state_transition_scheduler DAG (later)
    CM->>Oracle: SELECT campaigns WHERE state=IN_PROGRESS AND endTimestamp <= now()
    CM->>Oracle: UPDATE state=COMPLETED (ONETIME/RECURRING only)
    Note over CM: EVENT campaigns must be manually KILLED
```

---

## 5. Phase 3 — Audience Acquisition

**Services involved:** `uclm-campaign-audience-push`, `uclm-campaign-processor`, `uclm-campaign-data-file-download`  
**External:** Audience Manager (AM), Auth Manager, AWS S3

```mermaid
sequenceDiagram
    autonumber
    participant Airflow as Apache Airflow
    participant AP as uclm-campaign-audience-push
    participant AUTH as uclm-auth-manager
    participant AM as Audience Manager (External)
    participant CM as uclm-campaign-manager
    participant Oracle as Oracle / MySQL
    participant PROC as uclm-campaign-processor
    participant DFD as uclm-campaign-data-file-download
    participant Kafka

    Note over Airflow,AP: ── audience_push_trigger_scheduler DAG ──
    Airflow->>AP: Trigger scheduler
    AP->>Oracle: SELECT campaigns WHERE state=PUBLISHED (per tenant)
    AP->>AUTH: GET /tenant/config/{tenantId} → timezone
    AP->>AM: POST /audience-manager/audience/v1/fetch\n(get audience definition)
    AP->>AM: POST /audience-manager/push/file/v1/create\n{campaignId, tenantId, audienceDefinition}
    AM-->>AP: 202 Accepted {requestId}
    AP->>CM: PUT state=AUDIENCE_REQUESTED
    AP->>CM: PUT state=AUDIENCE_PUSHED

    Note over AM,PROC: ── AM asynchronously prepares files, then sends callback ──
    AM->>PROC: POST /campaign-processor/api/v1/callback\n{campaignId, ctrlFileUrl, ...}
    PROC->>CM: PUT state=CALLBACK_RECEIVED
    PROC->>AM: GET {ctrlFileUrl} → download .ctrl.gz
    PROC->>PROC: Decompress .ctrl.gz\nExtract: partFileUrls[], delimiter, attributeList, recordCount
    PROC->>CM: PUT state=CONTROL_FILE_DOWNLOADED\n(save partFileUrls to DB)
    PROC->>Kafka: PRODUCE control_file_request\n{campaignId, partFileUrls, delimiter, attributeList}
    PROC->>CM: PUT state=CONTROL_FILE_PUBLISHED

    Note over DFD,AM: ── Parallel part file download (up to 10 threads) ──
    Kafka->>DFD: CONSUME control_file_request
    DFD->>AUTH: GET /tenant/config/{tenantId} → timezone
    loop Each part file URL (parallel)
        DFD->>AM: GET {partFileUrl}
        AM-->>DFD: Part file (CSV/TSV rows)
        DFD->>DFD: Validate first 1000 rows (column integrity check)
        DFD->>DFD: Store to disk / S3 / in-memory
    end
    DFD->>CM: PUT state=DATA_FILE_DOWNLOADED
    DFD->>Kafka: PRODUCE event-enrichment\n{campaignId, fileLocation, partFiles[]}
```

---

## 6. Phase 4 — Event Enrichment

**Services involved:** `uclm-campaign-manager-event-enrichment`, `uclm-campaign-cg-exclusion`, `uclm-campaign-exclusion-scan`, `uclm-contentmgmt`

```mermaid
sequenceDiagram
    autonumber
    participant Kafka
    participant EE as uclm-campaign-manager-\nevent-enrichment
    participant AM as Audience Manager
    participant CM as uclm-campaign-manager
    participant CONT as uclm-contentmgmt
    participant CGX as uclm-campaign-cg-exclusion
    participant ESC as uclm-campaign-exclusion-scan

    Kafka->>EE: CONSUME event-enrichment\n{campaignId, subscriberRecord}
    Note over EE: For each subscriber record in the audience file:

    EE->>AM: GET /audience-manager/audience/v1/attributes\n{subscriberId, tenantId}
    AM-->>EE: {attr1: val1, attr2: val2, ...}

    EE->>CM: GET /campaigns/{campaignId} → CampaignDTO\n(metadata, goals, subgoals, CG config)
    CM-->>EE: CampaignDTO

    EE->>CONT: GET /content-manager/api/v1/template/{templateId}
    CONT-->>EE: TemplateDAO{message, params[]}

    EE->>EE: Resolve dynamic params {{1}}, {{2}}, {{3}}\nfrom audience attributes into template message

    EE->>CGX: POST /api/v1/excludecg\n{campaignId, tenantId, kpiValues{}}
    CGX->>CGX: Load SpEL rules from Oracle (cg_rules table)\nEvaluate: kpi.value > 500 and kpi.name == 'revenue'
    CGX-->>EE: {exclude: true/false, groupType: "CG"/"TG"}

    alt excluded by CG rule
        EE->>Kafka: PRODUCE analytics event (excluded)
    else not excluded
        EE->>ESC: POST /api/v1/exclusionscan\n{mobile, exclusionType: "EMPLOYEE/VIP/RETAILER"}
        ESC->>ESC: O(1) lookup in ConcurrentHashMap (loaded from CSV at startup)
        ESC-->>EE: {eligible: true/false}

        alt excluded by CSV scan
            EE->>Kafka: PRODUCE analytics event (excluded)
        else eligible
            EE->>Kafka: PRODUCE enriched-events\n{enrichedEvent with resolved message + metadata}
        end
    end
```

---

## 7. Phase 5 — Time Validation & Channel Dispatch

**Services involved:** `uclm-campaign-time-validation`, `uclm-test-campaign` (parallel), `uclm-campaign-cg-exclusion`, `uclm-campaign-exclusion-scan`, `uclm-contentmgmt`

### 7-Stage Validation Pipeline

```mermaid
flowchart TD
    IN(["Kafka: enriched-events"])

    S1["Stage 1: VALIDATION\nField-level schema validation\nCheck required fields per channel\nValidate channel type enum"]
    S2["Stage 2: TGCG\nControl/Target Group assignment\n→ POST /excludecg (CGX service)\nDetermine TG or CG bucket"]
    S3["Stage 3: EXCLUSION\nFilter excluded mobile numbers\n→ POST /exclusionscan (ESC service)\nCheck EMPLOYEE, VIP, RETAILER lists"]
    S4["Stage 4: GOVERNANCE\nCompliance rules check\n→ GET goals + subgoals (Campaign Manager)\nValidate frequency capping\nCheck campaign kill flag"]
    S5["Stage 5: TEMPLATE\nFinal message template render\n→ GET template (ContentMgmt)\nApply remaining dynamic params\nValidate final message length"]
    S6["Stage 6: PAYLOAD\nBuild channel-specific dispatch payload\nSMS: sender ID, DLT entity, template ID\nEmail: from, to, subject, body, attachments\nWA: template name, language, components\nRCS/Push: channel-specific format"]
    S7["Stage 7: KAFKA DISPATCH\nProduce to channel-specific topic\nSMS → sms_nrt_svc_valgov\nEmail → eml_nrt_svc_valgov\nWA → wa_nrt_svc_valgov\nRCS → rcs_nrt_svc_valgov\nPush → push_nrt_svc_valgov"]

    IN --> S1 --> S2 --> S3 --> S4 --> S5 --> S6 --> S7

    S1 -->|"fail"| DLQ1(["Kafka: exceptions\n(field validation error)"])
    S2 -->|"excluded"| DLQ2(["Kafka: analytics\n(CG exclusion recorded)"])
    S3 -->|"excluded"| DLQ3(["Kafka: analytics\n(exclusion scan recorded)"])
    S4 -->|"fail"| DLQ4(["Kafka: exceptions\n(governance failure)"])
    S5 -->|"fail"| DLQ5(["Kafka: exceptions\n(template render error)"])

    classDef kafka fill:#f39c12,color:#000,stroke:#d68910
    classDef stage fill:#2980b9,color:#fff,stroke:#1f618d
    classDef drop fill:#e74c3c,color:#fff,stroke:#c0392b

    class IN kafka
    class S1,S2,S3,S4,S5,S6,S7 stage
    class DLQ1,DLQ2,DLQ3,DLQ4,DLQ5 drop
```

---

## 8. Phase 6 — Bummlebee Delivery Pipeline

**Services involved:** `uclm-rate-controller-service`, `uclm-validation-governance-service`, `uclm-orchestrator-service`, `uclm-dlr-aerospike-cache-loader`  
**External:** Airtel SMS IQ, Netcore Email, Airtel IQ WhatsApp, Airtel IQ Conversation (RCS), FCM Push, DLT API, CMS Quota API

```mermaid
sequenceDiagram
    autonumber
    participant TV as Time Validation\n(Comms)
    participant RC as uclm-rate-controller-service
    participant PG as PostgreSQL
    participant VG as uclm-validation-governance-service
    participant DLT as DLT Scrubbing API
    participant CMS as CMS Quota API
    participant ORCH as uclm-orchestrator-service
    participant PROV as Channel Provider\n(SMS/Email/WA/RCS/Push)
    participant CACHE as uclm-dlr-aerospike-cache-loader
    participant AS as Aerospike

    TV->>RC: Kafka: channel_partner_*_nrt_svc_valgov\n(EventRequestDTO per subscriber)

    Note over RC,PG: ── Phase 6a: Rate Control ──
    RC->>PG: SELECT TPS config\n(tenant_onboarding, team_onboarding tables)
    RC->>RC: Resilience4j RateLimiter.acquirePermission()\nGlobal cap: 2000 TPS\nPer-tenant default: 5 TPS

    alt TPS limit exceeded
        RC->>RC: acknowledgment.nack(500ms)
        Note over RC: Kafka re-delivers after 500ms
    else TPS OK
        RC->>RC: acknowledgment.acknowledge()
        RC->>RC: Check downstream consumer lag\n(valgov + orch consumer groups)\nIf lag > threshold → pause consumption
        RC->>RC: Kafka PRODUCE → event topic
    end

    Note over VG,CMS: ── Phase 6b: Validation & Governance ──
    RC->>VG: Kafka CONSUME: event
    VG->>VG: Validate channel type\n(SMS, EMAIL, WHATSAPP, PUSH, RCS, D2C, FS)
    VG->>VG: Load lookup files (raw/schema/normalized)\nField-level validation + normalization
    VG->>VG: Language code normalization\nCategory validation

    alt SMS channel
        VG->>DLT: POST /dlt/scrub {mobile, templateId, entityId}
        DLT-->>VG: {approved: true/false, dltEntityId}
    end

    VG->>CMS: GET /cms/quota/check {campaignId, tenantId}
    CMS-->>VG: {remaining: N}

    VG->>VG: Build ChannelDispatchRequest\n(channel-specific endpoint payload)
    VG->>VG: Kafka PRODUCE → cs_raw_reporting_topic (analytics)
    VG->>VG: Kafka PRODUCE → dispatch topic

    alt Validation/DLT failure
        VG->>VG: Kafka PRODUCE → exceptions\n(or apb-exceptions for APB channels)
    end

    Note over ORCH,PROV: ── Phase 6c: Orchestration & Dispatch ──
    VG->>ORCH: Kafka CONSUME: dispatch
    ORCH->>ORCH: Check message expiry timestamp\nIf expired → drop + publish to error topic

    alt SMS
        ORCH->>PROV: POST https://iqsms.airtel.in/api/v1/send-sms\n{mobile, message, senderId, templateId}
    else Email
        ORCH->>PROV: POST https://emailapi.netcorecloud.net/v5.1/mail/send\n{from, to, subject, body}
    else WhatsApp
        ORCH->>PROV: POST https://iqwhatsapp.airtel.in/template/send\n{to, templateName, components[]}
    else RCS
        ORCH->>PROV: POST https://www.iqconversation.airtel.in/rcs/message/send
    else Push
        ORCH->>PROV: FCM /fcm/send {deviceToken, notification{}}
    end

    PROV-->>ORCH: HTTP 200 {messageId / requestId}

    ORCH->>ORCH: Kafka PRODUCE → dispatch-response (success)\nKafka PRODUCE → cs_raw_reporting_topic (analytics)
    ORCH->>ORCH: Kafka PRODUCE → wa_main_service (FullKafkaDml)\n{uuid, endpoint_request_id, status, moc}
    ORCH->>CM: Feign: PUT /counter/decrement {campaignId, tenantId}

    Note over CACHE,AS: ── Phase 6d: Cache Loading (for DLR) ──
    ORCH->>CACHE: Kafka CONSUME: wa_main_service / channel_partner_*_succ
    CACHE->>CACHE: Validate required fields (uuid, endpoint_request_id)
    CACHE->>AS: put(key=endpoint_request_id, TTL, FullKafkaDml)
    AS-->>CACHE: OK

    alt Aerospike DOWN
        CACHE->>CACHE: Do NOT ack → Kafka redelivers automatically
    else Write failure (Aerospike UP)
        CACHE->>CACHE: Retry 3× with exponential backoff
        CACHE->>CACHE: Kafka PRODUCE → wa_main_service_dlq (DLQ)
        CACHE->>CACHE: Acknowledge Kafka
    end
```

---

## 9. Phase 7 — DLR (Delivery Report) Tracking

**Services involved:** `uclm-dlr-api-service`, `uclm-dlr-enricher`  
**External:** Channel Providers (SMS/WA/RCS gateways)

```mermaid
sequenceDiagram
    autonumber
    participant GW as Channel Provider\n(SMS IQ / WA Gateway)
    participant DLRAPI as uclm-dlr-api-service
    participant Kafka
    participant ENR as uclm-dlr-enricher
    participant AS as Aerospike

    Note over GW,DLRAPI: ── DLR Webhook ──
    GW->>DLRAPI: POST /channel/dlr/status\n{requestId, mobile, status: "DELIVERED/FAILED",\ndeliveredAt, errorCode, providerRef}
    DLRAPI->>DLRAPI: Validate required fields
    DLRAPI->>Kafka: PRODUCE comms_iq_sms_dlr_raw\n(verbatim JSON, acks=all, idempotent=true)
    DLRAPI-->>GW: HTTP 200 OK

    Note over ENR,AS: ── DLR Enrichment ──
    Kafka->>ENR: CONSUME comms_iq_sms_dlr_raw\n(manual offset commit)
    ENR->>ENR: Parse raw DLR JSON\nExtract join key (requestId / uuid)

    alt JSON parse error or missing join key
        ENR->>Kafka: PRODUCE comms_iq_sms_dlr_enriched_dlq
        ENR->>ENR: Manual ACK (avoid infinite loop)
    else join key present
        ENR->>AS: get(key=requestId) → FullKafkaDml

        alt Aerospike MISS (message not yet cached)
            ENR->>ENR: Check retry count
            alt retryCount < 3
                ENR->>Kafka: PRODUCE comms_iq_sms_dlr_enriched_retrial\n{rawDlr, retryCount+1, retryAfter=now+15min}
            else retry exhausted
                ENR->>Kafka: PRODUCE comms_iq_sms_dlr_enriched_dlq
            end
            ENR->>ENR: Manual ACK
        else Aerospike HIT
            ENR->>ENR: Merge: rawDlr + FullKafkaDml\n→ EnrichedDLR{mobile, campaignId, tenantId,\n  channel, status, deliveredAt, errorCode,\n  originalRequestId, ...}
            ENR->>Kafka: PRODUCE comms_iq_sms_dlr_enriched
            ENR->>Kafka: PRODUCE cs_raw_reporting_topic (analytics event)
            ENR->>ENR: Manual ACK
        end
    end

    Note over Kafka: ── Retry consumer (self) ──
    Kafka->>ENR: CONSUME comms_iq_sms_dlr_enriched_retrial\n(after retryAfter delay has passed)
    Note over ENR: Repeats the same enrichment logic above\nwith incremented retryCount
```

**Retry Schedule:**

| Attempt | Delay Before Re-Consume |
|---------|------------------------|
| 1st retry | 15 minutes |
| 2nd retry | 30 minutes |
| 3rd retry | 60 minutes |
| After 3rd | → `dlr_enriched_dlq` (permanent failure) |

---

## 10. Phase 8 — Analytics & Reporting

**Services involved:** `uclm-analytics-reporting-service`  
**Databases:** Oracle/MySQL, Apache Druid  
**Cache:** Hazelcast in-memory

```mermaid
sequenceDiagram
    autonumber
    participant Kafka
    participant ARS as uclm-analytics-reporting-service
    participant Druid as Apache Druid
    participant HZ as Hazelcast Cache
    participant UI as Frontend UI

    Note over Kafka,ARS: ── Dimension Metadata Pipeline (async) ──
    Kafka->>ARS: CONSUME uclm_analytics\n(dimension/status events from Campaign Manager)
    ARS->>ARS: Upsert dimension table\n(channel, campaign_type, template, etc.)
    ARS->>HZ: refresh Hazelcast cache\n(id → human-readable name map)
    ARS->>Kafka: PRODUCE dimension_refresh_topic
    ARS->>Kafka: PRODUCE uclm_campaign_status (if campaign status change)

    Note over UI,Druid: ── Analytics Query (synchronous UI request) ──
    UI->>ARS: POST /analytics-reporting/api/v1/campaignOverview\n{tenantId, workspaceId,\n dateRange: {start, end},\n dimensions: ["campaign","channel","template"],\n metrics: ["sent","delivered","failed"]}
    ARS->>ARS: Build Druid SQL query\n(datasource: a{tenantId})
    ARS->>Druid: POST /druid/v2/sql\n{query: "SELECT __time, campaign_id, SUM(sent)\nFROM a{tenantId}\nWHERE ..."}
    Druid-->>ARS: Raw time-series rows [{campaignId, sent, delivered, ...}]
    ARS->>HZ: Batch lookup: campaignId → campaignName\n               channelId → channelName\n               templateId → templateName
    HZ-->>ARS: Enriched dimension map
    ARS->>ARS: Merge rows with dimension names\nApply pagination
    ARS-->>UI: 200 AnalyticsResponse\n{metrics, data[], pagination{page, total}}

    Note over UI,ARS: ── Excel Download ──
    UI->>ARS: GET /analytics-reporting/api/v1/download?reportId=X
    ARS->>Druid: Same query (no pagination limit)
    ARS->>ARS: Build .xlsx workbook (Apache POI)
    ARS-->>UI: application/vnd.openxmlformats-officedocument\n(binary Excel download)
```

---

## 11. Campaign State Machine Reference

```mermaid
stateDiagram-v2
    [*] --> DRAFT : Campaign Created (Campaign Manager)
    DRAFT --> APPROVAL_PENDING : Submit for Approval
    APPROVAL_PENDING --> PUBLISHED : Approved\n(CampaignDetails rows created)
    APPROVAL_PENDING --> REJECTED : Rejected
    REJECTED --> [*]

    PUBLISHED --> IN_PROGRESS : Airflow DAG\ncampaign_state_transition_scheduler\n(startTimestamp passed)
    IN_PROGRESS --> COMPLETED : Airflow DAG\n(endTimestamp passed)
    COMPLETED --> [*]

    IN_PROGRESS --> PAUSED : Manual pause
    PAUSED --> IN_PROGRESS : Manual resume

    PUBLISHED --> KILLED : Manual kill
    IN_PROGRESS --> KILLED : Manual kill
    PAUSED --> KILLED : Manual kill
    KILLED --> [*]

    PUBLISHED --> AUDIENCE_REQUESTED : Airflow audience_push_trigger_scheduler
    AUDIENCE_REQUESTED --> AUDIENCE_PUSHED : AM acknowledges
    AUDIENCE_REQUESTED --> PUBLISHED : AM error (rollback)

    AUDIENCE_PUSHED --> CALLBACK_RECEIVED : AM sends callback
    AUDIENCE_PUSHED --> CALLBACK_FAILED : Callback invalid (terminal)
    CALLBACK_FAILED --> [*]

    CALLBACK_RECEIVED --> CONTROL_FILE_DOWNLOADED : .ctrl.gz parsed
    CONTROL_FILE_DOWNLOADED --> CONTROL_FILE_PUBLISHED : Kafka publish success
    CONTROL_FILE_DOWNLOADED --> PUBLISHED : Kafka publish failed (rollback)

    CONTROL_FILE_PUBLISHED --> DATA_FILE_DOWNLOADED : Part files downloaded
    CONTROL_FILE_PUBLISHED --> DATA_FILE_DOWNLOAD_FAILED : Download error (terminal)
    DATA_FILE_DOWNLOAD_FAILED --> [*]

    DATA_FILE_DOWNLOADED --> PROCESSING_START : Dispatch started (Time Validation)
    PROCESSING_START --> DATA_FILE_DOWNLOADED : Dispatch error (rollback)
```

**State Ownership:**

| Transition | Responsible Service | Trigger |
|------------|--------------------|---------| 
| → `DRAFT` | Campaign Manager | REST API |
| `DRAFT` → `APPROVAL_PENDING` | Campaign Manager | REST API |
| `APPROVAL_PENDING` → `PUBLISHED` | Campaign Manager | REST API (approve action) |
| `PUBLISHED` → `IN_PROGRESS` | Campaign Manager | Airflow DAG (every 15 min) |
| `IN_PROGRESS` → `COMPLETED` | Campaign Manager | Airflow DAG (every 15 min) |
| `PUBLISHED` → `AUDIENCE_REQUESTED` | Audience Push | Airflow DAG (every 15 min) |
| `AUDIENCE_REQUESTED` → `AUDIENCE_PUSHED` | Audience Push | AM API response |
| `AUDIENCE_PUSHED` → `CALLBACK_RECEIVED` | Campaign Processor | AM async callback |
| `CALLBACK_RECEIVED` → `CONTROL_FILE_DOWNLOADED` | Campaign Processor | ctrl.gz parsed |
| `CONTROL_FILE_DOWNLOADED` → `CONTROL_FILE_PUBLISHED` | Campaign Processor | Kafka produce success |
| `CONTROL_FILE_PUBLISHED` → `DATA_FILE_DOWNLOADED` | Data File Download | All parts downloaded |
| `DATA_FILE_DOWNLOADED` → `PROCESSING_START` | Time Validation | Airflow DAG |

---

## 12. Kafka Topic Reference

### Comms Pipeline Topics

| Topic | Producer | Consumer | Payload |
|-------|----------|----------|---------|
| `control_file_request` | Campaign Processor | Data File Download | Control file metadata, partFile URLs |
| `event-enrichment` | Data File Download | Event Enrichment | Per-subscriber records + file location |
| `enriched-events` | Event Enrichment | Time Validation | Enriched subscriber events |
| `*_nrt_svc_valgov` (per channel) | Time Validation | Rate Controller | Validated EventRequestDTO |
| `uclm_analytics` | Campaign Manager, ContentMgmt | Analytics Reporting | Dimension/status change events |

### Bummlebee Dispatch Topics

| Topic | Producer | Consumer | Payload |
|-------|----------|----------|---------|
| `comms-input` / `channel-partner-rate-controller-input` | Upstream / Time Validation | Rate Controller | EventRequestDTO |
| `event` | Rate Controller | Validation Governance | EventRequestDTO (TPS-cleared) |
| `dispatch` / `channel_partner_*_endpoint` | Validation Governance | Orchestrator | ChannelDispatchRequest |
| `channel_partner_*_succ` / `wa_main_service` | Orchestrator | Aerospike Cache Loader | FullKafkaDml (success) |
| `channel_partner_*_err` | Orchestrator | Monitoring | FullKafkaDml (failure) |
| `cs_raw_reporting_topic` | VG, Orchestrator, DLR Enricher | Analytics | AnalyticsEventDTO |
| `exceptions` | Validation Governance | Monitoring | ExceptionEvent |
| `apb-exceptions` | Validation Governance | APB Monitoring | APB ExceptionEvent |
| `orchestrator-exceptions` | Orchestrator | Monitoring | Unhandled error |
| `d2c-clm-sit` (external Kafka) | Validation Governance | D2C downstream | D2C/FS events |

### DLR Topics

| Topic | Producer | Consumer | Payload |
|-------|----------|----------|---------|
| `comms_iq_sms_dlr_raw` | DLR API Service | DLR Enricher | Raw SMS DLR (verbatim from provider) |
| `comms_iq_wa_dlr_raw` | DLR API Service | DLR Enricher | Raw WhatsApp DLR |
| `comms_iq_sms_dlr_enriched` | DLR Enricher | Analytics/Downstream | Enriched DLR |
| `comms_iq_sms_dlr_enriched_retrial` | DLR Enricher | DLR Enricher (retry) | DLR retry envelope |
| `comms_iq_sms_dlr_enriched_dlq` | DLR Enricher | Manual recovery | Permanently failed DLRs |
| `wa_main_service_dlq` / `comms_iq_sms_dlr_cache_dlq` | Cache Loader | Manual recovery | Failed Aerospike writes |

### Analytics Output Topics

| Topic | Producer | Consumer |
|-------|----------|----------|
| `dimension_refresh_topic` | Analytics Reporting | Downstream analytics |
| `uclm_campaign_status` | Analytics Reporting | Downstream |

---

## 13. Error & Compensation Flows

```mermaid
flowchart TD
    subgraph CommsErrors["Comms — Compensation Flows"]
        A1["AM API error\n(Audience Push)"] -->|"rollback"| R1["state → PUBLISHED\n(retry on next DAG run)"]
        A2["Kafka publish failed\n(Campaign Processor)"] -->|"rollback"| R2["state → PUBLISHED\n(retry on next DAG run)"]
        A3["Callback invalid\n(Campaign Processor)"] -->|"terminal"| T1["state → CALLBACK_FAILED"]
        A4["Part file download error\n(Data File Download)"] -->|"terminal"| T2["state → DATA_FILE_DOWNLOAD_FAILED"]
        A5["Dispatch error\n(Time Validation)"] -->|"rollback"| R3["state → DATA_FILE_DOWNLOADED\n(retry on next cycle)"]
    end

    subgraph BumblebeeErrors["Bummlebee — Error Flows"]
        B1["TPS exceeded\n(Rate Controller)"] -->|"nack 500ms"| R4["Kafka redelivers\n(auto-retry)"]
        B2["Validation failure\n(Validation Governance)"] -->|"drop + publish"| T3["exceptions topic\n(monitoring)"]
        B3["Provider HTTP failure\n(Orchestrator)"] -->|"Resilience4j retry"| B3R["retry N times"]
        B3R -->|"exhausted"| T4["channel_partner_*_err\n(error topic)"]
        B4["Aerospike DOWN\n(Cache Loader)"] -->|"no ACK"| R5["Kafka redelivers\n(until AS is back)"]
        B5["Write failure (AS UP)\n(Cache Loader)"] -->|"retry 3×"| T5["DLQ → ACK"]
    end

    subgraph DLRErrors["DLR — Error Flows"]
        D1["JSON parse error\n(DLR Enricher)"] -->|"DLQ + ACK"| T6["dlr_enriched_dlq"]
        D2["Aerospike MISS\n(DLR Enricher)"] -->|"retry (15→30→60 min)"| D2R["retrial topic"]
        D2R -->|"3 attempts exhausted"| T7["dlr_enriched_dlq"]
        D2R -->|"HIT on retry"| OK1["enriched topic"]
    end

    classDef error fill:#e74c3c,color:#fff,stroke:#c0392b
    classDef recover fill:#2980b9,color:#fff,stroke:#1f618d
    classDef terminal fill:#7f8c8d,color:#fff,stroke:#636e72
    classDef success fill:#27ae60,color:#fff,stroke:#1e8449

    class A1,A2,A3,A4,A5,B1,B2,B3,B4,B5,D1,D2 error
    class R1,R2,R3,R4,R5,B3R,D2R recover
    class T1,T2,T3,T4,T5,T6,T7 terminal
    class OK1 success
```

### Summary Table

| Service | Error Condition | Recovery Action |
|---------|----------------|-----------------|
| Audience Push | AM API error | Rollback state → PUBLISHED |
| Campaign Processor | Callback invalid | State → CALLBACK_FAILED (terminal) |
| Campaign Processor | Kafka publish fail | Rollback state → PUBLISHED |
| Data File Download | Download error | State → DATA_FILE_DOWNLOAD_FAILED (terminal) |
| Time Validation | Dispatch error | Rollback state → DATA_FILE_DOWNLOADED |
| Rate Controller | TPS limit exceeded | `nack(500ms)` — Kafka redelivers |
| Validation Governance | Validation/DLT failure | Publish to `exceptions` — drop message |
| Orchestrator | Provider HTTP failure | Resilience4j retry → `channel_partner_*_err` |
| Orchestrator | Message expired | Drop + publish error |
| Aerospike Cache Loader | Aerospike DOWN | No ACK — Kafka holds message |
| Aerospike Cache Loader | Write failure | Retry 3× → DLQ → ACK |
| DLR Enricher | JSON parse error | DLQ → ACK |
| DLR Enricher | Aerospike MISS | Retry topic (15/30/60 min) → DLQ after 3 |
| DLR Enricher | Aerospike HIT | Enriched topic → ACK |

---

## 14. IAM Header Contract

All authenticated requests between services must carry these headers, populated from the JWT issued by `uclm-auth-manager`:

| Header | JWT Claim | Format | Example |
|--------|-----------|--------|---------|
| `x-tenant-id` | `x-tenant-id` | Integer string | `"1"` |
| `x-workspace-id` | `x-workspace-id` | Integer string | `"5"` |
| `x-user-id` | `x-user-id` | Email / username | `"user@corp.com"` |
| `x-user-hierarchy` | `x-user-hierarchy` | Hyphen-separated node IDs | `"1-3-5"` |
| `Authorization` | — | `Bearer <JWT>` | `"Bearer eyJhbGci..."` |

### Whitelisted Paths (No Auth Required)

| Service | Path | Reason |
|---------|------|--------|
| auth-manager | `POST /ssoLogin` | Login initiation |
| auth-manager | `POST /user/saml/response` | SAML IdP callback |
| auth-manager | `GET /tenant/config/**` | Public tenant lookup |
| auth-manager | `GET /user/saml/logout/response` | SAML logout callback |
| contentmgmt | `POST /template/callback/sms` | DLT webhook |
| campaign-processor | `POST /api/v1/callback` | Audience Manager async callback |
| dlr-api-service | `POST /channel/dlr/status` | Channel provider DLR webhook |
