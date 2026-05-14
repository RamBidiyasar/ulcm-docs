# End-to-End Integration Flow

This diagram shows every service, how data flows between them, and which external systems are involved.

---

## Full System Flowchart

```mermaid
flowchart TD
    UI([ User / UI])

    subgraph Phase1["Phase 1 — Campaign Creation"]
        CM[" Campaign Manager\n(CRUD · State Owner)\nPORT: 80"]
    end

    subgraph Phase2["Phase 2 — Audience Push"]
        AP[" Audience Push\nPORT: 8095"]
    end

    subgraph External_AM["External — Audience Manager"]
        AM[(" Audience Manager\n(External Platform)")]
    end

    subgraph Phase3["Phase 3 — Control File Processing"]
        PROC[" Campaign Processor\n(Callback Receiver)\nPORT: 8080"]
    end

    subgraph Kafka1["Kafka"]
        KT1[[" control_file_request"]]
    end

    subgraph Phase4["Phase 4 — Data File Download"]
        DFD[" Data File Download\n(Parallel Downloader)\nPORT: 8070"]
    end

    subgraph Kafka2["Kafka"]
        KT2[[" event-enrichment"]]
    end

    subgraph Phase5["Phase 5 — Event Enrichment"]
        EE[" Event Enrichment\nPORT: 8091"]
    end

    subgraph Kafka3["Kafka"]
        KT3[[" enriched-events"]]
    end

    subgraph Phase6["Phase 6 — Validation & Dispatch"]
        TV[" Time Validation\nPORT: 8091"]
        TC[" Test Campaign\n(Mirror of TV)"]
    end

    subgraph ExclusionSvcs["Shared Exclusion Services"]
        CGX[" CG Exclusion\nSpEL Rules · PORT: 8080"]
        ESC[" Exclusion Scan\nCSV Lists · PORT: 8080"]
    end

    subgraph ChannelKafka["Channel Kafka Topics"]
        KSMS[[" sms_nrt_svc_valgov"]]
        KEML[[" eml_nrt_svc_valgov"]]
        KWA[[" wa_nrt_svc_valgov"]]
        KRCS[[" rcs_nrt_svc_valgov"]]
        KPUSH[[" push_nrt_svc_valgov"]]
        KANAL[[" comms_analytics_logs"]]
    end

    subgraph ChannelPartners["Channel Partners"]
        GW_SMS[SMS Gateway]
        GW_EML[Email Service]
        GW_WA[WhatsApp Business]
        GW_RCS[RCS Provider]
        GW_PUSH[FCM / APNs]
        ANALYTICS[Analytics System]
    end

    subgraph Phase7["Phase 7 — Analytics Reporting"]
        ARS[" Analytics Reporting\nDruid + Hazelcast\nPORT: 8080"]
    end

    subgraph ExtServices["External Support Services"]
        AUTH[(" Auth Manager\nTenant Timezone")]
        CONT[(" Content Manager\nTemplates")]
        DRUID[(" Apache Druid\nTime-series store")]
    end

    %% Phase 1  2
    UI -->|"POST /api/v1/campaigns"| CM
    CM -->|"state: PUBLISHED\n(scheduled or manual trigger)"| AP

    %% Phase 2  AM
    AP -->|"GET tenant timezone"| AUTH
    AP -->|"POST /audience-manager/push/file/v1/create"| AM
    AP -->|"GET /audience-manager/audience/v1/fetch"| AM

    %% AM  Phase 3
    AM -->|"POST /campaign-processor/api/v1/callback\n(async, after file ready)"| PROC
    PROC -->|"GET .ctrl.gz download"| AM

    %% Phase 3  Kafka  Phase 4
    PROC -->|"Produce control file JSON"| KT1
    KT1 -->|"Consume"| DFD

    %% Phase 4 downloads
    DFD -->|"GET part files (parallel)"| AM
    DFD -->|"GET tenant timezone"| AUTH

    %% Phase 4  Phase 5
    DFD -->|"state: DATA_FILE_DOWNLOADED\n(triggers enrichment)"| KT2
    KT2 -->|"Consume"| EE

    %% Phase 5 enrichment calls
    EE -->|"GET audience attributes"| AM
    EE -->|"GET campaign metadata"| CM
    EE -->|"GET template"| CONT
    EE -->|"POST excludecg"| CGX
    EE -->|"POST exclusionscan"| ESC

    %% Phase 5  Phase 6
    EE -->|"Produce enriched events"| KT3
    KT3 -->|"Consume"| TV
    KT3 -.->|"Consume (test)"| TC

    %% Phase 6 validation calls
    TV -->|"POST excludecg"| CGX
    TV -->|"POST exclusionscan"| ESC
    TV -->|"GET template"| CONT
    TV -->|"GET goals + subgoals"| CM

    %% Phase 6  Channel topics
    TV -->|"Produce"| KSMS
    TV -->|"Produce"| KEML
    TV -->|"Produce"| KWA
    TV -->|"Produce"| KRCS
    TV -->|"Produce"| KPUSH
    TV -->|"Produce"| KANAL

    %% Channel topics  Partners
    KSMS --> GW_SMS
    KEML --> GW_EML
    KWA --> GW_WA
    KRCS --> GW_RCS
    KPUSH --> GW_PUSH
    KANAL --> ANALYTICS

    %% Phase 7 — Analytics (parallel path from Campaign Manager)
    CM -->|"Kafka: uclm_analytics\n(dimension & status events)"| ARS
    ARS -->|"SQL queries"| DRUID
    UI -->|"POST /analytics-reporting/api/v1/*"| ARS

    %% Styling
    classDef kafka fill:#ff9900,color:#000,stroke:#cc7700
    classDef service fill:#4a90d9,color:#fff,stroke:#2c6ea8
    classDef external fill:#27ae60,color:#fff,stroke:#1e8449
    classDef channel fill:#8e44ad,color:#fff,stroke:#6c3483
    classDef exclusion fill:#e74c3c,color:#fff,stroke:#c0392b

    class KT1,KT2,KT3,KSMS,KEML,KWA,KRCS,KPUSH,KANAL kafka
    class CM,AP,PROC,DFD,EE,TV,TC,ARS service
    class AM,AUTH,CONT,DRUID external
    class GW_SMS,GW_EML,GW_WA,GW_RCS,GW_PUSH,ANALYTICS channel
    class CGX,ESC exclusion
    classDef svc fill:#2980b9,color:#fff,stroke:#1f618d
    classDef db fill:#27ae60,color:#fff,stroke:#1e8449
    classDef ext fill:#8e44ad,color:#fff,stroke:#6c3483
    classDef user fill:#34495e,color:#fff,stroke:#2c3e50
    class n kafka
    class UI user
```

---

## Sequence Diagram — Happy Path

```mermaid
sequenceDiagram
    autonumber
    actor User
    participant CM as Campaign Manager
    participant AP as Audience Push
    participant AUTH as Auth Manager
    participant AM as Audience Manager
    participant PROC as Campaign Processor
    participant DFD as Data File Download
    participant EE as Event Enrichment
    participant CGX as CG Exclusion
    participant ESC as Exclusion Scan
    participant CONT as Content Manager
    participant TV as Time Validation
    participant CH as Channel Partners
    participant ARS as Analytics Reporting
    participant DRUID as Apache Druid

    User->>CM: POST /api/v1/campaigns (create)
    CM-->>User: Campaign created (state: DRAFT)
    User->>CM: Publish campaign
    CM-->>User: state  PUBLISHED

    Note over AP: Scheduled or manual trigger
    AP->>CM: Read campaigns (state=PUBLISHED)
    AP->>AUTH: GET tenant timezone
    AP->>AM: POST /push/file/v1/create
    AP->>CM: Update state  AUDIENCE_REQUESTED
    AM-->>AP: 202 Accepted
    AP->>CM: Update state  AUDIENCE_PUSHED

    AM->>PROC: POST /callback (ctrl.gz URL ready)
    PROC->>AM: GET .ctrl.gz (download control file)
    PROC->>CM: Update state  CALLBACK_RECEIVED
    PROC->>CM: Update state  CONTROL_FILE_DOWNLOADED
    PROC-)DFD: Kafka: control_file_request
    PROC->>CM: Update state  CONTROL_FILE_PUBLISHED

    DFD->>AUTH: GET tenant timezone
    DFD->>AM: GET part file 1 (parallel)
    DFD->>AM: GET part file 2 (parallel)
    DFD->>AM: GET part file N (parallel)
    DFD->>CM: Update state  DATA_FILE_DOWNLOADED

    Note over EE: Triggered by DATA_FILE_DOWNLOADED state
    EE->>AM: GET audience attributes
    EE->>CM: GET campaign metadata + goals
    EE->>CONT: GET message template
    EE->>CGX: POST /excludecg (SpEL rule check)
    EE->>ESC: POST /exclusionscan (CSV check)
    EE-)TV: Kafka: enriched-events

    TV->>CGX: POST /excludecg
    TV->>ESC: POST /exclusionscan
    TV->>CONT: GET template (final render)
    TV->>CM: GET goals + subgoals
    TV-)CH: Kafka: channel_partner_sms/eml/wa/rcs/push_nrt_svc_valgov

    Note over CM,ARS: Parallel path — runs independently of campaign execution
    CM-)ARS: Kafka: uclm_analytics (dimension/status events)
    ARS->>ARS: Upsert dimension table + refresh Hazelcast cache
    User->>ARS: POST /analytics-reporting/api/v1/campaignOverview
    ARS->>DRUID: POST /druid/v2/sql {query: "SELECT ..."}
    DRUID-->>ARS: raw time-series rows
    ARS->>ARS: Enrich IDs  names (Hazelcast lookup)
    ARS-->>User: 200 AnalyticsResponse {metrics, data, pagination}
```
