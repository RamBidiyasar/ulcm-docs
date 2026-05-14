# Kafka Message Flows

All Kafka topic producers, consumers, message formats, and consumer groups.

---

## Topic Flow Diagram

```mermaid
flowchart LR
    subgraph Producers[" Producers"]
        CM["Campaign Manager"]
        PROC["Campaign Processor"]
        EE["Event Enrichment"]
        TV["Time Validation"]
        TC["Test Campaign"]
        ARS["Analytics Reporting"]
    end

    subgraph Topics[" Kafka Topics"]
        T0A[["uclm_analytics"]]
        T0B[["orchestrator.request"]]
        T1[["control_file_request"]]
        T2[["event-enrichment"]]
        T3[["enriched-events"]]
        T4[["sms_nrt_svc_valgov\nchannel_partner_sms_..."]]
        T5[["eml_nrt_svc_valgov\nchannel_partner_eml_..."]]
        T6[["wa_nrt_svc_valgov\nchannel_partner_wa_..."]]
        T7[["rcs_nrt_svc_valgov\nchannel_partner_rcs_..."]]
        T8[["push_nrt_svc_valgov\nchannel_partner_push_..."]]
        T9[["comms_analytics_logs"]]
        T10[["comms_planner_logs\n(optional)"]]
        T11[["dimension_refresh_topic"]]
        T12[["uclm_campaign_status"]]
    end

    subgraph Consumers[" Consumers"]
        DFD["Data File Download"]
        EE2["Event Enrichment"]
        TV2["Time Validation"]
        TC2["Test Campaign"]
        ARS2["Analytics Reporting"]
        CH_SMS["SMS Gateway"]
        CH_EML["Email Service"]
        CH_WA["WhatsApp Business"]
        CH_RCS["RCS Provider"]
        CH_PUSH["FCM / APNs"]
        ANA["Analytics System"]
        ORCH["Email Orchestrator"]
        DIM_DOWN["Downstream\nDim Consumers"]
    end

    CM -->|"Produce (state changes)"| T0A
    CM -->|"Produce (approval emails)"| T0B
    T0A --> ARS2
    T0B --> ORCH

    PROC -->|"Produce\nconsumer-group: control_file_request"| T1
    T1 --> DFD

    EE -->|"Produce"| T3
    T2 --> EE2

    TV -->|"Produce"| T4
    TV -->|"Produce"| T5
    TV -->|"Produce"| T6
    TV -->|"Produce"| T7
    TV -->|"Produce"| T8
    TV -->|"Produce"| T9
    TV -->|"Produce (optional)"| T10

    TC -->|"Produce (test topics)"| T4
    TC -->|"Produce (test topics)"| T5

    T3 --> TV2
    T3 -.->|"test path"| TC2

    T4 --> CH_SMS
    T5 --> CH_EML
    T6 --> CH_WA
    T7 --> CH_RCS
    T8 --> CH_PUSH
    T9 --> ANA

    ARS -->|"Produce (after dim upsert)"| T11
    ARS -->|"Produce (campaign status)"| T12
    T11 --> DIM_DOWN
    T12 --> ANA

    classDef topic fill:#ff9900,color:#000,stroke:#cc7700,rx:8
    classDef producer fill:#4a90d9,color:#fff,stroke:#2c6ea8
    classDef consumer fill:#27ae60,color:#fff,stroke:#1e8449

    class T1,T2,T3,T4,T5,T6,T7,T8,T9,T10,T11,T12 topic
    class PROC,EE,TV,TC,ARS producer
    class DFD,EE2,TV2,TC2,ARS2,CH_SMS,CH_EML,CH_WA,CH_RCS,CH_PUSH,ANA consumer
    classDef kafka fill:#f39c12,color:#000,stroke:#d68910
    classDef svc fill:#2980b9,color:#fff,stroke:#1f618d
    classDef ext fill:#8e44ad,color:#fff,stroke:#6c3483
    classDef channel fill:#d35400,color:#fff,stroke:#a04000
    class CM,DIM_DOWN,ORCH,T0A,T0B svc
```

---

## Kafka Topic Registry

| Topic | Producer | Consumer | Consumer Group | Ack Mode |
|-------|----------|----------|----------------|----------|
| `uclm_analytics` | **Campaign Manager** | **Analytics Reporting** | `analytics-metadata-service` | earliest |
| `orchestrator.request` | **Campaign Manager** | Email Orchestrator | — | — |
| `control_file_request` | Campaign Processor | Data File Download | `control_file_request` | Manual |
| `event-enrichment` | Upstream trigger | Event Enrichment | `event-enrichment-group` | Manual |
| `enriched-events` | Event Enrichment | Time Validation | `validation-service-group` | Manual |
| `channel_partner_sms_nrt_svc_valgov` | Time Validation | SMS Gateway | — | `acks=all` |
| `channel_partner_eml_nrt_svc_valgov` | Time Validation | Email Service | — | `acks=all` |
| `channel_partner_wa_nrt_svc_valgov` | Time Validation | WhatsApp Business | — | `acks=all` |
| `channel_partner_rcs_nrt_svc_valgov` | Time Validation | RCS Provider | — | `acks=all` |
| `channel_partner_push_nrt_svc_valgov` | Time Validation | FCM / APNs | — | `acks=all` |
| `comms_analytics_logs` | Time Validation | Analytics System (external) | — | — |
| `comms_planner_logs` | Time Validation | Logging System | — | optional |
| `dimension_refresh_topic` | **Analytics Reporting** | Downstream dim consumers | — | — |
| `uclm_campaign_status` | **Analytics Reporting** | Analytics System (external) | — | — |

---

## control_file_request — Message Schema

```mermaid
classDiagram
    class ControlFileMessage {
        +String audienceId
        +String transactionId
        +String date
        +int partFileCount
        +List~PartFile~ partFiles
        +String recordDelimiter
        +String fieldDelimiter
        +long recordCount
        +List~String~ attributeList
        +String compression
        +String expiryTime
    }

    class PartFile {
        +String url
    }

    ControlFileMessage "1" --> "*" PartFile
```

**Example:**
```json
{
  "audienceId": "AUD_12345",
  "transactionId": "txn-abc-123",
  "date": "2025-01-15",
  "partFileCount": 3,
  "partFiles": [
    { "url": "https://am-host/files/AUD_12345_part1.csv.gz" },
    { "url": "https://am-host/files/AUD_12345_part2.csv.gz" },
    { "url": "https://am-host/files/AUD_12345_part3.csv.gz" }
  ],
  "recordDelimiter": "\\u000a",
  "fieldDelimiter": "\\u0001",
  "recordCount": 500000,
  "attributeList": ["msisdn", "circle", "segment"],
  "compression": "gzip",
  "expiryTime": "2025-01-16T00:00:00Z"
}
```

---

## Kafka Security Modes (all services)

```mermaid
flowchart LR
    KafkaConfig{Environment} -->|local| NONE["Security: NONE\n(plaintext)"]
    KafkaConfig -->|UAT / Prod| KERB["SASL_PLAINTEXT\nGSSAPI / Kerberos\n(keytab)"]
    KafkaConfig -->|Cloud| SCRAM["SASL_PLAINTEXT\nSCRAM-SHA-256\n(username+password)"]
    KafkaConfig -->|Simple| PLAIN["SASL_PLAINTEXT\nPLAIN\n(username+password)"]
    classDef kafka fill:#f39c12,color:#000,stroke:#d68910
    classDef svc fill:#2980b9,color:#fff,stroke:#1f618d
    classDef user fill:#34495e,color:#fff,stroke:#2c3e50
    class KafkaConfig kafka
    class KERB svc
    class PLAIN,SCRAM user
```
