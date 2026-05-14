# REST API Call Graph

All synchronous HTTP calls between services and to external systems.

---

## Full REST Call Graph

```mermaid
flowchart TD
    subgraph Internal[" Internal Services"]
        RC["Rate Controller\n:8081"]
        VG["Validation Governance\n:7777"]
        ORCH["Orchestrator\n(Kafka-only)"]
        DLRAPI["DLR API Service\n:8080"]
    end

    subgraph ExtGov[" Governance APIs"]
        DLT[("DLT Scrubbing API\nhttp://10.222.160.29:2501")]
        CMS[("CMS Quota API")]
    end

    subgraph ExtChannel[" Channel Providers"]
        AIRTEL_SMS[("Airtel SMS IQ\nhttps://iqsms.airtel.in")]
        NETCORE[("Netcore Email API\nhttps://emailapi.netcorecloud.net")]
        SMTP[("SMTP Server\n10.56.131.8:25")]
        WA_AIRTEL[("Airtel IQ WhatsApp\nhttps://iqwhatsapp.airtel.in")]
        RCS_AIRTEL[("Airtel IQ Conversation\nhttps://www.iqconversation.airtel.in")]
        FCM[("Firebase FCM\n(Push)")]
    end

    subgraph ExtGateway[" SMS Gateways"]
        GW[("SMS Gateway\nTwilio / Airtel IQ")]
    end

    %% Validation Governance external calls
    VG -->|"POST /api/process/scrubbing/sms/single\n DLT compliance check for SMS"| DLT
    VG -->|"POST <cms-url>/quota/check\n CMS quota governance"| CMS

    %% Orchestrator channel calls (via Feign)
    ORCH -->|"POST /api/v1/send-sms\n(Basic Auth)"| AIRTEL_SMS
    ORCH -->|"POST /v5.1/mail/send\n(API Key)"| NETCORE
    ORCH -->|"SMTP send\n(optional fallback)"| SMTP
    ORCH -->|"POST /gateway/airtel-xchange/basic/whatsapp-manager/v1/template/send\n(Basic Auth)"| WA_AIRTEL
    ORCH -->|"POST /gateway/airtel-xchange/whatsapp-message-acceptor/v1/rcs/message/send\n(Basic Auth)"| RCS_AIRTEL
    ORCH -->|"FCM HTTP v1\n(Firebase Admin SDK)"| FCM
    ORCH -->|"POST <cms-url>/quota/decrement\n on success or failure"| CMS

    %% DLR API receives DLRs
    GW -->|"HTTP POST /channel/dlr/status\n DLR webhook callback"| DLRAPI

    classDef internal fill:#4a90d9,color:#fff,stroke:#2c6ea8
    classDef external fill:#27ae60,color:#fff,stroke:#1e8449
    classDef governance fill:#9b59b6,color:#fff,stroke:#7d3c98
    classDef channel fill:#e67e22,color:#fff,stroke:#ca6f1e

    class RC,VG,ORCH,DLRAPI internal
    class DLT,CMS governance
    class AIRTEL_SMS,NETCORE,SMTP,WA_AIRTEL,RCS_AIRTEL,FCM channel
    class GW external
    classDef kafka fill:#f39c12,color:#000,stroke:#d68910
    classDef svc fill:#2980b9,color:#fff,stroke:#1f618d
    classDef ext fill:#8e44ad,color:#fff,stroke:#6c3483
    class n ext
```

---

## REST Endpoint Details

### Validation Governance → DLT (SMS only)

```
POST http://10.222.160.29:2501/api/process/scrubbing/sms/single
Content-Type: application/json
Authorization: Basic <dlt.username>:<dlt.password>

{
  "mobile": "9999999999",
  "templateId": "...",
  "entityId": "DLT_PEID_TEST",
  "tmid": "DLT_TMID_TEST"
}
```

### Orchestrator → Airtel SMS IQ

```
POST https://iqsms.airtel.in/api/v1/send-sms
Authorization: Basic <base64(username:password)>
Content-Type: application/json

{ "mobile": "...", "message": "...", ... }
```

### Orchestrator → Netcore Email API

```
POST https://emailapi.netcorecloud.net/v5.1/mail/send
api_key: <API_KEY>
Content-Type: application/json

{ "from": { "email": "..." }, "to": [...], "subject": "...", "content": [...] }
```

### Orchestrator → Airtel IQ WhatsApp

```
POST https://iqwhatsapp.airtel.in/gateway/airtel-xchange/basic/whatsapp-manager/v1/template/send
Authorization: Basic <base64(username:password)>
from: 918045003912
Content-Type: application/json
```

### Orchestrator → Airtel IQ Conversation (RCS)

```
POST https://www.iqconversation.airtel.in/gateway/airtel-xchange/whatsapp-message-acceptor/v1/rcs/message/send
Authorization: Basic <base64(username:password)>
customer-id: InternalDemo_Aishwarya
sub-account-id: <uuid>
agent-id: wynk_p9disahh_agent@rbm.goog
```

### DLR API Service — Incoming Webhook

```
POST /channel/dlr/status
Content-Type: application/json

{
  "requestId": "...",
  "mobile": "9999999999",
  "status": "DELIVERED",
  "deliveredAt": "...",
  "errorCode": null
}
Response: HTTP 200 OK (sync Kafka publish confirmed)
Response: HTTP 500 (Kafka publish failed)
```

### Health Check Endpoints (DLR API Service)

```
GET /actuator/health/liveness   → liveness probe
GET /actuator/health/readiness  → readiness probe
GET /actuator/health            → full health status
```

---

## No REST between Internal Services

The Rate Controller, Validation Governance, and Orchestrator communicate **exclusively via Kafka** — there are no synchronous REST calls between them.

The DLR Enricher and Aerospike Cache Loader also communicate only via Kafka and Aerospike — no REST.
