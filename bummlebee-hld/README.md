# Bummlebee Platform — High-Level Design (HLD)

> Generated from full source-code analysis of all 6 microservices  
> Stack: Java 17/21 · Spring Boot 3.x · Kafka · PostgreSQL · Aerospike · Kubernetes

---

## Files in This Folder

### System-Level HLD
| File | Description |
|------|-------------|
| `00-services-overview.md` | All 6 services at a glance with roles and tech |
| `01-end-to-end-flow.md` | Full integration flow — delivery pipeline + DLR pipeline |
| `02-kafka-flows.md` | Kafka topic producers, consumers, and message flows |
| `03-state-machine.md` | DLR enrichment state machine |
| `04-rest-api-calls.md` | Synchronous HTTP/REST call graph |
| `05-database-map.md` | DB / cache ownership and access per service |
| `06-service-details.md` | Per-service summary (ports, env vars, endpoints) |
| `07-deployment-guide.md` | Docker, Kubernetes, and Helm deployment guide |

### Per-Service Detailed HLD
| File | Service |
|------|---------|
| `hld-01-rate-controller.md` | uclm-rate-controller-service — TPS throttle, lag monitor, capacity guard |
| `hld-02-validation-governance.md` | uclm-validation-governance-service — validation pipeline, DLT, CMS, payload build |
| `hld-03-orchestrator.md` | uclm-orchestrator-service — channel routing, Feign clients, CMS quota, analytics |
| `hld-04-dlr-api-service.md` | uclm-dlr-api-service — webhook receiver, Kafka producer, error handling |
| `hld-05-aerospike-cache-loader.md` | uclm-dlr-aerospike-cache-loader — Kafka→Aerospike, retry, DLQ |
| `hld-06-dlr-enricher.md` | uclm-dlr-enricher — DLR correlation, enrichment, retry scheduler, circuit breaker |

---

## System Overview

The **Bummlebee** platform is the **multi-channel message delivery and DLR tracking layer** of the UCLM ecosystem. It receives enriched, validated events from the upstream UCLM comms pipeline, applies **rate limiting**, runs **validation & governance** checks, dispatches messages to **external channel providers** (SMS · Email · WhatsApp · RCS · Push), and tracks **Delivery Reports (DLRs)** back.

### 6 Services at a Glance

```mermaid
graph LR
    subgraph Delivery[" Delivery Pipeline"]
        RC[Rate Controller\nThrottle & TPS]
        VG[Validation Governance\nDLT + CMS + Payload Build]
        ORCH[Orchestrator\nChannel Routing]
    end

    subgraph DLR[" DLR Pipeline"]
        DLRAPI[DLR API\nWebhook Receiver]
        CACHE[Aerospike Cache Loader\nKafka  Aerospike]
        ENR[DLR Enricher\nCorrelation + Enrichment]
    end

    subgraph External[" External"]
        UP[("Upstream UCLM\ncomms-input")]
        CH[Channel Partners\nSMS·Email·WA·RCS·Push]
        AS[(Aerospike\nCache)]
        AERO[Analytics\ncs_raw_reporting_topic]
    end

    UP --> RC
    RC -->|event| VG
    VG -->|dispatch| ORCH
    ORCH --> CH

    CH -->|DLR webhook| DLRAPI
    DLRAPI -->|iq_channel_dlr_raw| ENR
    ORCH -->|wa_main_service| CACHE
    CACHE --> AS
    ENR --> AS
    ENR -->|enriched DLR| AERO
    classDef kafka fill:#f39c12,color:#000,stroke:#d68910
    classDef svc fill:#2980b9,color:#fff,stroke:#1f618d
    classDef db fill:#27ae60,color:#fff,stroke:#1e8449
    classDef ext fill:#8e44ad,color:#fff,stroke:#6c3483
    classDef channel fill:#d35400,color:#fff,stroke:#a04000
    class CH,ORCH channel
    class AS,CACHE db
    class DLRAPI,VG ext
    class AERO,UP kafka
    class ENR,RC svc
```

---

## Tech Stack

| Layer | Technology |
|-------|-----------|
| Language | Java 17 (DLR services) · Java 21 (Orchestrator, Validation) |
| Frameworks | Spring Boot 3.2.x / 3.3.x |
| Messaging | Apache Kafka (Kerberos/SASL_PLAINTEXT in UAT/Prod) |
| Database | PostgreSQL (Rate Controller) · Aerospike 7.1.0 (DLR cache) |
| ORM | Spring Data JPA |
| HTTP Clients | Spring Cloud OpenFeign · RestTemplate |
| Resilience | Resilience4j (Circuit Breaker · Retry · Rate Limiter) |
| Container | Docker · Kubernetes (OCP) · Helm |
| Security | Kerberos GSSAPI for Kafka in UAT/Prod |
