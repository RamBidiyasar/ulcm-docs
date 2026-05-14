# UCLM Campaign Platform — High-Level Design (HLD)

> Generated from full source-code analysis of all 10 microservices  
> Namespace: `nextgenclm-api-develop` | Stack: Java 17 · Spring Boot 3.x · Kafka · Oracle · Kubernetes

---

## Files in This Folder

| File | Description |
|------|-------------|
| `01-end-to-end-flow.md` | Full integration flow across all 9 services |
| `02-kafka-flows.md` | Kafka topic producers & consumers |
| `03-state-machine.md` | Campaign lifecycle state machine |
| `04-rest-api-calls.md` | Synchronous HTTP/REST call graph |
| `05-database-map.md` | DB table ownership & service access |
| `06-service-details.md` | Per-service deep dive (ports, env vars, endpoints) |
| `10-airflow-dags.md` | Airflow DAG scripts — scheduled triggers for state transitions, audience push, and campaign execution |
| `11-critical-bugs.md` | Deep static-analysis bug report — 26 confirmed bugs across 5 campaign services (20 Critical · 5 High · 1 Medium) |

---

## System Overview

The UCLM platform manages the **complete lifecycle of multi-channel marketing campaigns** —
from creation and audience selection, through exclusion filtering, data enrichment, and validation,
to final delivery over **SMS · Email · WhatsApp · RCS · Push**.

### 9 Services at a Glance

```mermaid
graph LR
    subgraph Core[" Core Domain"]
        CM[Campaign Manager\nCRUD + State Owner]
    end

    subgraph Audience[" Audience Pipeline"]
        AP[Audience Push]
        PROC[Campaign Processor]
        DFD[Data File Download]
    end

    subgraph Processing[" Processing Pipeline"]
        EE[Event Enrichment]
        TV[Time Validation]
        TC[Test Campaign]
    end

    subgraph Exclusion[" Exclusion Services"]
        CGX[CG Exclusion\nSpEL Rules]
        ESC[Exclusion Scan\nCSV Lists]
    end

    subgraph External[" External"]
        AM[(Audience Manager)]
        AUTH[(Auth Manager)]
        CONT[(Content Manager)]
        CH[Channel Partners\nSMS·Email·WA·RCS·Push]
    end

    CM --> AP
    AP --> AM
    AM -->|callback| PROC
    PROC -->|Kafka| DFD
    DFD --> EE
    EE --> TV
    TV --> CH
    TV --> CGX
    TV --> ESC
    EE --> CGX
    EE --> ESC
    EE --> CONT
    TV --> CONT
    AP --> AUTH
    DFD --> AUTH
    classDef kafka fill:#f39c12,color:#000,stroke:#d68910
    classDef svc fill:#2980b9,color:#fff,stroke:#1f618d
    classDef ext fill:#8e44ad,color:#fff,stroke:#6c3483
    classDef channel fill:#d35400,color:#fff,stroke:#a04000
    class CH channel
    class AM ext
    class EE kafka
    class AP,AUTH,CGX,CM,CONT,DFD,ESC,PROC,TC,TV svc
```

---

## Tech Stack

| Layer | Technology |
|-------|-----------|
| Language | Java 17 |
| Frameworks | Spring Boot 3.5.6 / 4.0.x |
| Messaging | Apache Kafka |
| Database | Oracle (primary) · MySQL (secondary) |
| ORM | Spring Data JPA + Hibernate |
| HTTP Clients | RestTemplate · OpenFeign · WebClient |
| Cloud Storage | AWS S3 SDK 2.25.56 |
| Resilience | Resilience4j 2.x (circuit breaker + retry) |
| Observability | OpenTelemetry 1.38.0 · Prometheus |
| Caching | Caffeine |
| Concurrency | Virtual Threads (Project Loom) |
| Rule Engine | Spring Expression Language (SpEL) |
| Container | Docker · Kubernetes |
