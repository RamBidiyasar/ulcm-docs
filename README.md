# UCLM Documentation

> **Unified Customer Lifecycle Management (UCLM)** — a multi-tenant, multi-channel marketing campaign platform.  
> This repository contains all High-Level Design (HLD), Low-Level Design (LLD), and service reference documentation.

---

## Table of Contents

- [Platform Overview](#platform-overview)
- [Top-Level Docs](#top-level-docs)
- [comms-hld/ — Campaign Lifecycle Domain](#comms-hld--campaign-lifecycle-domain)
- [bummlebee-hld/ — Message Delivery Domain](#bummlebee-hld--message-delivery-domain)
- [user-management-hld/ — Identity & Content Domain](#user-management-hld--identity--content-domain)

---

## Platform Overview

UCLM is divided into **three domains**:

| Domain | Folder | Responsibility |
|--------|--------|----------------|
| **Comms** | `comms-hld/` | End-to-end campaign lifecycle — creation, audience processing, enrichment, validation |
| **Bummlebee** | `bummlebee-hld/` | Kafka-native message delivery — rate limiting, governance, dispatch, DLR tracking |
| **User Management** | `user-management-hld/` | Authentication, RBAC, org hierarchy, content templates and media assets |

---

## Top-Level Docs

These files sit at the root and cover the whole platform.

| File | What it covers |
|------|---------------|
| [`DOMAINS_AND_SERVICES.md`](./DOMAINS_AND_SERVICES.md) | One-line description of every domain and every service — the quickest way to understand what each piece does |
| [`SERVICE_DETAILS.md`](./SERVICE_DETAILS.md) | Detailed write-up for all 18 services — purpose, internal flow, key components, data stores, Kafka topics, APIs, deployment notes |
| [`SERVICE_OVERVIEW.md`](./SERVICE_OVERVIEW.md) | Plain-English summary per service: what it does, what it needs, what it does NOT need, and how it runs |
| [`SERVICE_SCOPE.md`](./SERVICE_SCOPE.md) | Scoping table — which dimension (tenant / workspace / channel / team) each service is scoped by and the possible values |
| [`SERVICE_DEPENDENCIES.md`](./SERVICE_DEPENDENCIES.md) | Full dependency map — which services call which, over what protocol, and why |
| [`SERVICE_PROCESSING_LEVELS.md`](./SERVICE_PROCESSING_LEVELS.md) | Processing level classification for every service (per-message, per-campaign, batch, scheduled, utility) |
| [`SERVICE_DEPLOYMENT.md`](./SERVICE_DEPLOYMENT.md) | Deployment topology — pod counts, Helm release strategy, channel-per-pod model, Spring profile selection |
| [`HLD-FULL-SYSTEM.md`](./HLD-FULL-SYSTEM.md) | Full platform HLD — architecture overview, module breakdown, technology stack, data stores, messaging infrastructure, non-functional characteristics |
| [`HLD-DETAILED-FLOW.md`](./HLD-DETAILED-FLOW.md) | Detailed end-to-end flow from campaign creation through audience push, CSV streaming, validation, and Bummlebee dispatch |
| [`HLD-SCHEDULED-CAMPAIGN.md`](./HLD-SCHEDULED-CAMPAIGN.md) | HLD for the **ONETIME / RECURRING** campaign path — three Airflow DAGs, audience file acquisition, parallel download, 7-stage per-record time-validation pipeline |
| [`HLD-NRT-CAMPAIGN.md`](./HLD-NRT-CAMPAIGN.md) | HLD for the **EVENT / NRT** campaign path — real-time Kafka-triggered enrichment, LOB-based PrimaryId resolution, per-subscriber AM API call, inline 8-stage dispatch pipeline |
| [`UCLM_SYSTEM_DEPENDENCIES.md`](./UCLM_SYSTEM_DEPENDENCIES.md) | External system dependency inventory — Audience Manager, CMS, Airflow, channel providers, and more |
| [`UCLM_DEPLOYMENT_STRATEGIES.md`](./UCLM_DEPLOYMENT_STRATEGIES.md) | Deployment strategies, environment matrix (DEV / UAT / PROD), Helm values, OCP / Kubernetes setup |

---

## comms-hld/ — Campaign Lifecycle Domain

Cross-cutting HLD documents for the **Comms** domain.

| File | What it covers |
|------|---------------|
| [`README.md`](./comms-hld/README.md) | Index and guide to all Comms HLD documents |
| [`00-services-overview.md`](./comms-hld/00-services-overview.md) | One-page summary of all Comms services and how they relate |
| [`01-end-to-end-flow.md`](./comms-hld/01-end-to-end-flow.md) | Full end-to-end campaign execution flow across all Comms services |
| [`02-kafka-flows.md`](./comms-hld/02-kafka-flows.md) | All Kafka topics, producers, consumers and payload shapes within Comms |
| [`03-state-machine.md`](./comms-hld/03-state-machine.md) | Campaign state machine — all states, transitions, and the service responsible for each transition |
| [`04-rest-api-calls.md`](./comms-hld/04-rest-api-calls.md) | All REST API calls between Comms services and to external systems |
| [`05-database-map.md`](./comms-hld/05-database-map.md) | Database schema map — tables, owners, key columns, and which service reads/writes each |
| [`06-service-details.md`](./comms-hld/06-service-details.md) | Deep-dive technical detail for each Comms service |
| [`07-deployment-guide.md`](./comms-hld/07-deployment-guide.md) | Deployment steps, Helm values, environment variables, and rollout order for Comms services |
| [`08-rbac-access-control.md`](./comms-hld/08-rbac-access-control.md) | RBAC model — roles, permissions, and how `x-user-hierarchy` headers gate Comms APIs |
| [`09-er-diagram.md`](./comms-hld/09-er-diagram.md) | Entity-Relationship diagram for the Comms Oracle schema |
| [`10-airflow-dags.md`](./comms-hld/10-airflow-dags.md) | Airflow DAG specifications — DAG 1 (state transition), DAG 2 (audience push), DAG 3 (time validation trigger) |
| [`11-critical-bugs.md`](./comms-hld/11-critical-bugs.md) | Known critical bugs, root causes, and applied or pending fixes |

**Per-service HLD files:**

| File | Service |
|------|---------|
| [`hld-01-campaign-manager.md`](./comms-hld/hld-01-campaign-manager.md) | `uclm-campaign-manager` — campaign CRUD, state ownership, approval workflow |
| [`hld-02-audience-push.md`](./comms-hld/hld-02-audience-push.md) | `uclm-campaign-audience-push` — scheduled audience file creation at Audience Manager |
| [`hld-03-campaign-processor.md`](./comms-hld/hld-03-campaign-processor.md) | `uclm-campaign-processor` — ctrl.gz download, part-file URL extraction, Kafka publish |
| [`hld-04-data-file-download.md`](./comms-hld/hld-04-data-file-download.md) | `uclm-campaign-data-file-download` — parallel part-file download, validation, storage |
| [`hld-05-event-enrichment.md`](./comms-hld/hld-05-event-enrichment.md) | `uclm-campaign-manager-event-enrichment` — per-subscriber metadata enrichment for scheduled campaigns |
| [`hld-06-time-validation.md`](./comms-hld/hld-06-time-validation.md) | `uclm-campaign-time-validation` — CSV streaming, 7-stage per-record validation, dispatch |
| [`hld-07-cg-exclusion.md`](./comms-hld/hld-07-cg-exclusion.md) | `uclm-campaign-cg-exclusion` — SpEL-based contact group exclusion rule evaluation |
| [`hld-08-exclusion-scan.md`](./comms-hld/hld-08-exclusion-scan.md) | `uclm-campaign-exclusion-scan` — CSV-based EMPLOYEE / VIP / RETAILER exclusion lookup |
| [`hld-09-test-campaign.md`](./comms-hld/hld-09-test-campaign.md) | `uclm-test-campaign` — pre-production dry-run mirror of time-validation |
| [`hld-10-analytics-reporting.md`](./comms-hld/hld-10-analytics-reporting.md) | `uclm-analytics-reporting-service` — Kafka-driven analytics aggregation and reporting |

---

## bummlebee-hld/ — Message Delivery Domain

Cross-cutting HLD documents for the **Bummlebee** delivery engine.

| File | What it covers |
|------|---------------|
| [`README.md`](./bummlebee-hld/README.md) | Index and guide to all Bummlebee HLD documents |
| [`00-services-overview.md`](./bummlebee-hld/00-services-overview.md) | One-page summary of all Bummlebee services and the two internal pipelines (Dispatch + DLR) |
| [`01-end-to-end-flow.md`](./bummlebee-hld/01-end-to-end-flow.md) | Full end-to-end flow from Comms Kafka handoff through dispatch and DLR tracking |
| [`02-kafka-flows.md`](./bummlebee-hld/02-kafka-flows.md) | All Kafka topics, producers, consumers and payload shapes within Bummlebee |
| [`03-state-machine.md`](./bummlebee-hld/03-state-machine.md) | Message delivery state machine — from rate-controlled input to enriched DLR |
| [`04-rest-api-calls.md`](./bummlebee-hld/04-rest-api-calls.md) | All outbound REST calls to channel providers (Airtel IQ SMS/WA/RCS, Netcore Email, FCM Push) |
| [`05-database-map.md`](./bummlebee-hld/05-database-map.md) | Aerospike namespace / set map — what each service stores and reads |
| [`06-service-details.md`](./bummlebee-hld/06-service-details.md) | Deep-dive technical detail for each Bummlebee service |
| [`07-deployment-guide.md`](./bummlebee-hld/07-deployment-guide.md) | Deployment steps, Spring profile selection per channel, Helm values, pod-per-channel model |
| [`08-full-services-diagram.md`](./bummlebee-hld/08-full-services-diagram.md) | Full Bummlebee architecture diagram covering both Dispatch and DLR pipelines |

**Per-service HLD files:**

| File | Service |
|------|---------|
| [`hld-01-rate-controller.md`](./bummlebee-hld/hld-01-rate-controller.md) | `uclm-rate-controller-service` — per-team/channel TPS throttle gate, Resilience4j, Aerospike counters |
| [`hld-02-validation-governance.md`](./bummlebee-hld/hld-02-validation-governance.md) | `uclm-validation-governance-service` — bounce/unsubscribe/kill-campaign checks against Aerospike |
| [`hld-03-orchestrator.md`](./bummlebee-hld/hld-03-orchestrator.md) | `uclm-orchestrator-service` — message dispatch to external channel provider APIs |
| [`hld-04-dlr-api-service.md`](./bummlebee-hld/hld-04-dlr-api-service.md) | `uclm-dlr-api-service` — HTTP webhook receiver for DLR callbacks from channel providers |
| [`hld-05-aerospike-cache-loader.md`](./bummlebee-hld/hld-05-aerospike-cache-loader.md) | `uclm-dlr-aerospike-cache-loader` — writes dispatch outcome records into Aerospike for DLR correlation |
| [`hld-06-dlr-enricher.md`](./bummlebee-hld/hld-06-dlr-enricher.md) | `uclm-dlr-enricher` — joins raw DLRs with Aerospike dispatch records, publishes enriched DLRs |

---

## user-management-hld/ — Identity & Content Domain

Cross-cutting HLD documents for the **User Management** domain.

| File | What it covers |
|------|---------------|
| [`README.md`](./user-management-hld/README.md) | Index and guide to all User Management HLD documents |
| [`00-services-overview.md`](./user-management-hld/00-services-overview.md) | One-page summary of `uclm-auth-manager` and `uclm-contentmgmt` and how they interact |
| [`01-end-to-end-flow.md`](./user-management-hld/01-end-to-end-flow.md) | Full login → JWT issuance → downstream header propagation flow |
| [`02-rest-api-calls.md`](./user-management-hld/02-rest-api-calls.md) | All REST API endpoints exposed by User Management services |
| [`03-state-machine.md`](./user-management-hld/03-state-machine.md) | Content approval workflow state machine — DRAFT → L1_PENDING → L2_PENDING → APPROVED |
| [`04-database-map.md`](./user-management-hld/04-database-map.md) | Oracle (auth-manager) and MongoDB (contentmgmt) schema maps |
| [`05-service-details.md`](./user-management-hld/05-service-details.md) | Deep-dive technical detail for both User Management services |
| [`06-saml-sso-flow.md`](./user-management-hld/06-saml-sso-flow.md) | SAML 2.0 SSO flow — IdP-initiated and SP-initiated login sequences, assertion validation, JWT issuance |
| [`07-deployment-guide.md`](./user-management-hld/07-deployment-guide.md) | Deployment steps, environment variables, Aerospike session config, JWT secret rotation |

**Per-service HLD files:**

| File | Service |
|------|---------|
| [`hld-01-auth-manager.md`](./user-management-hld/hld-01-auth-manager.md) | `uclm-auth-manager` — SAML SSO, JWT, RBAC, org hierarchy, workspace switching, session TTL |
| [`hld-02-contentmgmt.md`](./user-management-hld/hld-02-contentmgmt.md) | `uclm-contentmgmt` — template and media management, L1/L2 approval, WhatsApp/RCS template registration |
