# UCLM — Domains & Services

> Quick-reference guide: what every domain does and what every service does in one line.  
> Last updated: 2026-05-14

---

## Domains

### 👤 User Management
Handles **identity, access control, and content** for the platform.  
Every other service trusts the JWT headers (`x-tenant-id`, `x-workspace-id`, `x-user-id`) that this domain issues. It also owns all campaign content templates and media assets.

### 📣 Comms (Campaign Lifecycle)
Manages the **end-to-end campaign journey** — from a marketer creating a campaign in the UI, through audience processing and per-subscriber enrichment, all the way to placing validated, ready-to-send events on Kafka for Bummlebee to dispatch.

### 🐝 Bummlebee (Message Delivery Engine)
A **Kafka-native delivery pipeline** that picks up pre-validated events from Comms, enforces rate limits and governance rules, dispatches messages to external channel providers (SMS, Email, WhatsApp, RCS, Push), and tracks delivery receipts (DLRs) back through the system.

---

## Services

### 👤 User Management

| Service | What it does |
|---------|-------------|
| **uclm-auth-manager** | Authentication & authorisation hub — issues JWT tokens, manages tenants, workspaces, users, roles, org hierarchy, and sessions (300 s TTL in Aerospike). |
| **uclm-contentmgmt** | Stores and serves all campaign content — templates, media, creative bundles — per tenant/workspace/channel; tracks content performance via analytics Kafka events. |

---

### 📣 Comms (Campaign Lifecycle)

| Service | What it does |
|---------|-------------|
| **uclm-campaign-manager** | Core campaign CRUD service — create, approve, schedule, and track the full lifecycle of campaigns across all channels; owns campaign state. |
| **uclm-campaign-audience-push** | Scheduled service that triggers audience file generation at the external Audience Management system for Push channel campaigns. |
| **uclm-campaign-processor** | Downloads and parses the audience control file (`.ctrl.gz`, up to 500 MB) from AWS S3 and publishes individual subscriber records to Kafka. |
| **uclm-campaign-data-file-dowload** | REST API (reactive) that lets clients download audience part-files stored in AWS S3. |
| **uclm-campaign-manager-event-enrichment** | Kafka consumer that decorates each per-subscriber dispatch event with campaign metadata, templates, audience info, and exclusion check results. |
| **uclm-campaign-time-validation** | 7-stage REST/Kafka validation pipeline that checks timing windows, channel eligibility, governance rules, and exclusions before forwarding events downstream. |
| **uclm-campaign-cg-exclusion** | Evaluates SpEL-based rules to decide which contact groups (control/target) should be excluded from a campaign for a given tenant. |
| **uclm-campaign-exclusion-scan** | Batch utility that loads employee/VIP/retailer exclusion lists from local CSV files into the system — no network dependencies. |
| **uclm-test-campaign** | Pre-production mirror of `campaign-time-validation` used to safely validate and dry-run campaigns before going live. |
| **uclm-analytics-reporting-service** | Consumes campaign analytics events from Kafka and aggregates delivery, click, and conversion metrics per tenant/workspace/channel for reporting. |

---

### 🐝 Bummlebee (Message Delivery Engine)

| Service | What it does |
|---------|-------------|
| **uclm-rate-controller-service** | Throttle gate — enforces per-team/per-channel TPS caps (global max 2 000 TPS) using Resilience4j rate limiters backed by Aerospike counters. |
| **uclm-validation-governance-service** | Pre-dispatch compliance check — validates each message against bounce lists, unsubscribe lists, and kill-campaign flags stored in Aerospike before allowing dispatch. |
| **uclm-orchestrator-service** | The actual message sender — reads dispatch requests from Kafka and calls the correct external provider API (Airtel IQ SMS/WA/RCS, Netcore Email, FCM Push). |
| **uclm-dlr-api-service** | HTTP webhook receiver — accepts delivery report callbacks from channel providers and publishes raw DLR events to Kafka for downstream processing. |
| **uclm-dlr-aerospike-cache-loader** | Consumes dispatch outcome records from Kafka and writes them into Aerospike so the DLR enricher can correlate delivery receipts with original sends. |
| **uclm-dlr-enricher** | Joins raw DLR events (from Kafka) with original dispatch records (from Aerospike) and publishes fully enriched DLR events for analytics and reporting. |
