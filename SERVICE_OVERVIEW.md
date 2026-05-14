# UCLM — What Does Each Service Need?

> Plain-English summary of every service: what it does, what it depends on, and how it runs.  
> Last updated: 2026-05-12

---

## comms / uclm-campaign-cg-exclusion

**What it does:**  
Manages exclusion rules for contact groups — decides which customer groups should be excluded from campaigns for a given tenant.

**Needs:**
- 🗄️ **Oracle DB** — stores and queries exclusion rules (`tenant_id` scoped)

**Does NOT need:**
- Kafka, Redis, Aerospike, any other service

**Runs as:** 1 pod · global (all tenants share one instance)

---

## comms / uclm-campaign-time-validation

**What it does:**  
REST API that validates whether a campaign can run at a given time — checks timing rules, channel windows, and eligibility per tenant and workspace.

**Needs:**
- 🗄️ **Oracle DB** or **MySQL** (switchable via profile)
- 🔗 **ContentManager Service** (Feign)
- 🔗 **Governance Service** (Feign)
- 🔗 **TGCG Service** (Feign)
- 🔗 **SubGoal Service** (Feign)
- 🔗 **Exclusion Service** (Feign)
- 🔗 **Media Service** (Feign)
- 🔗 **Goal Service** (Feign)
- 🔗 **TenantConfig Service** (RestTemplate)
- 📨 **Kafka** — consumes and produces timing validation events

**Does NOT need:**
- Aerospike, MongoDB, AWS S3

**Runs as:** 1 pod · global · tenant + workspace scoped at runtime (via `x-tenant-id` / `x-workspace-id` headers)

---

## comms / uclm-campaign-manager

**What it does:**  
The core campaign management service — create, approve, schedule, and manage the full lifecycle of campaigns across all channels (SMS, Email, Push, WhatsApp, RCS).

**Needs:**
- 🗄️ **Oracle DB** or **MySQL** (switchable)
- 📨 **Kafka** — consumes `orchestrator.request` (approvals), produces `uclm_analytics`
- 🌐 **CMS API** — `https://cms-dev.airtel.com` (DEV) / internal cluster URL (UAT/PROD)
- 🔗 **ContentManager Service** (Feign)
- 🔗 **Media Filter Service** (Feign)
- 🔗 **CohortConfig Service** (Feign)
- 🔗 **CampaignConfig Service** (Feign)
- 🔗 **CmsAuth Service** (Feign)
- 📧 **SMTP** — for email notifications

**Does NOT need:**
- Aerospike, MongoDB, AWS S3

**Runs as:** 2 pods (auto-scales 2–6) · Helm managed · global · all channels in one pod

---

## comms / uclm-test-campaign

**What it does:**  
Validates and runs test/dry-run campaigns before going live — same validation logic as campaign-time-validation but scoped to test campaign flows.

**Needs:**
- 🗄️ **Oracle DB** or **MySQL** (switchable)
- 🔗 **ContentManager Service** (Feign)
- 🔗 **Governance Service** (Feign)
- 🔗 **SubGoal Service** (Feign)
- 🔗 **Media Service** (Feign)
- 🔗 **Goal Service** (Feign)
- 📨 **Kafka** — consumes and produces test campaign events

**Does NOT need:**
- Aerospike, MongoDB, AWS S3

**Runs as:** 2 pods · global · tenant + workspace scoped at runtime

---

## comms / uclm-campaign-exclusion-scan

**What it does:**  
A batch utility that scans and loads exclusion data from CSV files into the system. No network dependencies — purely file-based.

**Needs:**
- 📁 **Local file system** — reads from `INGEST_BASE_FOLDER`

**Does NOT need:**
- Kafka, any DB, any other service

**Runs as:** 1 pod · global · no business scope (utility)

---

## comms / uclm-campaign-audience-push

**What it does:**  
Scheduled service that pushes audience data to the Audience Management system for Push channel campaigns. Iterates all configured tenants on a schedule.

**Needs:**
- 🗄️ **Oracle DB** or **MySQL** (switchable)
- 🔗 **TenantConfig Service** (WebClient)
- 🔗 **AudienceManagement Service** (WebClient)

**Does NOT need:**
- Kafka, Aerospike, MongoDB, AWS S3

**Runs as:** 1 pod · Helm managed · Push channel only · iterates all configured tenant IDs

---

## comms / uclm-campaign-processor

**What it does:**  
Downloads and processes audience control files from AWS S3 for campaigns, then publishes processing results to Kafka. Handles large files up to 500 MB.

**Needs:**
- 🗄️ **Oracle DB** or **MySQL** (switchable)
- ☁️ **AWS S3** — reads/writes audience control files (up to 500 MB)
- 📨 **Kafka** — consumes and produces `control_file_request` topic

**Does NOT need:**
- Aerospike, MongoDB, other services

**Runs as:** 1 pod · global · tenant + campaign scoped

---

## comms / uclm-campaign-manager-event-enrichment

**What it does:**  
Kafka consumer that enriches campaign dispatch events with campaign metadata — looks up campaign details, audience records, and channel info before passing events downstream.

**Needs:**
- 🗄️ **Oracle DB** or **MySQL** (switchable)
- 📨 **Kafka** — consumes inbound events, produces enriched events
- 🔗 **TGCG Service** (Feign)
- 🔗 **SubGoal Service** (Feign)
- 🔗 **Exclusion Service** (Feign)
- 🔗 **Audience Service** (Feign)
- 🔗 **Media Service** (Feign)
- 🔗 **Goal Service** (Feign)
- 🔗 **ContentManager Service** (Feign)

**Does NOT need:**
- Aerospike, MongoDB, AWS S3

**Runs as:** 1 pod · global · processes individual audience records per tenant + channel + campaign

---

## comms / uclm-analytics-reporting-service

**What it does:**  
Consumes campaign analytics events from Kafka and aggregates them into reporting data per tenant, workspace, and channel.

**Needs:**
- 🗄️ **Oracle DB** or **MySQL** (switchable)
- 📨 **Kafka** — consumes `uclm_analytics`, `dimension_refresh_topic`, `uclm_campaign_status`

**Does NOT need:**
- Aerospike, MongoDB, AWS S3, other services

**Runs as:** 1 pod · global · tenant + workspace + channel scoped from Kafka events

---

## comms / uclm-campaign-data-file-dowload

**What it does:**  
REST API (reactive/WebFlux) that lets clients download campaign audience data files stored in AWS S3.

**Needs:**
- 🗄️ **Oracle DB** or **MySQL** (switchable)
- ☁️ **AWS S3** — stores and serves campaign data files

**Does NOT need:**
- Kafka, Aerospike, MongoDB, other services

**Runs as:** 1 pod · global · tenant + campaign scoped

---

## bummlebee / uclm-validation-governance-service

**What it does:**  
Validates every outgoing message against governance rules — checks bounce lists, unsubscribe lists, and kill-campaign flags before a message is dispatched.

**Needs:**
- 🔴 **Aerospike** — stores bounce data, unsubscribe data, kill-campaign flags
  - Sets: `bounce_data` · `unsubs_data` · `kill_campaign_data`
- 📨 **Kafka** — consumes `event`, `exceptions`, `apb-exceptions` · produces `exceptions`, `apb-exceptions`, `cs_raw_reporting_topic`

**Does NOT need:**
- Oracle, MySQL, MongoDB, AWS S3, other services

**Runs as:** **10 pods — one per channel** (`SMS-IQ`, `SMS-Lobby`, `Email`, `Email-SMTP`, `Email-Attachment`, `Push`, `RCS`, `WhatsApp`, `D2C`) · each pod is fully isolated

---

## bummlebee / uclm-dlr-aerospike-cache-loader

**What it does:**  
Loads Delivery Report (DLR) enrichment data into Aerospike cache for each channel — this cache is later used by the DLR enricher to enrich raw delivery receipts.

**Needs:**
- 🔴 **Aerospike** — writes enrichment data per channel into channel-specific sets
  - SMS → `uat_iq_sms_main` · Email → `uat_email_netcore_main` · Push → `uat_push_main` · RCS → `uat_rcs_main` · WhatsApp → `uat_iq_wa_main`
- 📨 **Kafka** — consumes channel-specific DLR topics (e.g. `channel_partner_sms_nrt_svc_succ`)
- 🔐 **Kerberos** (SCRAM) — for Aerospike authentication

**Does NOT need:**
- Oracle, MySQL, MongoDB, AWS S3, other services

**Runs as:** 1 pod per channel · channel selected via `SPRING_PROFILES_ACTIVE` (e.g. `sms-uat`) · 5 channels: `SMS` `Email-Netcore` `Push` `RCS` `WhatsApp`

---

## bummlebee / uclm-orchestrator-service

**What it does:**  
The actual message dispatcher — reads a dispatch request from Kafka and sends the message to the appropriate external channel provider (SMS, Email, WhatsApp, RCS).

**Needs:**
- 📨 **Kafka** — consumes `dispatch` + `orchestrator-exceptions` · produces `dispatch-response`, `apb-log`, `apb-success-log`, `analytics`, `orchestrator-exceptions`
- 🌐 **Email API (Netcore)** — `https://emailapi.netcorecloud.net`
- 🌐 **SMS API (Airtel IQ)** — `https://iqsms.airtel.in`
- 🌐 **WhatsApp API (Airtel IQ)** — `https://iqwhatsapp.airtel.in`
- 🌐 **RCS API (Airtel IQ)** — `https://www.iqconversation.airtel.in`
- 🌐 **SMS Lobby** — `http://10.92.230.97:10200/cgi-bin/sendsms`
- 🌐 **Push (APB)** — `https://channels-connect-api-apb.prd.adl.internal`
- 🔗 **campaign-manager** (Feign) — calls `/counter/decrement` on each dispatch

**Does NOT need:**
- Oracle, MySQL, MongoDB, Aerospike, AWS S3

**Runs as:** **5 pods — one per channel** (`SMS-IQ`, `SMS-Lobby`, `Email-SMTP`, `Email-Netcore`, `RCS`) · each Helm release enables only one channel

---

## bummlebee / uclm-dlr-api-service

**What it does:**  
REST API that receives Delivery Report callbacks from external channel providers and publishes raw DLR events to Kafka for downstream processing.

**Needs:**
- 📨 **Kafka** — produces raw DLR events per channel
- 🔴 **Aerospike** — lookup for DLR metadata

**Does NOT need:**
- Oracle, MySQL, MongoDB, AWS S3, other services

**Runs as:** 1 pod per channel · channel selected via `SPRING_PROFILES_ACTIVE` · 3 channels: `SMS` `Email-Netcore` `RCS`

---

## bummlebee / uclm-rate-controller-service

**What it does:**  
Rate-limits outgoing messages per team and channel — enforces TPS (transactions per second) caps so no team can flood a channel. Acts as a throttle gate before dispatch.

**Needs:**
- 🔴 **Aerospike** — stores rate-limit counters per team + channel
- 🗄️ **PostgreSQL** or **Oracle** or **MySQL** (switchable via profile)
- 📨 **Kafka** — consumes `channel-partner-rate-controller-input` / `comms-input` · produces `exceptions`

**Does NOT need:**
- MongoDB, AWS S3, other services

**Runs as:** 1 pod · global · scoped by **team + channel + tenant** at runtime  
**Rate limits:** Global TPS = `2000` · Per-tenant TPS = `5` (default)

---

## bummlebee / uclm-dlr-enricher

**What it does:**  
Consumes raw DLR events from Kafka, looks up sender/recipient metadata from Aerospike, and publishes enriched DLR events back to Kafka for analytics and reporting.

**Needs:**
- 🔴 **Aerospike** — looks up enrichment data per channel
- 📨 **Kafka** — consumes raw DLR topics (e.g. `comms_iq_sms_dlr_raw`) · produces enriched topics (e.g. `comms_iq_sms_dlr_enriched`)

**Does NOT need:**
- Oracle, MySQL, MongoDB, AWS S3, other services

**Runs as:** 1 pod per channel · channel selected via `SPRING_PROFILES_ACTIVE` · 5 channels: `SMS` `Email-Netcore` `Push` `RCS` `WhatsApp`

---

## user-management / uclm-contentmgmt

**What it does:**  
Manages all campaign content — templates, media, creative assets — per tenant, workspace, and channel. Also consumes analytics events to track content performance.

**Needs:**
- 🍃 **MongoDB** — stores all content templates and media assets
- 📨 **Kafka** — consumes `uclm_analytics` · produces `content_change_log`
- 🔒 **ShedLock** (via MongoDB) — distributed locking for scheduled jobs

**Does NOT need:**
- Oracle, MySQL, Aerospike, AWS S3, other services

**Runs as:** 1 pod · global · tenant + workspace + channel scoped per request  
**Channels:** `SMS` · `RCS` · `WhatsApp` · `Push`

---

## user-management / uclm-auth-manager

**What it does:**  
Authentication and authorisation service for the entire UCLM platform — manages tenants, workspaces, users, roles, org hierarchy, JWT tokens, and sessions.

**Needs:**
- 🗄️ **Oracle DB** or **MySQL** or **Aurora** (switchable via profile)
- 🔴 **Aerospike** — session/user cache (`namespace: arch`, host: `10.5.247.156:3000`)
- 🔗 **uclm-test-campaign** (REST) — notifies test campaign service on certain auth events

**Does NOT need:**
- Kafka, MongoDB, AWS S3

**Runs as:** 1 pod · global · tenant + workspace + user scoped  
**Session config:** 300 s inactivity timeout · JWT validity: 360,000 ms
