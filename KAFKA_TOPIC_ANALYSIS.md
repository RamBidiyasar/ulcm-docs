# UCLM Kafka Topic Analysis

> Generated: 2026-05-20 · Updated: 2026-05-27
> Covers: `bummlebee`, `comms`, `user-management` repos

> **Channel abbreviations used in topic names:**
> `sms` = SMS · `wa` / `whatsapp` = WhatsApp · `push` = Push Notification · `rcs` = RCS · `eml` / `email` = Email
> *(legacy topics already deployed use `wa` for WhatsApp and `eml` for Email — noted inline)*

> **Registry source:** `UCLM_Kafka_Topic_Registry.xlsx` — authoritative provisioned topic list.
> **Service name mapping:** The registry uses operational deployment names (e.g. "SMS NRT Service", "Email SMTP NRT Service"). These are **not** separate repositories — they are per-channel deployment instances of the actual service repos listed below. See `DOMAINS_AND_SERVICES.md` for the canonical service list.

---

## Part 1 — Service-wise Kafka Topics

> Each section header is the **actual repo/service name** from `DOMAINS_AND_SERVICES.md`.
> The *Registry name* column shows the label used in `UCLM_Kafka_Topic_Registry.xlsx` for cross-reference.

---

### 🐝 BUMMLEBEE Services

---

#### `uclm-validation-governance-service`

> **Repo:** `bummlebee` · **Registry name:** "Validation Governance Service" · **Deployed:** per channel

| Topic | Type | Consumer Group | Notes |
|-------|------|----------------|-------|
| `channel_partner_val_gov_email_svc_input` | input | `val-gov-consumer-group` | Inbound Email messages for validation & governance |
| `channel_partner_val_gov_push_svc_input` | input | `val-gov-consumer-group` | Inbound Push messages for validation & governance |
| `channel_partner_val_gov_rcs_svc_input` | input | `val-gov-consumer-group` | Inbound RCS messages for validation & governance |
| `channel_partner_val_gov_whatsapp_svc_input` | input | `val-gov-consumer-group` | Inbound WhatsApp messages for validation & governance |
| `channel_partner_val_gov_svc_input` | input | `val-gov-consumer-group` | Generic / fallback valgov input |

*Also produces to:* `channel_partner_sms_endpoint` · `channel_partner_whatsapp_endpoint` · `channel_partner_push_endpoint` · `channel_partner_rcs_endpoint` · `channel_partner_email_endpoint` · `dispatch` *(fallback/shared)* · `cs_raw_reporting_topic` · `exceptions` · `apb-exceptions` · `d2c-clm-sit` *(external Kafka, D2C/FS only)*

---

#### `uclm-orchestrator-service`

> **Repo:** `bummlebee` · **Channels:** SMS · WhatsApp · Email · Push · RCS
> One deployment instance per channel — there is **no separate repository** per channel; it is always `uclm-orchestrator-service`.

*Consumes from:* `channel_partner_sms_endpoint` · `channel_partner_whatsapp_endpoint` · `channel_partner_push_endpoint` · `channel_partner_rcs_endpoint` · `channel_partner_email_endpoint` · `dispatch` *(fallback/shared)*

| Channel | Topic | Type | Consumer Group | Notes |
|---------|-------|------|----------------|-------|
| **SMS** | `channel_partner_sms_nrt_svc_endpoint` | output | `sms-nrt-consumer-group` | Dispatched to SMS vendor endpoint |
| | `channel_partner_sms_nrt_svc_succ` | output | `sms-nrt-consumer-group` | Successfully delivered SMS |
| | `channel_partner_sms_nrt_svc_err` | dlq | `sms-nrt-consumer-group` | Failed / errored SMS messages |
| **WhatsApp** | `channel_partner_wa_nrt_svc_endpoint` | output | `wa-nrt-consumer-group` | Dispatched to WA vendor endpoint *(legacy `wa`)* |
| | `channel_partner_wa_nrt_svc_succ` | output | `wa-nrt-consumer-group` | Successfully delivered WA message |
| | `channel_partner_wa_nrt_svc_err` | dlq | `wa-nrt-consumer-group` | Failed / errored WA messages |
| **Email** | `channel_partner_eml_nrt_svc_endpoint` | output | `email-nrt-consumer-group` | Dispatched to Email vendor endpoint *(legacy `eml`)* |
| | `channel_partner_eml_nrt_svc_succ` | output | `email-nrt-consumer-group` | Successfully delivered Email |
| | `channel_partner_eml_nrt_svc_err` | dlq | `email-nrt-consumer-group` | Failed / errored Email messages |
| **Push** | `channel_partner_push_nrt_svc_endpoint` | output | `push-nrt-consumer-group` | Dispatched to Push vendor endpoint |
| | `channel_partner_push_nrt_svc_succ` | output | `push-nrt-consumer-group` | Successfully delivered Push |
| | `channel_partner_push_nrt_svc_err` | dlq | `push-nrt-consumer-group` | Failed / errored Push messages |
| **RCS** | `channel_partner_rcs_nrt_svc_endpoint` | output | `rcs-nrt-consumer-group` | Dispatched to RCS vendor endpoint |
| | `channel_partner_rcs_nrt_svc_succ` | output | `rcs-nrt-consumer-group` | Successfully delivered RCS |
| | `channel_partner_rcs_nrt_svc_err` | dlq | `rcs-nrt-consumer-group` | Failed / errored RCS messages |

*Also produces to:* `channel_partner_sms_nrt_svc_succ` · `wa_main_service` · `channel_partner_push_nrt_svc_succ` · `channel_partner_rcs_nrt_svc_succ` · `channel_partner_email_nrt_svc_succ` · `channel_partner_sms_err` · `channel_partner_whatsapp_err` · `channel_partner_push_err` · `channel_partner_rcs_err` · `channel_partner_email_err` · `cs_raw_reporting_topic` · `orchestrator-exceptions`

---

#### `uclm-dlr-enricher`

> **Repo:** `bummlebee` · **Deployed:** one instance per channel (SMS · RCS · WhatsApp · Email/Netcore)

*Also consumes:* `iq_channel_dlr_raw` · `dlr-retry-topic` *(self-loop)*

| Deployment | Registry name | Topic | Type | Consumer Group | Notes |
|------------|--------------|-------|------|----------------|-------|
| **SMS** | "DLR Enricher – SMS" | `comms_iq_sms_dlr_enriched` | output | `dlr-enricher-group` | Enriched SMS DLR events |
| | | `comms_iq_sms_dlr_enriched_retrial` | retry | `dlr-enricher-group` | SMS DLR enrichment retry |
| | | `comms_iq_sms_dlr_enriched_dlq` | dlq | `dlr-enricher-group` | SMS DLR enrichment DLQ |
| **RCS** | "DLR Enricher – RCS" | `comms_iq_rcs_dlr_enriched` | output | `iq-rcs-dlr-enricher-group` | Enriched RCS DLR events |
| | | `comms_iq_rcs_dlr_enriched_retrial` | retry | `iq-rcs-dlr-enricher-group` | RCS DLR enrichment retry |
| | | `comms_iq_rcs_dlr_enriched_dlq` | dlq | `iq-rcs-dlr-enricher-group` | RCS DLR enrichment DLQ |
| **WhatsApp** | "DLR Enricher – WhatsApp" | `comms_iq_wa_dlr_enriched` | output | `iq-wa-dlr-enricher-group` | Enriched WA DLR events |
| | | `comms_iq_wa_dlr_enriched_retrial` | retry | `iq-wa-dlr-enricher-group` | WA DLR enrichment retry |
| | | `comms_iq_wa_dlr_enriched_dlq` | dlq | `iq-wa-dlr-enricher-group` | WA DLR enrichment DLQ |
| **Email (Netcore)** | "DLR Enricher – Email (Netcore)" | `comms_netcore_email_dlr_enriched` | output | `dlr-enricher-group-netcore` | Enriched Netcore Email DLR events |
| | | `comms_netcore_email_dlr_enriched_retrial` | retry | `dlr-enricher-group-netcore` | Netcore Email DLR enrichment retry |
| | | `comms_netcore_email_dlr_enriched_dlq` | dlq | `dlr-enricher-group-netcore` | Netcore Email DLR enrichment DLQ |

*Also produces to:* `enriched-dlr-topic` · `dlr-retry-topic` *(self-loop)* · `dlr-enricher-dlq` · `cs_raw_reporting_topic`

---

#### `uclm-dlr-aerospike-cache-loader`

> **Repo:** `bummlebee` · **Registry name:** "DLR Aerospike Cache Loader"

| Topic | Type | Consumer Group | Notes |
|-------|------|----------------|-------|
| `wa_main_service` | input | `iq-wa-cache-loader-group` | WA main-service events for Aerospike cache population |
| `wa_main_service_dlq` | dlq | `iq-wa-cache-loader-group` | DLQ for WA cache loader failures |
| `comms_netcore_email_dlr_cache_dlq` | dlq | `cache-loader-group` | DLQ for Email (Netcore) DLR cache loader failures |

*Also consumes:* `channel_partner_sms_nrt_svc_succ` · `channel_partner_push_nrt_svc_succ` · `channel_partner_rcs_nrt_svc_succ` · `channel_partner_email_nrt_svc_succ`
*Also produces to:* `comms_iq_sms_dlr_cache_dlq` *(DLQ only)*

---

#### `uclm-dlr-api-service`

> **Repo:** `bummlebee`
> Receives HTTP webhooks from channel providers — **no input Kafka topics**.
> *Produces to:* `iq_channel_dlr_raw`

---

### 📢 COMMS Services

---

#### `uclm-campaign-manager-event-enrichment`

> **Repo:** `comms` · **Registry name:** "Event Enrichment Service"

| Topic | Type | Consumer Group | Notes |
|-------|------|----------------|-------|
| `enriched-events` | output | `event-enrichment-group` | Fully enriched campaign events |

*Consumes:* `event_enrichment_prod` *(external upstream — ⚠️ need external broker config)*
*Also produces to:* `channel_partner_sms_nrt_svc_valgov` · `channel_partner_wa_nrt_svc_valgov` · `channel_partner_eml_nrt_svc_valgov` · `channel_partner_push_nrt_svc_valgov` · `channel_partner_rcs_nrt_svc_valgov` · `cs_raw_reporting_topic`

---

#### `uclm-campaign-time-validation`

> **Repo:** `comms` · **Registry name:** "Campaign Time Validation"
> *(No Kafka topics registered yet in registry — placeholder section)*

*Produces to:* `channel_partner_sms_nrt_svc_valgov` · `channel_partner_wa_nrt_svc_valgov` · `channel_partner_eml_nrt_svc_valgov` · `channel_partner_push_nrt_svc_valgov` · `channel_partner_rcs_nrt_svc_valgov` · `cs_raw_reporting_topic` · `comms_analytics_logs`

---

#### `uclm-campaign-data-file-dowload`

> **Repo:** `comms` · **Registry name:** "Campaign Data File Download"
> *(No Kafka topics registered yet in registry — placeholder section)*

*Consumes:* `control_file_request`

---

#### `uclm-campaign-manager`

> **Repo:** `comms`
> *(No topics in registry — produces ad-hoc)*

*Produces to:* `uclm_analytics` · `orchestrator.request`

---

#### `uclm-campaign-processor`

> **Repo:** `comms`
> Receives HTTP callback from Audience Manager — no input Kafka topics.

*Produces to:* `control_file_request`

---

#### `uclm-campaign-audience-push`

> **Repo:** `comms` · **Pure REST service** — no Kafka topics.

---

#### `uclm-campaign-cg-exclusion`

> **Repo:** `comms` · **Pure REST / SpEL rule engine** — no Kafka topics.

---

#### `uclm-campaign-exclusion-scan`

> **Repo:** `comms` · **Pure REST / in-memory CSV** — no Kafka topics.

---

#### `uclm-test-campaign`

> **Repo:** `comms`

*Produces to:* `channel_partner_sms_nrt_svc_valgov` · `channel_partner_wa_nrt_svc_valgov` · `channel_partner_eml_nrt_svc_valgov` · `channel_partner_push_nrt_svc_valgov` · `channel_partner_rcs_nrt_svc_valgov`

---

#### `uclm-analytics-reporting-service`

> **Repo:** `comms`

*Consumes:* `uclm_analytics`
*Produces to:* `dimension_refresh_topic` · `uclm_campaign_status`

---

### 👤 USER-MANAGEMENT Services

---

#### `uclm-contentmgmt`

> **Repo:** `user-management` · **Registry name:** "Content Manager"

| Topic | Type | Consumer Group | Notes |
|-------|------|----------------|-------|
| `content_change_log` | analytics | — | Content-change audit / log stream |

---

#### `uclm-auth-manager`

> **Repo:** `user-management` · **Pure REST / JWT service** — no Kafka topics.

---

## Part 2 — Topic-wise Producer & Consumer Map

> **Legend:**
> - ✅ `FULLY INTERNAL` — both producer and consumer are UCLM services from these repos → **safe to use your own custom Kafka broker**
> - ⚠️ `ONLY PRODUCER` — you produce to this topic but the consumer is external → **need external consumer's Kafka broker/config**
> - ⚠️ `ONLY CONSUMER` — you consume from this topic but the producer is external → **need external producer's Kafka broker/config**
> - 🔧 `INTERNAL DLQ` — internal dead-letter queue, no automated consumer, ops-managed

---

### Bummlebee Dispatch Pipeline Topics

> Services are deployed **per channel**. Each row below corresponds to one channel-scoped deployment.

#### Stage 1 — Comms → Rate Controller (channel input)

| Topic | Producer | Consumer | Status |
|-------|----------|----------|--------|
| `channel_partner_sms_nrt_svc_valgov` | `uclm-campaign-time-validation` · `uclm-campaign-manager-event-enrichment` · `uclm-test-campaign` | `uclm-rate-controller-service` (SMS instance) | ✅ FULLY INTERNAL |
| `channel_partner_wa_nrt_svc_valgov` | `uclm-campaign-time-validation` · `uclm-campaign-manager-event-enrichment` · `uclm-test-campaign` | `uclm-rate-controller-service` (WA instance) | ✅ FULLY INTERNAL |
| `channel_partner_eml_nrt_svc_valgov` | `uclm-campaign-time-validation` · `uclm-campaign-manager-event-enrichment` · `uclm-test-campaign` | `uclm-rate-controller-service` (Email instance) | ✅ FULLY INTERNAL |
| `channel_partner_push_nrt_svc_valgov` | `uclm-campaign-time-validation` · `uclm-campaign-manager-event-enrichment` · `uclm-test-campaign` | `uclm-rate-controller-service` (Push instance) | ✅ FULLY INTERNAL |
| `channel_partner_rcs_nrt_svc_valgov` | `uclm-campaign-time-validation` · `uclm-campaign-manager-event-enrichment` · `uclm-test-campaign` | `uclm-rate-controller-service` (RCS instance) | ✅ FULLY INTERNAL |

#### Stage 2 — Rate Controller → Validation-Governance (per-channel input)

> Each rate-controller instance (one per channel) produces to its own channel-scoped input topic for validation-governance.

| Topic | Producer | Consumer | Status |
|-------|----------|----------|--------|
| `channel_partner_val_gov_sms_svc_input` | `uclm-rate-controller-service` (SMS) | `uclm-validation-governance-service` (SMS) | ✅ FULLY INTERNAL |
| `channel_partner_val_gov_whatsapp_svc_input` | `uclm-rate-controller-service` (WA) | `uclm-validation-governance-service` (WA) | ✅ FULLY INTERNAL |
| `channel_partner_val_gov_push_svc_input` | `uclm-rate-controller-service` (Push) | `uclm-validation-governance-service` (Push) | ✅ FULLY INTERNAL |
| `channel_partner_val_gov_rcs_svc_input` | `uclm-rate-controller-service` (RCS) | `uclm-validation-governance-service` (RCS) | ✅ FULLY INTERNAL |
| `channel_partner_val_gov_email_svc_input` | `uclm-rate-controller-service` (Email) | `uclm-validation-governance-service` (Email) | ✅ FULLY INTERNAL |

#### Stage 3 — Validation-Governance → Orchestrator (per-channel endpoint)

| Topic | Producer | Consumer | Status |
|-------|----------|----------|--------|
| `channel_partner_sms_endpoint` | `uclm-validation-governance-service` (SMS) | `uclm-orchestrator-service` (SMS) | ✅ FULLY INTERNAL |
| `channel_partner_whatsapp_endpoint` | `uclm-validation-governance-service` (WA) | `uclm-orchestrator-service` (WA) | ✅ FULLY INTERNAL |
| `channel_partner_push_endpoint` | `uclm-validation-governance-service` (Push) | `uclm-orchestrator-service` (Push) | ✅ FULLY INTERNAL |
| `channel_partner_rcs_endpoint` | `uclm-validation-governance-service` (RCS) | `uclm-orchestrator-service` (RCS) | ✅ FULLY INTERNAL |
| `channel_partner_email_endpoint` | `uclm-validation-governance-service` (Email) | `uclm-orchestrator-service` (Email) | ✅ FULLY INTERNAL |
| `dispatch` | `uclm-validation-governance-service` | `uclm-orchestrator-service` *(fallback / shared routing)* | ✅ FULLY INTERNAL |

#### Stage 4 — Orchestrator → DLR Cache Loader (success acknowledgement)

| Topic | Producer | Consumer | Status |
|-------|----------|----------|--------|
| `channel_partner_sms_nrt_svc_succ` | `uclm-orchestrator-service` (SMS) | `uclm-dlr-aerospike-cache-loader` | ✅ FULLY INTERNAL |
| `wa_main_service` | `uclm-orchestrator-service` (WA) | `uclm-dlr-aerospike-cache-loader` | ✅ FULLY INTERNAL |
| `channel_partner_push_nrt_svc_succ` | `uclm-orchestrator-service` (Push) | `uclm-dlr-aerospike-cache-loader` | ✅ FULLY INTERNAL |
| `channel_partner_rcs_nrt_svc_succ` | `uclm-orchestrator-service` (RCS) | `uclm-dlr-aerospike-cache-loader` | ✅ FULLY INTERNAL |
| `channel_partner_email_nrt_svc_succ` | `uclm-orchestrator-service` (Email) | `uclm-dlr-aerospike-cache-loader` | ✅ FULLY INTERNAL |

---

### DLR Pipeline Topics

| Topic | Producer | Consumer | Status |
|-------|----------|----------|--------|
| `iq_channel_dlr_raw` *(aka `comms_iq_sms_dlr_raw` / `comms_iq_wa_dlr_raw`)* | `uclm-dlr-api-service` | `uclm-dlr-enricher` | ✅ FULLY INTERNAL |
| `dlr-retry-topic` *(aka `comms_iq_sms_dlr_enriched_retrial` / `comms_iq_wa_dlr_enriched_retrial`)* | `uclm-dlr-enricher` | `uclm-dlr-enricher` (self-loop retry) | ✅ FULLY INTERNAL |
| `enriched-dlr-topic` *(aka `comms_iq_sms_dlr_enriched`)* | `uclm-dlr-enricher` | External Analytics / Downstream | ⚠️ ONLY PRODUCER — need external consumer's Kafka details |
| `comms_iq_sms_dlr_cache_dlq` / `wa_main_service_dlq` | `uclm-dlr-aerospike-cache-loader` | Manual ops recovery | 🔧 INTERNAL DLQ — no automated consumer |
| `dlr-enricher-dlq` *(aka `comms_iq_sms_dlr_enriched_dlq`)* | `uclm-dlr-enricher` | Manual ops recovery | 🔧 INTERNAL DLQ — no automated consumer |

---

### Campaign / File Pipeline Topics

| Topic | Producer | Consumer | Status |
|-------|----------|----------|--------|
| `control_file_request` | `uclm-campaign-processor` | `uclm-campaign-data-file-download` | ✅ FULLY INTERNAL |
| `uclm_analytics` | `uclm-campaign-manager` | `uclm-analytics-reporting-service` | ✅ FULLY INTERNAL |
| `event_enrichment_prod` *(aka `event_enrichment` in dev)* | **External upstream event/trigger system** | `uclm-campaign-manager-event-enrichment` | ⚠️ ONLY CONSUMER — need external producer's Kafka broker/config |
| `orchestrator.request` | `uclm-campaign-manager` | **External Email Orchestrator** | ⚠️ ONLY PRODUCER — need external consumer's Kafka details |

---

### Analytics / Reporting Topics

| Topic | Producer | Consumer | Status |
|-------|----------|----------|--------|
| `cs_raw_reporting_topic` | `uclm-validation-governance-service` · `uclm-orchestrator-service` · `uclm-dlr-enricher` · `uclm-campaign-time-validation` · `uclm-campaign-manager-event-enrichment` | **External Analytics System** | ⚠️ ONLY PRODUCER — need external consumer's Kafka details |
| `comms_analytics_logs` | `uclm-campaign-time-validation` | **External Analytics / Logging System** | ⚠️ ONLY PRODUCER — need external consumer's Kafka details |
| `dimension_refresh_topic` | `uclm-analytics-reporting-service` | **External downstream dim consumers** | ⚠️ ONLY PRODUCER — need external consumer's Kafka details |
| `uclm_campaign_status` | `uclm-analytics-reporting-service` | **External Analytics System** | ⚠️ ONLY PRODUCER — need external consumer's Kafka details |

---

### Error / Exception / DLQ Topics

| Topic | Producer | Consumer | Status |
|-------|----------|----------|--------|
| `channel_partner_sms_err` | `uclm-orchestrator-service` (SMS) | **External Monitoring / Alerting** | ⚠️ ONLY PRODUCER — need external consumer's Kafka details |
| `channel_partner_whatsapp_err` | `uclm-orchestrator-service` (WA) | **External Monitoring / Alerting** | ⚠️ ONLY PRODUCER — need external consumer's Kafka details |
| `channel_partner_push_err` | `uclm-orchestrator-service` (Push) | **External Monitoring / Alerting** | ⚠️ ONLY PRODUCER — need external consumer's Kafka details |
| `channel_partner_rcs_err` | `uclm-orchestrator-service` (RCS) | **External Monitoring / Alerting** | ⚠️ ONLY PRODUCER — need external consumer's Kafka details |
| `channel_partner_email_err` | `uclm-orchestrator-service` (Email) | **External Monitoring / Alerting** | ⚠️ ONLY PRODUCER — need external consumer's Kafka details |
| `channel_partner_apb_succ` | `uclm-orchestrator-service` | **External APB Platform** | ⚠️ ONLY PRODUCER — need APB Platform Kafka details |
| `channel_partner_apb_err` | `uclm-orchestrator-service` | **External APB Platform** | ⚠️ ONLY PRODUCER — need APB Platform Kafka details |
| `exceptions` | `uclm-validation-governance-service` | **External Monitoring** | ⚠️ ONLY PRODUCER — need external consumer's Kafka details |
| `apb-exceptions` | `uclm-validation-governance-service` | **External APB Monitoring** | ⚠️ ONLY PRODUCER — need APB Monitoring Kafka details |
| `orchestrator-exceptions` | `uclm-orchestrator-service` | **External Monitoring** | ⚠️ ONLY PRODUCER — need external consumer's Kafka details |
| `d2c-clm-sit` *(runs on a separate external Kafka cluster)* | `uclm-validation-governance-service` | **External D2C downstream** | ⚠️ ONLY PRODUCER — uses dedicated external broker: `10.248.244.115:9092, 10.248.244.116:9092, 10.248.244.117:9092` |
| `comms_iq_sms_dlr_cache_dlq` | `uclm-dlr-aerospike-cache-loader` | Manual ops recovery | 🔧 INTERNAL DLQ — no automated consumer |
| `wa_main_service_dlq` | `uclm-dlr-aerospike-cache-loader` | Manual ops recovery | 🔧 INTERNAL DLQ — no automated consumer |
| `dlr-enricher-dlq` *(aka `comms_iq_sms_dlr_enriched_dlq`)* | `uclm-dlr-enricher` | Manual ops recovery | 🔧 INTERNAL DLQ — no automated consumer |

---

## Part 3 — Custom Kafka Broker Decision Guide

### ✅ Topics — BOTH sides are internal → Use your own Kafka broker

These topics are fully owned within the UCLM ecosystem. You can safely stand up a custom Kafka broker for these without any coordination with external teams.

| Topic | Internal Flow (Producer → Consumer) |
|-------|-------------------------------------|
| `channel_partner_sms_nrt_svc_valgov` | comms (time-validation / event-enrichment / test-campaign) → bummlebee rate-controller (SMS) |
| `channel_partner_wa_nrt_svc_valgov` | comms → bummlebee rate-controller (WA) |
| `channel_partner_eml_nrt_svc_valgov` | comms → bummlebee rate-controller (Email) |
| `channel_partner_push_nrt_svc_valgov` | comms → bummlebee rate-controller (Push) |
| `channel_partner_rcs_nrt_svc_valgov` | comms → bummlebee rate-controller (RCS) |
| `channel_partner_val_gov_sms_svc_input` | rate-controller (SMS) → validation-governance (SMS) |
| `channel_partner_val_gov_whatsapp_svc_input` | rate-controller (WA) → validation-governance (WA) |
| `channel_partner_val_gov_push_svc_input` | rate-controller (Push) → validation-governance (Push) |
| `channel_partner_val_gov_rcs_svc_input` | rate-controller (RCS) → validation-governance (RCS) |
| `channel_partner_val_gov_email_svc_input` | rate-controller (Email) → validation-governance (Email) |
| `dispatch` | validation-governance → orchestrator (fallback/shared) |
| `channel_partner_sms_endpoint` | validation-governance (SMS) → orchestrator (SMS) |
| `channel_partner_whatsapp_endpoint` | validation-governance (WA) → orchestrator (WA) |
| `channel_partner_push_endpoint` | validation-governance (Push) → orchestrator (Push) |
| `channel_partner_rcs_endpoint` | validation-governance (RCS) → orchestrator (RCS) |
| `channel_partner_email_endpoint` | validation-governance (Email) → orchestrator (Email) |
| `channel_partner_sms_nrt_svc_succ` | orchestrator (SMS) → dlr-aerospike-cache-loader |
| `wa_main_service` | orchestrator (WA) → dlr-aerospike-cache-loader |
| `channel_partner_push_nrt_svc_succ` | orchestrator (Push) → dlr-aerospike-cache-loader |
| `channel_partner_rcs_nrt_svc_succ` | orchestrator (RCS) → dlr-aerospike-cache-loader |
| `channel_partner_email_nrt_svc_succ` | orchestrator (Email) → dlr-aerospike-cache-loader |
| `iq_channel_dlr_raw` | dlr-api-service → dlr-enricher |
| `dlr-retry-topic` | dlr-enricher → dlr-enricher (self-loop) |
| `control_file_request` | campaign-processor → campaign-data-file-download |
| `uclm_analytics` | campaign-manager → analytics-reporting-service |

---

### ⚠️ Topics — YOU ARE ONLY PRODUCER → Need external consumer's Kafka details

For these topics you own the producer side. You need the **external team's Kafka broker address, topic config, and consumer group** before you can route messages correctly.

| Topic | Your Producer Service | Action Needed |
|-------|-----------------------|---------------|
| `cs_raw_reporting_topic` | validation-governance · orchestrator · dlr-enricher · time-validation · event-enrichment | Get Kafka broker + topic config from the **Analytics team** |
| `enriched-dlr-topic` | dlr-enricher | Get Kafka broker + topic config from the **DLR / Analytics downstream team** |
| `comms_analytics_logs` | time-validation | Get Kafka broker + topic config from the **Analytics / Logging team** |
| `dimension_refresh_topic` | analytics-reporting-service | Get Kafka broker + topic config from the **Dimension consumers team** |
| `uclm_campaign_status` | analytics-reporting-service | Get Kafka broker + topic config from the **Analytics system team** |
| `orchestrator.request` | campaign-manager | Get Kafka broker + topic config from the **Email Orchestrator team** |
| `channel_partner_sms_err` | orchestrator (SMS) | Get Kafka broker + topic config from the **Monitoring / Alerting team** |
| `channel_partner_whatsapp_err` | orchestrator (WA) | Get Kafka broker + topic config from the **Monitoring / Alerting team** |
| `channel_partner_push_err` | orchestrator (Push) | Get Kafka broker + topic config from the **Monitoring / Alerting team** |
| `channel_partner_rcs_err` | orchestrator (RCS) | Get Kafka broker + topic config from the **Monitoring / Alerting team** |
| `channel_partner_email_err` | orchestrator (Email) | Get Kafka broker + topic config from the **Monitoring / Alerting team** |
| `channel_partner_apb_succ` / `channel_partner_apb_err` | orchestrator | Get Kafka broker + topic config from the **APB Platform team** |
| `exceptions` / `apb-exceptions` | validation-governance | Get Kafka broker + topic config from the **Monitoring / APB Monitoring team** |
| `orchestrator-exceptions` | orchestrator | Get Kafka broker + topic config from the **Monitoring team** |
| `d2c-clm-sit` | validation-governance | **Already uses an external broker**: `10.248.244.115:9092, 10.248.244.116:9092, 10.248.244.117:9092` — align with D2C downstream team |

---

### ⚠️ Topics — YOU ARE ONLY CONSUMER → Need external producer's Kafka details

For these topics you own the consumer side. You need the **external team's Kafka broker address and topic config** so you can point your consumer group at the right broker.

| Topic | Your Consumer Service | Action Needed |
|-------|-----------------------|---------------|
| `event_enrichment_prod` | campaign-manager-event-enrichment | Get Kafka broker + topic config from the **upstream event ingestion / trigger system team** |
| `comms-input` *(local only)* | rate-controller | Only used in local dev; upstream UCLM platform produces to this |

---

## Part 4 — Consumer Group Reference

| Consumer Group ID | Service | Topics Consumed | Concurrency |
|-------------------|---------|-----------------|-------------|
| `channel-partner-rate-controller-sms-svc` *(per-channel, e.g. sms/whatsapp/push/rcs/email)* | uclm-rate-controller-service | `channel_partner_{channel}_nrt_svc_valgov` | 1 (manual ACK + nack on rate limit) |
| `channel-partner-rate-controller-svc` (dev) / `channel-tenant-throttle-listener` (local) | uclm-rate-controller-service | `channel-partner-rate-controller-input` / `comms-input` | 1 (manual ACK + nack on rate limit) |
| `val-gov-sms-consumer-group` *(per-channel, e.g. sms/whatsapp/push/rcs/email)* | uclm-validation-governance-service | `channel_partner_val_gov_{channel}_svc_input` | 4 |
| `event-consumer` (dev) / `default` (local) | uclm-validation-governance-service | `channel_partner_val_gov_*_svc_input` (legacy: `event`) | 4 |
| `dispatch-request-consumer-group` *(per-channel instance)* | uclm-orchestrator-service | `channel_partner_{channel}_endpoint` · `dispatch` | 4 |
| `iq-sms-cache-loader-group-uat` (dev) / `cache-loader-group` (local) | uclm-dlr-aerospike-cache-loader | `channel_partner_sms_nrt_svc_succ` · `wa_main_service` · `channel_partner_push_nrt_svc_succ` · `channel_partner_rcs_nrt_svc_succ` · `channel_partner_email_nrt_svc_succ` | 1 (manual ACK) |
| `dlr-enricher-group-uat` (dev) / `dlr-enricher-group-prod` (prod) | uclm-dlr-enricher | `iq_channel_dlr_raw` · `dlr-retry-topic` | 1 (manual ACK) |
| `event-enrichment-group` | uclm-campaign-manager-event-enrichment | `event_enrichment_prod` | — (manual ACK) |
| `control_file_request` | uclm-campaign-data-file-download | `control_file_request` | — (manual ACK + Semaphore(100) back-pressure) |
| `analytics-metadata-service` | uclm-analytics-reporting-service | `uclm_analytics` | — |

---

## Part 5 — Kafka Security Modes

| Environment | Protocol | Auth Mechanism |
|-------------|----------|----------------|
| Local / Dev | `PLAINTEXT` | None |
| UAT | `SASL_PLAINTEXT` | Kerberos (GSSAPI) with keytab |
| PROD | `SASL_SSL` | Kerberos (GSSAPI) + TLS |
| Cloud | `SASL_PLAINTEXT` | SCRAM-SHA-256 (username + password) |
| External D2C broker (DEV) | `PLAINTEXT` | None — broker: `10.248.244.115:9092,...` |

---

## Part 6 — Key Message Types Per Topic

| Topic | Message Type / Schema |
|-------|-----------------------|
| `channel_partner_{channel}_nrt_svc_valgov` (all 5 channels) | `EventRequestDTO` — uuid, moc, si_id, script_body, tenant_id, workspace_id, expire_timestamp, dynamic_params, etc. |
| `channel_partner_val_gov_{channel}_svc_input` (all 5 channels) | `EventRequestDTO` — same as above (after TPS throttle pass by rate-controller) |
| `channel_partner_{channel}_endpoint` / `dispatch` | `ChannelDispatchRequest` — wraps EventRequestDTO + channel-specific payload (DLT entity ID, sender ID, etc.) |
| `channel_partner_{channel}_nrt_svc_succ` / `wa_main_service` | `FullKafkaDml` — uuid, channel, status (SUCCESS/FAILURE), providerResponse, statusCode, timestamp, eventRequest, cmsResponse |
| `channel_partner_{channel}_err` | `FullKafkaDml` — same as above, status=FAILURE |
| `iq_channel_dlr_raw` | Raw DLR JSON — requestId, mobile, status (DELIVERED/FAILED/PENDING), deliveredAt, errorCode, providerRef |
| `enriched-dlr-topic` | Enriched DLR — raw DLR fields merged with original dispatch record from Aerospike |
| `dlr-retry-topic` | `RetryEnvelope` — payload, lookupKey, attempt (1-3), scheduledAt, delayMinutes (15→30→60) |
| `cs_raw_reporting_topic` | `AnalyticsEventDTO` — every significant processing event across all pipeline stages |
| `control_file_request` | `ControlFileMessage` — audienceId, transactionId, partFiles (URLs), recordCount, attributeList, delimiters |
| `uclm_analytics` | Campaign lifecycle events — created, published, approved, rejected |
| `orchestrator.request` | Thymeleaf-rendered HTML email body for async email notification |
| `dimension_refresh_topic` | Dimension upsert events — after each successful DB dimension write |
| `uclm_campaign_status` | Campaign status change events |
| `comms_analytics_logs` | Per-record structured analytics logs (one per audience record dispatched) |

---

## Part 7 — Complete Kafka Topic Creation Checklist

> **This is the master list of all topics you need to provision on your Kafka broker.**
> Topics on external brokers (`d2c-clm-sit`, `event_enrichment_prod`) are excluded — those are provisioned by the owning external team.

### 🟢 Group A — Comms → Rate Controller (channel input) — 5 topics

| # | Topic Name | Notes |
|---|-----------|-------|
| 1 | `channel_partner_sms_nrt_svc_valgov` | SMS campaign input from comms to rate-controller |
| 2 | `channel_partner_wa_nrt_svc_valgov` | WhatsApp campaign input (legacy `wa` abbreviation) |
| 3 | `channel_partner_eml_nrt_svc_valgov` | Email campaign input (legacy `eml` abbreviation) |
| 4 | `channel_partner_push_nrt_svc_valgov` | Push campaign input |
| 5 | `channel_partner_rcs_nrt_svc_valgov` | RCS campaign input |

---

### 🟢 Group B — Validation-Governance → Orchestrator (per-channel dispatch endpoint) — 6 topics

| # | Topic Name | Channel |
|---|-----------|---------|
| 6 | `dispatch` | Fallback / shared routing |
| 7 | `channel_partner_sms_endpoint` | SMS |
| 8 | `channel_partner_whatsapp_endpoint` | WhatsApp |
| 9 | `channel_partner_push_endpoint` | Push |
| 10 | `channel_partner_rcs_endpoint` | RCS |
| 11 | `channel_partner_email_endpoint` | Email |

---

### 🟢 Group C — Orchestrator → DLR Cache Loader (success acknowledgement) — 5 topics

| # | Topic Name | Channel |
|---|-----------|---------|
| 12 | `channel_partner_sms_nrt_svc_succ` | SMS |
| 13 | `wa_main_service` | WhatsApp *(special legacy topic name)* |
| 14 | `channel_partner_push_nrt_svc_succ` | Push |
| 15 | `channel_partner_rcs_nrt_svc_succ` | RCS |
| 16 | `channel_partner_email_nrt_svc_succ` | Email |

---

### 🟢 Group D — DLR Pipeline — 3 topics

| # | Topic Name | Notes |
|---|-----------|-------|
| 17 | `iq_channel_dlr_raw` | Raw DLR from dlr-api-service → dlr-enricher *(aka `comms_iq_sms_dlr_raw` / `comms_iq_wa_dlr_raw`)* |
| 18 | `dlr-retry-topic` | Self-loop retry topic for dlr-enricher *(aka `comms_iq_sms_dlr_enriched_retrial`)* |
| 19 | `enriched-dlr-topic` | Enriched DLR output → external analytics *(aka `comms_iq_sms_dlr_enriched`)* |

---

### 🟢 Group E — Campaign / File Pipeline — 2 topics

| # | Topic Name | Notes |
|---|-----------|-------|
| 20 | `control_file_request` | campaign-processor → campaign-data-file-download |
| 21 | `uclm_analytics` | campaign-manager → analytics-reporting-service |

---

### 🟡 Group F — Analytics / Reporting (produce to external consumer) — 4 topics

> Create on your broker; external teams consume from here. Coordinate topic config with each team.

| # | Topic Name | External Consumer |
|---|-----------|-------------------|
| 22 | `cs_raw_reporting_topic` | Analytics team |
| 23 | `comms_analytics_logs` | Analytics / Logging team |
| 24 | `dimension_refresh_topic` | Dimension consumers team |
| 25 | `uclm_campaign_status` | Analytics system team |

---

### 🟡 Group G — External Orchestration (produce to external consumer) — 1 topic

| # | Topic Name | External Consumer |
|---|-----------|-------------------|
| 26 | `orchestrator.request` | Email Orchestrator team |

---

### 🟡 Group H — Per-Channel Error / Exception Topics (produce to external) — 7 topics

> Create on your broker; external monitoring systems consume from here.

| # | Topic Name | Channel | External Consumer |
|---|-----------|---------|-------------------|
| 27 | `channel_partner_sms_err` | SMS | Monitoring / Alerting team |
| 28 | `channel_partner_whatsapp_err` | WhatsApp | Monitoring / Alerting team |
| 29 | `channel_partner_push_err` | Push | Monitoring / Alerting team |
| 30 | `channel_partner_rcs_err` | RCS | Monitoring / Alerting team |
| 31 | `channel_partner_email_err` | Email | Monitoring / Alerting team |
| 32 | `channel_partner_apb_succ` | All (APB) | APB Platform team |
| 33 | `channel_partner_apb_err` | All (APB) | APB Platform team |

---

### 🟡 Group I — Service Exception Topics (produce to external) — 3 topics

| # | Topic Name | Producer | External Consumer |
|---|-----------|----------|-------------------|
| 34 | `exceptions` | validation-governance | Monitoring team |
| 35 | `apb-exceptions` | validation-governance | APB Monitoring team |
| 36 | `orchestrator-exceptions` | orchestrator | Monitoring team |

---

### 🔧 Group J — Internal DLQ Topics (ops-managed, no automated consumer) — 3 topics

| # | Topic Name | Notes |
|---|-----------|-------|
| 37 | `comms_iq_sms_dlr_cache_dlq` | DLQ for dlr-aerospike-cache-loader (SMS) |
| 38 | `wa_main_service_dlq` | DLQ for dlr-aerospike-cache-loader (WA) |
| 39 | `dlr-enricher-dlq` | DLQ for dlr-enricher *(aka `comms_iq_sms_dlr_enriched_dlq`)* |

---

### ⛔ Group K — External Broker Topics (DO NOT create on own broker)

| Topic Name | Reason |
|-----------|--------|
| `d2c-clm-sit` | Dedicated external broker: `10.248.244.115:9092, 10.248.244.116:9092, 10.248.244.117:9092` — align with D2C team |
| `event_enrichment_prod` | Owned and produced by external upstream event system — you are only a consumer |
| `comms-input` | Local dev only — upstream UCLM platform topic |

---

### 📊 Summary

| Group | Category | Topics to Create |
|-------|----------|-----------------|
| A | Comms → Rate Controller (channel input) | 5 |
| B | Val-Gov → Orchestrator (per-channel endpoint) | 6 |
| C | Orchestrator → DLR Cache (per-channel succ) | 5 |
| D | DLR Pipeline | 3 |
| E | Campaign / File | 2 |
| F | Analytics / Reporting | 4 |
| G | External Orchestration | 1 |
| H | Per-Channel Error Topics | 7 |
| I | Service Exception Topics | 3 |
| J | Internal DLQ | 3 |
| **Total** | | **39 topics** |

---

*This document is generated and maintained from HLD documentation across `ulcm-docs/bummlebee-hld`, `ulcm-docs/comms-hld`, and `ulcm-docs/user-management-hld`. Part 7 (topic creation checklist) is the authoritative list for Kafka provisioning.*
