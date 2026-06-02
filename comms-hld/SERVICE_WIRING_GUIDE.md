# UCLM Comms — Service Wiring & Deployment Config Guide

> All config keys verified directly from source code (`@FeignClient`, `@Value`, `@ConfigurationProperties`).  
> This document answers: **"What exact environment variables / config values must I set to wire every service together in a real deployment?"**

---

## 0 — Shared Infrastructure (Cross-Service Dependencies)

### 0.1 — Shared PVC: Audience Data Files

| Service | Mount Path | Access |
|---------|-----------|--------|
| `uclm-campaign-data-file-download` | `$INGEST_BASE_FOLDER` (default `/data/comms_planner`) | **WRITE** — writes `AUD_DATA/{date}/{audienceId}/part_*.csv` |
| `uclm-campaign-time-validation` | `$INGEST_BASE_FOLDER` (default `/data/comms_planner`) | **READ** — reads same path via `file.download-path` |
| `uclm-test-campaign` | `$INGEST_BASE_FOLDER` (default `/data/comms_planner`) | **READ** — reads same path via `file.download-path` |

> ⚠️ **These three services MUST share the same PVC (ReadWriteMany).** Set the same `INGEST_BASE_FOLDER` env var on all three. If they are on different nodes, PVC must be RWX (NFS / CephFS / EFS).

```yaml
# K8s PVC example
kind: PersistentVolumeClaim
apiVersion: v1
metadata:
  name: comms-audience-data-pvc
spec:
  accessModes: [ReadWriteMany]
  resources:
    requests:
      storage: 50Gi
```

```yaml
# In each Deployment spec — volumeMount
env:
  - name: INGEST_BASE_FOLDER
    value: /data/comms_planner
volumeMounts:
  - name: audience-data
    mountPath: /data/comms_planner
volumes:
  - name: audience-data
    persistentVolumeClaim:
      claimName: comms-audience-data-pvc
```

---

### 0.2 — Shared PVC: Exclusion CSV Files

| Service | Mount Path | Access |
|---------|-----------|--------|
| `uclm-campaign-exclusion-scan` | `$INGEST_BASE_FOLDER` (default `/data/exclusion`) | **READ** — loads `employee.csv`, `vip.csv`, `retailer.csv` at startup + daily reload |

> This is a **separate PVC** from the audience data PVC. The Exclusion Scan service only reads. Files must be placed here by an external pipeline (Airflow / manual upload).

```yaml
# env var for exclusion-scan
env:
  - name: INGEST_BASE_FOLDER
    value: /data/exclusion
```

---

### 0.3 — Kafka Clusters (Three Separate Clusters!)

| Cluster | Brokers (UAT) | Auth | Used By |
|---------|--------------|------|---------|
| **Cluster A — Main Comms** | `10.92.36.48:9092, 10.92.36.44:9092, 10.92.36.46:9092` | Kerberos GSSAPI | campaign-manager, analytics-reporting (consume + `dimension_refresh`), campaign-processor, data-file-download, event-enrichment |
| **Cluster B — Channel/NRT** | `10.20.5.166:9092, 10.20.5.177:9092, 10.20.5.142:9092` | SCRAM-SHA-512 (`clm-admin` / `clm-admin-apb123`) | time-validation, test-campaign |
| **Cluster C — Campaign Status** | `10.222.201.101:9092, 10.222.201.102:9092, 10.222.201.103:9092` | PLAINTEXT (no auth) | analytics-reporting (`uclm_campaign_status` producer only) |

---

## 1 — uclm-campaign-manager

**Exposes:** `http://<host>:8080/campaign-manager/api/v1/...`

### Config to set

| Property Key | Env Var | Value / Description |
|-------------|---------|---------------------|
| `spring.kafka.bootstrap-servers` | `KAFKA_BOOTSTRAP_SERVERS` | Cluster A brokers |
| `campaign.analytics.kafka.topic` | — | `uclm_analytics` |
| `app.kafka.topics.approval-notification` | — | `orchestrator.request` |
| `campaign.kafka.enabled` | — | `true` |
| `campaign.analytics.kafka.enabled` | — | `true` |
| `cms.base-url` | — | CMS service URL |
| `cms.endpoints.campaign` | — | `/config/campaign` |
| `cms.endpoints.cohort` | — | `/config/cohort` |
| `campaign.email.attachment.media-filter-url` | — | Full URL: `{content-manager-url}/content-manager/api/v1/media/filter` |
| `campaign.ui.url` | — | UI origins for CORS (comma-separated) |
| `spring.datasource.url` | `DB_URL` | Oracle/MySQL JDBC URL |
| `spring.datasource.username` | `DB_USERNAME` | DB username |
| `spring.datasource.password` | `DB_PASSWORD` | DB password |
| Kerberos keytab | `KAFKA_KEYTAB_PATH` | `/tmp/keytabs/<principal>.keytab` |

### Called by other services

| Caller | Endpoint Called |
|--------|----------------|
| event-enrichment | `GET /campaign-manager/api/v1/campaigns/goals/{goalId}` |
| event-enrichment, time-validation, test-campaign | `GET /campaign-manager/api/v1/campaigns/goals/{goalId}/subgoals` |
| audience-push | (owns campaign state, not called directly — shares DB) |

> No external callback registration needed. This service only receives REST calls.

---

## 2 — uclm-campaign-audience-push

**Exposes:** `http://<host>:8095/` (no context path)

### Config to set

| Property Key | Env Var | Value / Description |
|-------------|---------|---------------------|
| `am.base-url` | — | Audience Manager base URL |
| `am.api.file-create` | — | `/audience-manager/push/file/v1/create` |
| `am.api.audience-delivery` | — | `/audience-manager/audience/v1/fetch` |
| `am.api-key` | — | API key for AM |
| `am.workflow-id` | — | Workflow ID for AM |
| `am.audience-delivery-api-key` | — | Separate key for delivery fetch |
| `auth-manager.base-url` | — | Auth Manager URL |
| `auth-manager.api.tenant-config` | — | `/auth-manager/api/v1/tenant/config` |
| `tenant.ids` | — | List of active tenant IDs |
| `tenant.default-timezone` | — | `Asia/Kolkata` |
| `spring.datasource.url` | `DB_URL` | Oracle/MySQL JDBC URL (shares campaign DB) |

### ⚠️ Critical: AM Callback URL Registration

`AudienceFileRequest` has **no `callbackUrl` field** — the callback URL is **not sent dynamically** in the request. Audience Manager must be **pre-configured** to call `uclm-campaign-processor` when file creation is complete.

**Action required in Audience Manager (external config):**
```
Callback URL to register in AM:
  POST http://<campaign-processor-host>:8080/api/v1/callback
```

Without this, `uclm-campaign-processor` will never receive the control file notification.

---

## 3 — uclm-campaign-processor

**Exposes:** `http://<host>:8080/api/v1/callback`  
← **This URL must be registered in Audience Manager** (see service 2 above)

### Config to set

| Property Key | Env Var | Value / Description |
|-------------|---------|---------------------|
| `spring.kafka.bootstrap-servers` | `KAFKA_BOOTSTRAP_SERVERS` | Cluster A brokers |
| `app.kafka.topic.control-file-push` | `CONTROL_FILE_PUSH_TOPIC` | `control_file_request` |
| `campaign.control-file.kafka.enabled` | — | `true` |
| `app.audience.download-timeout-seconds` | — | `300` |
| `app.control-file.max-size-mb` | — | `500` |
| `campaign.ui.url` | — | UI origins for CORS |
| `spring.datasource.url` | `DB_URL` | Oracle/MySQL JDBC URL (shares campaign DB) |
| Kerberos keytab | `KAFKA_KEYTAB_PATH` | `/tmp/keytabs/<principal>.keytab` |

### Topic published → consumed by

| Topic | Consumer |
|-------|----------|
| `control_file_request` | `uclm-campaign-data-file-download` |

---

## 4 — uclm-campaign-data-file-download

**No REST API exposed externally** (Kafka consumer only)

### Config to set

| Property Key | Env Var | Value / Description |
|-------------|---------|---------------------|
| `spring.kafka.bootstrap-servers` | `KAFKA_BOOTSTRAP_SERVERS` | Cluster A brokers |
| `spring.kafka.consumer.group-id` | `KAFKA_GROUP_ID` | `control_file_request` |
| `ingestor.topic` | `INGEST_TOPIC` | `control_file_request` |
| `ingestor.storage` | — | `local` / `s3` / `in-memory` |
| `ingestor.baseFolder` | `INGEST_BASE_FOLDER` | `/data/comms_planner` ← **MUST match time-validation** |
| `ingestor.parallelDownloads` | `PARALLEL_DOWNLOADS` | Number of concurrent part-file downloads |
| `ingestor.kafkaConcurrency` | `KAFKA_CONCURRENCY` | Kafka consumer thread count |
| `ingestor.downloadTimeout` | `DOWNLOAD_TIMEOUT` | Minutes (default 600) |
| `ingestor.s3-bucket` | `INGEST_S3_BUCKET` | Only if `storage=s3` |
| `ingestor.s3-region` | `INGEST_S3_REGION` | Only if `storage=s3` |
| `ingestor.s3-base-path` | `INGEST_S3_BASE_PATH` | Only if `storage=s3` |
| `auth-manager.base-url` | — | Auth Manager URL (for tenant timezone) |
| `auth-manager.api.tenant-config` | — | `/auth-manager/api/v1/tenant/config` |
| `spring.datasource.url` | `DB_URL` | Oracle/MySQL JDBC URL |
| Kerberos keytab | — | For Cluster A GSSAPI auth |

### Kafka security (UAT — Kerberos)
```yaml
campaign.control-file.kafka.security:
  enabled: true
  auth-type: KERBEROS
  protocol: SASL_PLAINTEXT
  mechanism: GSSAPI
  service-name: kafka
  principal: <principal>@INDIA.AIRTEL.ITM
  keytab: /tmp/keytabs/<principal>.keytab
```

---

## 5 — uclm-campaign-manager-event-enrichment

**Exposes:** `http://<host>:8095/event-enrichment/...`

### Config to set

All Feign client URLs are resolved from `external.*` config keys:

| Property Key | Env Var | Points To |
|-------------|---------|-----------|
| `external.tgcg.baseUrl` | — | `http://uclm-campaign-cg-exclusion:8080` |
| `external.exclusion.baseUrl` | — | `http://uclm-campaign-exclusion-scan:8080` |
| `external.audience.baseUrl` | — | Audience Manager base URL |
| `external.audience.endpoint` | — | `/audience-manager/user/audiences` |
| `external.content-manager.baseUrl` | — | `http://uclm-content-manager:7002/content-manager` |
| `external.goal.baseUrl` | — | `http://uclm-campaign-manager:80` |
| `external.goal.endpoint` | — | `/api/v1/campaigns/goals` |
| `external.goal.subgoal-endpoint` | — | `/api/v1/internal/subgoal` |
| `kafka.topics.inbound` | `KAFKA_INBOUND_TOPIC` | `event_enrichment_prod` (prod) |
| `kafka.analytics-topic` | `CLM_KAFKA_LOGS_ANALYTICS` | `cs_raw_reporting_topic` |
| `kafka.topics.SMS` | `CLM_KAFKA_CAMPAIGN_TOPIC_SMS` | `channel_partner_sms_nrt_svc_valgov` |
| `kafka.topics.WHATSAPP` | — | `channel_partner_wa_nrt_svc_valgov` |
| `kafka.topics.EMAIL` | — | `channel_partner_eml_nrt_svc_valgov` |
| `kafka.topics.PUSH` | — | `channel_partner_push_nrt_svc_valgov` |
| `kafka.topics.RCS` | — | `channel_partner_rcs_nrt_svc_valgov` |
| `KAFKA_BOOTSTRAP_SERVERS` | `KAFKA_BOOTSTRAP_SERVERS` | Cluster A brokers |
| `spring.datasource.url` | `DB_URL` | Oracle JDBC URL |
| `external.tgcg.authToken` | `TGCG_AUTH_TOKEN` | `Bearer <token>` for CG exclusion |

> ⚠️ Base `application.yml` has Kafka block **fully commented out**. The active config is in `application-uat.yml` / `application-prod.yml`. Ensure the correct Spring profile is active: `SPRING_PROFILES_ACTIVE=uat` or `prod`.

### Kafka security (UAT — Kerberos / Cluster A)
```yaml
spring.kafka.bootstrap-servers: ${KAFKA_BOOTSTRAP_SERVERS}
# + Kerberos JAAS config via environment variable or mounted keytab
```

---

## 6 — uclm-campaign-time-validation

**Exposes:** `http://<host>:8091/` (trigger endpoint via REST)

### Config to set

| Property Key | Env Var | Value / Description |
|-------------|---------|---------------------|
| `spring.kafka.bootstrap-servers` (under `kafka.bootstrap-servers`) | `KAFKA_BOOTSTRAP_SERVERS` | **Cluster B** (`10.20.5.166:9092,...`) ← different cluster! |
| `kafka.client-id` | `CLM_KAFKA_CLIENT_ID` | `field-level-time-validation` |
| `kafka.campaign.topics.SMS` | `CLM_KAFKA_CAMPAIGN_TOPIC_SMS` | `channel_partner_sms_nrt_svc_valgov` |
| `kafka.campaign.topics.WHATSAPP` | `CLM_KAFKA_CAMPAIGN_TOPIC_WHATSAPP` | `channel_partner_wa_nrt_svc_valgov` |
| `kafka.campaign.topics.EMAIL` | `CLM_KAFKA_CAMPAIGN_TOPIC_EMAIL` | `channel_partner_eml_nrt_svc_valgov` |
| `kafka.campaign.topics.PUSH` | `CLM_KAFKA_CAMPAIGN_TOPIC_PUSH` | `channel_partner_push_nrt_svc_valgov` |
| `kafka.campaign.topics.RCS` | `CLM_KAFKA_CAMPAIGN_TOPIC_RCS` | `channel_partner_rcs_nrt_svc_valgov` |
| `kafka.logs.analytics-topic` | `CLM_KAFKA_LOGS_ANALYTICS` | `cs_raw_reporting_topic` |
| `KAFKA_SECURITY_PROTOCOL` | `KAFKA_SECURITY_PROTOCOL` | `SASL_PLAINTEXT` |
| `KAFKA_SASL_MECHANISM` | `KAFKA_SASL_MECHANISM` | `SCRAM-SHA-512` |
| SCRAM credentials | — | username=`clm-admin`, password=`clm-admin-apb123` (rotate for prod!) |
| `external.exclusion.baseUrl` | — | `http://uclm-campaign-exclusion-scan:8080` |
| `external.content-manager.baseUrl` | — | Content Manager URL + `/content-manager` |
| `external.goal.baseUrl` | — | `http://uclm-campaign-manager:80` |
| `auth-manager.base-url` | — | Auth Manager URL |
| `file.download-path` | `INGEST_BASE_FOLDER` | `/data/comms_planner` ← **MUST match data-file-download** |
| `CLM_SERVER_PORT` | `CLM_SERVER_PORT` | `8091` |
| `spring.datasource.url` | `DB_URL` | Oracle/MySQL JDBC URL |

> ⚠️ **Note:** `TgcgClient.java` (CG Exclusion) is commented out in time-validation. It only calls `ExclusionClient` (exclusion-scan). **No `external.tgcg.baseUrl` needed here.**

### PVC mount (shared with data-file-download)
```yaml
env:
  - name: INGEST_BASE_FOLDER
    value: /data/comms_planner
volumeMounts:
  - name: audience-data
    mountPath: /data/comms_planner
    readOnly: true  # time-validation only reads
```

---

## 7 — uclm-campaign-exclusion-scan

**Exposes:** `http://<host>:8080/api/v1/internal/exclusionscan`

### Config to set

| Property Key | Env Var | Value / Description |
|-------------|---------|---------------------|
| `exclusion.path` | `INGEST_BASE_FOLDER` | `/data/exclusion` — CSV files dir |
| `server.port` | — | `8080` |

### PVC mount (exclusion CSV files)
```yaml
env:
  - name: INGEST_BASE_FOLDER
    value: /data/exclusion
volumeMounts:
  - name: exclusion-data
    mountPath: /data/exclusion
    readOnly: true
```

> CSV files (`employee.csv`, `vip.csv`, `retailer.csv`) must be pre-loaded into this PVC by an external pipeline. Service auto-reloads daily at 00:30 AM — **no restart needed after file update**.

---

## 8 — uclm-campaign-cg-exclusion

**Exposes:** `http://<host>:8080/api/v1/internal/excludecg`

### Config to set

| Property Key | Env Var | Value / Description |
|-------------|---------|---------------------|
| `spring.datasource.url` | — | Oracle JDBC URL |
| `spring.datasource.username` | — | DB username |
| `spring.datasource.password` | — | DB password |
| `server.port` | — | `8080` |

> No Kafka. No external REST calls. Pure Oracle DB → SpEL rule engine.

---

## 9 — uclm-test-campaign

**Exposes:** `http://<host>:8091/` (same as time-validation, separate deployment)

### Config to set

Same as time-validation (section 6) **except:**

| Property Key | Env Var | UAT Override Value |
|-------------|---------|---------------------|
| `kafka.campaign.topics.SMS` | `CLM_KAFKA_CAMPAIGN_TOPIC_SMS` | `uat_sms_topic` |
| `kafka.campaign.topics.WHATSAPP` | `CLM_KAFKA_CAMPAIGN_TOPIC_WHATSAPP` | `uat_wa_topic` |
| `kafka.campaign.topics.EMAIL` | `CLM_KAFKA_CAMPAIGN_TOPIC_EMAIL` | `uat_email_topic` |
| `kafka.campaign.topics.PUSH` | `CLM_KAFKA_CAMPAIGN_TOPIC_PUSH` | `uat_push_topic` |
| `kafka.campaign.topics.RCS` | `CLM_KAFKA_CAMPAIGN_TOPIC_RCS` | `uat_rcs_topic` |
| `external.exclusion.baseUrl` | — | **NOT needed** — test-campaign has no ExclusionClient |
| `external.tgcg.baseUrl` | — | **NOT needed** — no TGCG calls |

> ⚠️ test-campaign does **NOT** call CG Exclusion or Exclusion Scan. No `external.tgcg` or `external.exclusion` config needed.

### PVC mount (shared with data-file-download)
```yaml
env:
  - name: INGEST_BASE_FOLDER
    value: /data/comms_planner  # same PVC as data-file-download and time-validation
```

---

## 10 — uclm-analytics-reporting-service

**Exposes:** `http://<host>:8080/api/v1/analytics/...`

### Config to set

| Property Key | Env Var | Value / Description |
|-------------|---------|---------------------|
| `spring.kafka.bootstrap-servers` | `KAFKA_BOOTSTRAP_SERVERS` | **Cluster A** (consume `uclm_analytics` + produce `dimension_refresh_topic`) |
| `spring.kafka.campaign-status.bootstrap-servers` | — | **Cluster C** (`10.222.201.101:9092,...`) — separate broker for `uclm_campaign_status` |
| `app.kafka.topic.analytics-metadata-dimension` | — | `uclm_analytics` |
| `app.kafka.topic.dimension-refresh` | — | `dimension_refresh_topic` |
| `app.kafka.topic.campaign-status` | — | `uclm_campaign_status` |
| `druid.broker.url` | — | Druid broker HTTP URL |
| `druid.username` | — | Druid basic auth username (if enabled) |
| `druid.password` | — | Druid basic auth password |
| `auth.manager.base-url` | — | Auth Manager URL |
| `analytics.ui.allowed-origins` | — | UI origins for CORS |
| `hazelcast.network.join.kubernetes.service-name` | — | `analytics-reporting-service` (K8s service discovery for Hazelcast cluster) |
| `spring.datasource.url` | `DB_URL` | Oracle/MySQL JDBC URL |

### ⚠️ Dual Kafka cluster config

```yaml
# Cluster A (consume uclm_analytics + produce dimension_refresh_topic) — Kerberos
spring:
  kafka:
    bootstrap-servers: ${KAFKA_BOOTSTRAP_SERVERS:10.92.36.48:9092,...}
    # + Kerberos JAAS config

# Cluster C (produce uclm_campaign_status only) — PLAINTEXT, no auth
spring:
  kafka:
    campaign-status:
      bootstrap-servers: 10.222.201.101:9092,10.222.201.102:9092,10.222.201.103:9092
```

---

## Summary: Quick Wiring Checklist

### Shared PVCs (must create before deploying)

| PVC Name | Mode | Consumers |
|---------|------|-----------|
| `comms-audience-data-pvc` | RWX | data-file-download (write), time-validation (read), test-campaign (read) |
| `comms-exclusion-data-pvc` | ROX or RWX | exclusion-scan (read) |
| `comms-keytab-pvc` or Secret | RO | campaign-manager, campaign-processor, data-file-download, event-enrichment |

### Service URL wiring matrix

| Property Key (in service) | Service That Sets It | Points To |
|--------------------------|---------------------|-----------|
| `external.tgcg.baseUrl` | event-enrichment only | `uclm-campaign-cg-exclusion:8080` |
| `external.exclusion.baseUrl` | event-enrichment, time-validation | `uclm-campaign-exclusion-scan:8080` |
| `external.goal.baseUrl` | event-enrichment, time-validation, test-campaign | `uclm-campaign-manager:80` |
| `external.content-manager.baseUrl` | event-enrichment, time-validation, test-campaign | Content Manager URL |
| `external.audience.baseUrl` | event-enrichment | Audience Manager URL |
| `am.base-url` | audience-push | Audience Manager URL |
| `auth-manager.base-url` | audience-push, data-file-download, time-validation, analytics | Auth Manager URL |
| `cms.base-url` | campaign-manager | CMS Service URL |
| `druid.broker.url` | analytics-reporting | Druid broker URL |

### AM Callback registration (manual action required)

> Audience Manager is an **external system**. It must be configured to POST to campaign-processor when file creation completes:
>
> ```
> Callback URL → POST http://<campaign-processor-svc>:8080/api/v1/callback
> ```
>
> This is NOT set in any config file — it must be registered in the Audience Manager admin/config directly.

### Kafka topic ownership per cluster

| Cluster | Topics | Producer | Consumer |
|---------|--------|---------|---------|
| A (Main) | `uclm_analytics` | campaign-manager | analytics-reporting |
| A (Main) | `control_file_request` | campaign-processor | data-file-download |
| A (Main) | `dimension_refresh_topic` | analytics-reporting | (downstream — external) |
| A (Main) | `event_enrichment_prod` | (external NRT feed) | event-enrichment |
| B (NRT) | `channel_partner_*_nrt_svc_valgov` | event-enrichment, time-validation, test-campaign | (channel partners — external) |
| B (NRT) | `cs_raw_reporting_topic` | event-enrichment, time-validation | (reporting — external) |
| C (Status) | `uclm_campaign_status` | analytics-reporting | (downstream — external) |
| (bummlebee) | `orchestrator.request` | campaign-manager | bummlebee (email dispatcher) |
