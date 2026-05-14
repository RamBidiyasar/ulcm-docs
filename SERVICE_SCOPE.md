# UCLM — Service Scope & Possible Values

> For each service: what dimension it is scoped by, and what the known possible values are.  
> Last updated: 2026-05-12

---

| Service | Scope | Possible Values |
|---------|-------|----------------|
| **uclm-campaign-cg-exclusion** | Tenant | Any configured `tenantId` (dynamic — stored in DB) |
| **uclm-campaign-time-validation** | Tenant · Workspace | `tenantId` from `x-tenant-id` header · `workspaceId` from `x-workspace-id` header (both dynamic per request) |
| **uclm-campaign-manager** | Tenant · Channel | Channel: `SMS` · `Email` · `Push` · `WhatsApp` · `RCS` |
| **uclm-test-campaign** | Tenant · Workspace · Channel | Channel: `SMS` · `Email` · `Push` · `WhatsApp` · `RCS` · Workspace & Tenant: dynamic per request |
| **uclm-campaign-exclusion-scan** | *(none — utility)* | Processes all files in `INGEST_BASE_FOLDER` — no tenant/channel scope |
| **uclm-campaign-audience-push** | Tenant · Channel | Tenant: list configured in `tenantProperties.ids` · Channel: `Push` (hardcoded) |
| **uclm-campaign-processor** | Tenant · Campaign | `tenantId` + `campaignId` + `audienceId` (all dynamic — from DB / Kafka message) |
| **uclm-campaign-manager-event-enrichment** | Tenant · Workspace · Channel · Campaign · User | Channel: `SMS` · `Email` · `Push` · `WhatsApp` · `RCS` · Tenant & Workspace: from Kafka event · User: individual MSISDN / audience record |
| **uclm-analytics-reporting-service** | Tenant · Workspace · Channel | Channel: `SMS` · `Email` · `Push` · `WhatsApp` · `RCS` · Tenant & Workspace: from Kafka event |
| **uclm-campaign-data-file-dowload** | Tenant · Campaign | `tenantId` + `campaignId` + `audienceId` (dynamic) |
| **uclm-validation-governance-service** | Channel *(one deployment per channel)* | `SMS-IQ` · `SMS-Lobby` · `Email` · `Email-SMTP` · `Email-Attachment` · `Push` · `RCS` · `WhatsApp` · `D2C` |
| **uclm-dlr-aerospike-cache-loader** | Channel *(one deployment per channel, via Spring profile)* | `SMS` · `Email-Netcore` · `Push` · `RCS` · `WhatsApp` |
| **uclm-orchestrator-service** | Channel *(one deployment per channel)* | `SMS-IQ` · `SMS-Lobby` · `Email-SMTP` · `Email-Netcore` · `RCS` |
| **uclm-dlr-api-service** | Channel *(one deployment per channel, via Spring profile)* | `SMS` · `Email-Netcore` · `RCS` |
| **uclm-rate-controller-service** | Team · Channel · Tenant | Team: `teamId` (dynamic) · Channel: `SMS` · `Email` · `WhatsApp` · `RCS` · `Push` · Tenant: `tenantId` (dynamic) |
| **uclm-dlr-enricher** | Channel *(one deployment per channel, via Spring profile)* | `SMS` · `Email-Netcore` · `Push` · `RCS` · `WhatsApp` |
| **uclm-contentmgmt** | Tenant · Workspace · Channel | Channel: `SMS` · `RCS` · `WhatsApp` · `Push` · Tenant & Workspace: dynamic per request |
| **uclm-auth-manager** | Tenant · Workspace · User | `tenantId` · `workspaceId` · `userId` / `orgId` (all dynamic — managed entities in DB) |

---

## Notes

- **Deployment-level scope** (e.g., orchestrator, validation-governance, dlr-aerospike-cache-loader, dlr-enricher, dlr-api-service) — the channel is fixed at deploy time via Helm values file or `SPRING_PROFILES_ACTIVE`. A separate pod/release exists per channel value.
- **Runtime scope** (e.g., campaign-manager, campaign-time-validation, analytics) — the scope dimension (tenant, workspace, channel) is resolved at runtime from request headers or Kafka message fields.
- **`rate-controller-service`** is the only service scoped at **team level** — `teamId` is a first-class key in its TPS rate-limiting logic (`tryAcquire(teamId, teamTps)`).
- **`campaign-exclusion-scan`** has no business scope — it is a file-based utility scheduler.
