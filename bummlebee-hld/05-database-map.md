# Database & Cache Interaction Map

All databases, caches, and storage systems: their owner and which services read or write them.

---

## Data Store Ownership & Access

```mermaid
flowchart TD
    subgraph PG[" PostgreSQL — Rate Controller DB"]
        T1[("tenant_onboarding")]
        T2[("team_onboarding")]
    end

    subgraph AS[" Aerospike — DLR Cache"]
        AS1[("dispatch_records\n(primary key: requestId/UUID)")]
    end

    subgraph Lookups[" Lookup Files (mounted volumes)"]
        LF1[("raw/\n(raw lookup CSVs)")]
        LF2[("schema/\n(field schema definitions)")]
        LF3[("normalized/\n(normalized value maps)")]
    end

    subgraph RC["Rate Controller\n:8081"]
        RC_SVC["uclm-rate-controller-service"]
    end

    subgraph VG["Validation Governance\n:7777"]
        VG_SVC["uclm-validation-governance-service"]
    end

    subgraph CACHE["Aerospike Cache Loader"]
        CACHE_SVC["uclm-dlr-aerospike-cache-loader"]
    end

    subgraph ENR["DLR Enricher"]
        ENR_SVC["uclm-dlr-enricher"]
    end

    %% Rate Controller owns PostgreSQL
    RC_SVC -->|"READ TPS config\n(per-team, per-tenant)"| T1
    RC_SVC -->|"READ TPS config\n(per-team)"| T2

    %% Validation Governance reads lookup files
    VG_SVC -->|"READ at startup + hot reload"| LF1
    VG_SVC -->|"READ at startup + hot reload"| LF2
    VG_SVC -->|"READ at startup + hot reload"| LF3

    %% Aerospike Cache Loader writes
    CACHE_SVC -->|"WRITE dispatch records\n(primary key: requestId)"| AS1

    %% DLR Enricher reads
    ENR_SVC -->|"READ by requestId\n(O(1) lookup)"| AS1
    classDef svc fill:#2980b9,color:#fff,stroke:#1f618d
    classDef db fill:#27ae60,color:#fff,stroke:#1e8449
    class AS1,CACHE_SVC,LF1 db
    class ENR_SVC,RC_SVC,VG_SVC svc
```

---

## PostgreSQL Schema (Rate Controller)

### `tenant_onboarding`

| Column | Type | Description |
|--------|------|-------------|
| `id` | BIGINT PK | Auto-generated ID |
| `tenant_id` | VARCHAR | Unique tenant identifier |
| `channel` | VARCHAR | Channel type (SMS, WA, EMAIL, PUSH, RCS) |
| `tps` | INT | Max TPS for this tenant+channel |
| `created_at` | TIMESTAMP | Record creation time |
| `updated_at` | TIMESTAMP | Last update time |

### `team_onboarding`

| Column | Type | Description |
|--------|------|-------------|
| `id` | BIGINT PK | Auto-generated ID |
| `team_id` | VARCHAR | Unique team identifier |
| `tenant_id` | VARCHAR | Parent tenant |
| `channel` | VARCHAR | Channel type |
| `tps` | INT | Max TPS for this team+channel |
| `created_at` | TIMESTAMP | Record creation time |

---

## Aerospike Schema (DLR Cache)

### Namespace / Set: `dispatch_records`

| Bin Name | Type | Description |
|----------|------|-------------|
| `requestId` | STRING (PK) | Primary key — message UUID from dispatch |
| `channel` | STRING | Channel type sent on |
| `mobile` | STRING | Target mobile number |
| `campaignId` | STRING | Campaign identifier |
| `templateId` | STRING | Template identifier |
| `sentAt` | LONG | Epoch timestamp of dispatch |
| `tenantId` | STRING | Tenant identifier |
| `teamId` | STRING | Team identifier |
| *(all other fields)* | ANY | All fields from the dispatch record are preserved including null/blank |

**Key characteristics:**
- Dynamic schema — all dispatch record fields stored as-is
- TTL configured per namespace (records expire after a configurable period)
- O(1) read by primary key (requestId)

---

## Lookup Files (Validation Governance)

| Folder | Content | Usage |
|--------|---------|-------|
| `lookups/raw/` | Raw provider-specific lookup CSVs | Input normalization per MOC/channel |
| `lookups/schema/` | Field schema definitions per channel | Validates required fields, types, lengths |
| `lookups/normalized/` | Normalized output value maps | Maps raw values to canonical form |

Files are loaded into memory at startup and the service **does not require a restart** to reload — hot reload is supported via configuration.

---

## Storage Summary Table

| Service | Data Store | Access Mode | What is stored |
|---------|-----------|-------------|----------------|
| Rate Controller | PostgreSQL | R (TPS config) | Tenant + team TPS limits per channel |
| Validation Governance | File System (lookup) | R at startup | Validation schemas, lookup tables |
| Orchestrator | None | — | Stateless, all config from properties |
| DLR API Service | None | — | Stateless, no persistence |
| Aerospike Cache Loader | Aerospike | W | Full dispatch records keyed by requestId |
| DLR Enricher | Aerospike | R | Lookup dispatch records for DLR correlation |
