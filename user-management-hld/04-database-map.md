# User Management — Database Map

> Complete database schema for both services. uclm-auth-manager uses Oracle (relational), uclm-contentmgmt uses MongoDB (document store).

---

## uclm-auth-manager — Oracle DB

### Entity Relationship Overview

```
auth_org_hierarchy
    ├── id (PK)
    ├── parent_id → auth_org_hierarchy.id
    └── tenant_id

auth_user_details
    └── (tenant_id → auth_org_hierarchy.id conceptually)

auth_user_access
    ├── user_id → auth_user_details.user_id
    ├── node_id → auth_org_hierarchy.id
    └── role_id → auth_role.role_id

auth_role
    └── role_id (PK)

auth_resource
    └── id (PK)

auth_resource_endpoint
    ├── id (PK)
    └── resource_id → auth_resource.id

auth_role_endpoint
    ├── id (PK)
    ├── role_id → auth_role.role_id
    └── endpoint_id → auth_resource_endpoint.id

auth_tenant_config
    └── tenant_id (FK to org hierarchy root)
```

---

### Table: `auth_user_details`

Entity: `User.java`

| Column | Type | Constraints | Description |
|--------|------|-------------|-------------|
| `user_id` | `NUMBER` | PK, auto (sequence: `AUTH_USER_DETAILS_SEQUENCE`) | Internal user ID |
| `username` | `VARCHAR2` | UNIQUE, NOT NULL | User's email address / login ID |
| `password` | `VARCHAR2` | nullable | Password (not currently used — SSO only) |
| `is_active` | `BOOLEAN` | default `true` | Whether user account is active |
| `is_deleted` | `BOOLEAN` | default `false`, NOT NULL | Soft-delete flag; filtered by Hibernate `deletedUserFilter` |
| `tenant_id` | `NUMBER` | NOT NULL | ID of the tenant org root node |
| `created_by` | `VARCHAR2` | | Username of creator |
| `created_on` | `TIMESTAMP` | | Creation timestamp (UTC) |
| `updated_by` | `VARCHAR2` | | Username of last updater |
| `updated_on` | `TIMESTAMP` | | Last update timestamp (UTC) |

**Hibernate Filter:** `deletedUserFilter` (param: `isDeletedFlag`) applies condition `is_deleted = :isDeletedFlag`. Activated via `SoftDeleteAspect` AOP unless `@SkipDeletedFilter` is present.

---

### Table: `auth_org_hierarchy`

Entity: `OrgParentHierarchy.java`

| Column | Type | Constraints | Description |
|--------|------|-------------|-------------|
| `id` | `NUMBER` | PK, auto (sequence: `AUTH_ORG_HIERARCHY_SEQUENCE`) | Node ID |
| `org_name` | `VARCHAR2` | UNIQUE | Human-readable org/workspace name |
| `parent_id` | `NUMBER` | FK → self | Parent node ID (NULL for tenant root) |
| `tenant_id` | `NUMBER` | NOT NULL | Tenant root node ID (same as root's `id`) |
| `org_level` | `NUMBER` | | Level in hierarchy (0 = root tenant) |
| `created_by` | `VARCHAR2` | | |
| `created_on` | `TIMESTAMP` | | |
| `updated_by` | `VARCHAR2` | | |
| `updated_on` | `TIMESTAMP` | | |

**Note:** The root org node has `tenant_id = id` (self-referential). Child nodes inherit the root's `tenant_id`.

---

### Table: `auth_user_access`

Entity: `UserAccess.java`

| Column | Type | Constraints | Description |
|--------|------|-------------|-------------|
| `id` | `NUMBER` | PK, auto (sequence: `AUTH_USER_ACCESS_SEQUENCE`) | Access record ID |
| `user_id` | `NUMBER` | FK → `auth_user_details.user_id`, NOT NULL | The user |
| `node_id` | `NUMBER` | FK → `auth_org_hierarchy.id`, NOT NULL | Workspace node the user has access to |
| `role_id` | `NUMBER` | FK → `auth_role.role_id`, NOT NULL | Role assigned in that workspace |
| `tenant_id` | `NUMBER` | NOT NULL | Tenant context |

**Unique Constraint:** `auth_uk_user_node` on `(user_id, node_id)` — a user can only have one access record per workspace.

---

### Table: `auth_role`

Entity: `Role.java`

| Column | Type | Constraints | Description |
|--------|------|-------------|-------------|
| `role_id` | `NUMBER` | PK, auto | Role ID |
| `role_name` | `VARCHAR2` | UNIQUE | Role name (e.g., `ADMIN`, `EDITOR`, `VIEWER`) |
| `is_active` | `BOOLEAN` | default `true` | Whether role is active |
| `created_by` | `VARCHAR2` | | |
| `created_on` | `TIMESTAMP` | | |
| `updated_by` | `VARCHAR2` | | |
| `updated_on` | `TIMESTAMP` | | |

---

### Table: `auth_resource`

Entity: `Resource.java`

| Column | Type | Constraints | Description |
|--------|------|-------------|-------------|
| `id` | `NUMBER` | PK, auto | Resource ID |
| `resource_name` | `VARCHAR2` | | Display name (e.g., "Campaign Manager") |
| `resource_key` | `VARCHAR2` | | Machine key (e.g., `CAMPAIGN_MANAGER`) |
| `is_active` | `BOOLEAN` | | Whether resource is active |
| `created_by` | `VARCHAR2` | | |
| `created_on` | `TIMESTAMP` | | |

---

### Table: `auth_resource_endpoint`

Entity: `ResourceEndPoint.java`

| Column | Type | Constraints | Description |
|--------|------|-------------|-------------|
| `id` | `NUMBER` | PK, auto | Endpoint ID |
| `resource_id` | `NUMBER` | FK → `auth_resource.id` | Parent resource |
| `endpoint_name` | `VARCHAR2` | | Display name |
| `sub_module` | `VARCHAR2` | | Sub-module classification |
| `is_active` | `BOOLEAN` | default `true` | Active flag |
| `created_by` | `VARCHAR2` | | |
| `created_on` | `TIMESTAMP` | | |

---

### Table: `auth_role_endpoint`

Entity: `RoleEndPoint.java`

| Column | Type | Constraints | Description |
|--------|------|-------------|-------------|
| `id` | `NUMBER` | PK, auto | Mapping ID |
| `role_id` | `NUMBER` | FK → `auth_role.role_id` | Role |
| `endpoint_id` | `NUMBER` | FK → `auth_resource_endpoint.id` | Endpoint |

**Idempotent Insert:** `mapEndpointsToRole()` checks `existsByRole_RoleIdAndEndpoint_Id` before inserting to prevent duplicates.

---

### Table: `auth_tenant_config`

Entity: `TenantConfig.java`

| Column | Type | Description |
|--------|------|-------------|
| `id` | `NUMBER` | PK |
| `tenant_id` | `NUMBER` | Tenant org root ID |
| `auth_type` | `VARCHAR2` | Auth type (e.g., `SAML`) |
| `auth_config` | `CLOB/JSON` | JSON blob with SAML config: `{enabled, loginUrl, logoutUrl, certificate, entityId, acsUrl, spLoginUrl, spLogoutUrl, uiRedirectUrl, slackInSeconds, privateKey}` |
| `account_config` | `CLOB/JSON` | JSON blob with account config: `{userNameRegex, ...}` |

**Mapped by:** `JsonMapConverter` (JPA `@Converter`) serializes/deserializes Map ↔ JSON.

---

### Table: `org_configurations`

Entity: `OrgConfigurations.java` *(LDAP fields — currently unused in service code)*

| Column | Description |
|--------|-------------|
| `id` | PK |
| `ldap_url` | LDAP server URL |
| `bind_user` | LDAP bind user |
| `bind_password` | LDAP bind password |
| `security_type` | LDAP security type |

---

### Aerospike Session Store (uclm-auth-manager)

| Property | Value |
|----------|-------|
| Namespace | `auth.aerospike.nameSpace` (e.g., `arch`) |
| Set | `clm_user_session_{tenantId}` |
| Key | `username` (user's email) |
| TTL | `session.inactive.interval` (default 300 s) |

| Bin | Type | Description |
|-----|------|-------------|
| `user_id` | String | User's email/username |
| `session_id` | String | UUID (preserved across workspace switches) |
| `idp_ses_idx` | String | SAML IdP session index (for SLO) |
| `created_on` | String | ISO-8601 timestamp |
| `expires_on` | String | ISO-8601 expiry timestamp |

---

## uclm-contentmgmt — MongoDB

### Collections Overview

| Collection | DAO Class | Purpose |
|-----------|-----------|---------|
| `templates` | `TemplateDAO` | Message templates (SMS/Email/WhatsApp/RCS/Push/Voice) |
| `media` | `MediaDAO` | Uploaded media assets |
| `bundles` | `BundleDAO` | Template bundles (grouped templates) |
| `channel_configurations` | `ChannelConfiguration` | Per-tenant channel settings |
| `audit_log` | `AuditLog` | Field-level audit trail for all changes |
| `shedlock` | — | ShedLock distributed scheduler records |

---

### Collection: `templates`

Document: `TemplateDAO.java`

| Field | Type | Description |
|-------|------|-------------|
| `_id` (templateId) | String | MongoDB ObjectId |
| `templateName` | String | Human-readable template name (8–60 chars) |
| `templateNameNormalized` | String | Lowercased normalized name (for duplicate detection; `@JsonIgnore`) |
| `channel` | Enum (`Channel`) | `SMS`, `EMAIL`, `PUSH`, `WHATSAPP`, `RCS`, `VOICE` |
| `variant` | String | Template version (default `"v1"`) |
| `language` | String | Language code (e.g., `ENGLISH`, `HINDI`) |
| `templateCategory` | String | Category (e.g., `TRANSACTIONAL`, `PROMOTIONAL`) |
| `channelCategory` | String | Channel-specific category (e.g., `OTP`, `UTILITY`) |
| `tenant` | Integer | Tenant ID (from IAM header) |
| `department` | Integer | Department/workspace ID (from IAM header) |
| `state` | Enum (`State`) | Lifecycle state (see State Machine doc) |
| `sender` | String | Sender ID / Header (SMS) |
| `message` | String | Message body text |
| `paramDetails` | List\<ParamDetail\> | Parameter definitions (name, type) |
| `mediaDetails` | List\<MediaDetail\> | Media references |
| `hasMedia` | Boolean | Whether template includes media |
| `hasUrl` | Boolean | Whether template includes URL |
| `channelId` | `ChannelIdentifier` | Channel-specific registration identifier (set on APPROVED) |
| `templateData` | `ChannelTemplate` (polymorphic) | Channel-specific payload (SMS/Email/WhatsApp/RCS/Push) |
| `createdBy` | String | User ID from `RequestContext` |
| `createdOn` | Instant | Creation timestamp |
| `updatedBy` | String | Last updater |
| `updatedOn` | Instant | Last update timestamp |
| `configId` | String | ID of the `ChannelConfiguration` used (hidden from API) |

**Channel-specific templateData subtypes:**
- `SMSTemplate`: `{message, dltTemplateId, ...}`
- `EmailTemplate`: `{subject, body, fromName, ...}`
- `WhatsAppTemplate`: `{headerType, bodyText, footer, buttons, ...}`
- `RcsTemplate`: `{...RCS-specific fields}`
- `PushTemplate`: `{title, body, imageUrl, deepLink, ...}`

---

### Collection: `media`

Document: `MediaDAO.java`

| Field | Type | Description |
|-------|------|-------------|
| `_id` (mediaId) | String | MongoDB ObjectId |
| `mediaUrl` | String | Accessible URL of the stored file |
| `name` | String | Display name |
| `mname` | String | Normalized name (for unique constraint) |
| `tenant` | Integer | Tenant ID |
| `workspace` | Integer | Workspace ID |
| `mediaMeta` | `MediaMeta` | File metadata (size, MIME type, dimensions) |
| `channels` | `List<Channel>` | Channels this media is approved for |
| `storage` | `MediaStorage` | Storage backend info (GCS/S3 path, bucket) |
| `channelMedia` | `Map<Channel, MediaChannelInfo>` | Per-channel registration IDs |
| `state` | Enum (`State`) | Lifecycle state |
| `message` | String | Rejection reason or status message |
| `checksum` | String | File checksum (for deduplication) |
| `createdOn` / `updatedOn` | Instant | Timestamps |
| `createdBy` / `updatedBy` | String | User IDs |

**Compound Index:** `uniq_media_tenant_dept` on `{mname: 1, tenant: 1, department: 1}` — prevents duplicate media names per tenant/workspace.

---

### Collection: `bundles`

Document: `BundleDAO.java`

| Field | Type | Description |
|-------|------|-------------|
| `_id` (bundleId) | String | MongoDB ObjectId |
| `bundleName` (name) | String | Bundle display name |
| `bName` | String | Normalized bundle name |
| `category` | String | Bundle category |
| `description` | String | Human-readable description |
| `templates` | `Set<BundleTemplate>` | Set of {templateId, channel} references (NOT NULL, NOT EMPTY) |
| `tenant` / `department` | Integer | Scoping |
| `templateDetails` | `List<TemplateDAO>` | Populated on read (joined from `templates` collection) |
| `createdOn` / `updatedOn` | Instant | Timestamps |
| `createdBy` / `updatedBy` | String | User IDs |

---

### Collection: `channel_configurations`

Document: `ChannelConfiguration.java`

| Field | Type | Description |
|-------|------|-------------|
| `_id` (configId) | String | MongoDB ObjectId |
| `channel` | Enum (`Channel`) | Channel this config applies to |
| `tenant` | Integer | Tenant ID |
| `department` | Integer | Department/workspace ID |
| Configuration fields | Various | Channel-specific: sender keys, API credentials, feature flags |

**View DTOs per channel:** `ViewSMSConfigDTO`, `ViewEmailConfigDTO`, `ViewWhatsappConfigDTO`, `ViewRCSConfigDTO`, `ViewPushConfigDTO`, `ViewVoiceConfigDTO`

---

### Collection: `audit_log`

Document: `AuditLog.java`

| Field | Type | Description |
|-------|------|-------------|
| `_id` | String | MongoDB ObjectId |
| `entityId` | String | ID of the changed entity (templateId, mediaId, etc.) |
| `entityType` | String | Type of entity |
| `action` | String | Action performed |
| `changes` | `List<FieldChange>` | List of `{fieldName, oldValue, newValue}` |
| `performedBy` | String | User ID |
| `performedOn` | Instant | Timestamp |
