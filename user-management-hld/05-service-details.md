# User Management — Service Details

> Deep technical details for each service: key algorithms, business rules, important config, and component internals.

---

## 1. uclm-auth-manager

### 1.1 AppFilter — IAM Header Validation

Every request passes through `AppFilter` (`OncePerRequestFilter`) **before** reaching any controller.

```
Request arrives
    └─ URI matches /auth-manager/api/v1?
        └─ YES: Wrap in CachedBodyHttpServletRequest (allows body re-read)
            └─ URI in ignoreIamHeaderApis list?
                ├─ YES: skip header validation → forward to chain
                └─ NO:
                    ├─ Extract: x-tenant-id, x-workspace-id, x-user-id, x-user-hierarchy
                    ├─ Any blank? → 400 INSUFFICIENT_INFORMATION
                    └─ Validate tenantId in OrgParentHierarchy table
                        ├─ Not found? → 400 TENANT_INVALID
                        └─ Valid: Set RequestContext(tenantId, departmentId, userId, userHierarchy)
                               Set MDC(traceId, requestId, tenantId, userId)
                               → forward to chain
```

**Whitelisted paths (skip validation):**
- `POST /ssoLogin`
- `POST /user/saml/response`
- `GET /tenant/config/**`
- `GET /user/saml/logout/response`

### 1.2 JWT Token Algorithm

**Signing algorithm:** HS256 (HMAC-SHA256)  
**Secret:** `jwt.secret` property (Base64-encoded, 64+ bytes)  
**Validity:** `jwt.token.validity` milliseconds (default: 360,000 ms = 6 minutes)

```java
// Generation (JwtUtility.generateToken)
Jwts.builder()
    .setClaims(claims)               // x-user-id, x-tenant-id, x-workspace-id, x-user-hierarchy, x-session-id
    .setSubject(user.getUsername())
    .setIssuedAt(new Date())
    .setExpiration(new Date(now + validity))
    .signWith(hmacKey, SignatureAlgorithm.HS256)
    .compact()

// Validation (JwtUtility.extractAllClaims)
Jwts.parserBuilder()
    .setSigningKey(hmacKey)
    .build()
    .parseClaimsJws(token)
    .getBody()
```

### 1.3 SAML AuthnRequest Construction

```
1. Lookup user → get tenantId
2. SamlConfigResolver.resolve(tenantId):
   - Fetch TenantConfig from DB
   - Parse authConfig JSON → SamlRuntimeConfig
     (loginUrl, entityId, acsUrl, certificate, privateKey, slackInSeconds...)
3. SAMLSSOServiceHelper.generateAuthenticationRequest(config):
   - Bootstrap OpenSAML (if not already initialized)
   - Create AuthnRequest with:
     • ID = UUID
     • IssueInstant = now
     • AssertionConsumerServiceURL = acsUrl
     • Issuer = spEntityId
     • NameIDPolicy format = EMAIL
     • ProtocolBinding = HTTP-POST
4. Deflate AuthnRequest XML bytes (DEFLATE compression, no wrap)
5. Base64 encode (standard encoding)
6. URL encode
7. Return: idpLoginUrl + "?SAMLRequest=" + urlEncodedRequest
```

### 1.4 SAML Response Validation

`SAMLResponseValidator.validateSAMLLoginResponse()` performs:

| Check | Description |
|-------|-------------|
| Signature validation | Validate response or assertion signature against IdP certificate |
| Not Before | `assertion.conditions.notBefore <= now` |
| Not On Or After | `now < assertion.conditions.notOnOrAfter` |
| Slack tolerance | ±`slackInSeconds` (default 300 s) applied to timing checks |
| Audience restriction | ACS URL / SP entity ID must be in audience conditions |
| Status code | SAML Status must be `urn:oasis:names:tc:SAML:2.0:status:Success` |

### 1.5 User Onboarding Business Rules

1. **Username regex**: Read from `TenantConfig.accountConfig.userNameRegex`; must match if regex provided
2. **Self-onboarding prevention**: `RequestContext.getUserId() == req.userName` → `InvalidDataException`
3. **Soft-delete re-activation**: If user exists with `isDeleted=true`, reactivate in-place (preserves `userId`)
4. **Duplicate workspace access**: `existsByUser_UserIdAndNode_Id` check → `AlreadyExistsException`
5. **Workspace ownership**: Target workspace must belong to calling user's `tenantId`
6. **Async notification**: After saving `UserAccess`, calls `NotificationServiceClient.callNotificationApi(userName)` via `@Async` — non-blocking

### 1.6 Workspace Switch Logic

```
switchWorkspace(oldToken, workspaceId):
1. Extract username from JWT claims
2. Extract tenantId, currentWorkspaceId from JWT claims
3. If currentWorkspaceId == workspaceId → return original JWT unchanged
4. Otherwise:
   a. UserAccessRepository.findByUser_UsernameAndNode_IdAndTenantId(username, workspaceId, tenantId)
      → throws UnauthorizedException if not found
   b. WorkspaceService.getNodeWithoutTenant(tenantId) → root org node
   c. JwtUtility.generateToken(user, rootNode, newAccess, EXISTING sessionId from old token)
   d. Return JwtResponse{accessToken: newJWT}
```

### 1.7 Soft Delete AOP

`SoftDeleteAspect` (`@Aspect`) intercepts `@Repository` method calls:
- **Before** any `@Repository` method (without `@SkipDeletedFilter`): Enable Hibernate filter `deletedUserFilter` with `isDeletedFlag=false` → queries automatically exclude deleted users
- **After** `@SkipDeletedFilter`-annotated methods: Disable filter (used in SAML login flow where re-activating deleted users is intentional)

### 1.8 Multi-Tenant SAML Configuration

SAML configuration is stored **per-tenant** in `auth_tenant_config.auth_config` as a JSON map, not hardcoded in properties. This enables multiple corporate IdPs:

```json
{
  "enabled": true,
  "loginUrl": "https://adfs.corp.com/adfs/ls",
  "logoutUrl": "https://adfs.corp.com/adfs/ls?wa=wsignout1.0",
  "certificate": "MIICpDCCAYwCCQD...",
  "entityId": "https://uclm.airtel.in/auth",
  "acsUrl": "https://uclm.airtel.in/auth-manager/api/v1/user/saml/response",
  "spLoginUrl": "https://uclm.airtel.in/login",
  "spLogoutUrl": "https://uclm.airtel.in/logout",
  "uiRedirectUrl": "https://uclm.airtel.in",
  "slackInSeconds": 300,
  "privateKey": "MIIEvgIBADANBg..."
}
```

---

## 2. uclm-contentmgmt

### 2.1 AppFilter — IAM Header Validation

Same pattern as auth-manager but reads `x-tenant-id`, `x-workspace-id`, `x-user-id` and populates `RequestContext` (thread-local):
- **Only whitelisted path:** `POST /template/callback/sms`

### 2.2 Template Creation Flow

```
POST /template → TemplateServiceImpl.addTemplate(CreateTemplateDTO)
1. Resolve ContentValidator by channel (ValidatorResolver)
2. Map CreateTemplateDTO → TemplateDAO (TemplateMapper)
3. Get ChannelConfiguration for this tenant/channel
4. Set createdBy = RequestContext.userId
5. Resolve TemplateEnricher by channel (TemplateEnricherResolver):
   - SMS: Sets dltTemplateId, sets paramType, normalizes sender
   - WhatsApp: Populates header/button structures, media references
   - Email: Validates subject/body HTML
   - RCS: Sets rich content structure
   - Push: Sets notification fields
6. enricher.enrich(templateDAO)       ← pre-validation enrichment
7. validator.validate(templateDAO)    ← channel-specific validation
8. enricher.postEnrich(templateDAO)   ← post-validation enrichment
9. Set state = L1_PENDING
10. Set variant = "v1"
11. Set tenant, department from RequestContext
12. Set configId from ChannelConfiguration
13. templateRepository.save() → MongoDB
```

### 2.3 Template Approval Workflow Engine

`WorkflowEngine` + `StateRegistry` + Command pattern:

```
execute(CommandClass, TemplateContext):
1. Look up current template state → WorkflowState (from StateRegistry)
2. Check targetState in state.allowedTransitions() → ValidationException if not allowed
3. Execute the command:
   - ApproveL1Command:    transition L1_PENDING → L2_PENDING
   - ApproveL2Command:    transition L2_PENDING → CHANNEL_REG_PENDING / APPROVED
                          (calls PluginRegistry.getPlugin(channel).register(template))
   - RejectCommand:       transition X → REJECTED (set rejection reason)
   - EditTemplateCommand: transition APPROVED → L1_PENDING (create new version diff)
   - ChannelApproveCommand: transition CHANNEL_*_PENDING → APPROVED
4. WorkflowState.onEnter() callback → e.g., ApprovedState sets channelId from plugin response
5. Save updated TemplateDAO
6. Publish analytics event to Kafka (if configured)
7. Write AuditLog with field-level diffs (DeepDiffUtil)
```

### 2.4 Channel Plugin Architecture

`PluginRegistry` resolves `ChannelPlugin` by channel enum:

| Channel | Plugin | What it does |
|---------|--------|-------------|
| `WHATSAPP` | `WhatsAppPlugin` | POST to WhatsApp IQ API → get waTemplateId; poll for approval |
| `RCS` | `RcsPlugin` | POST to RCS IQ API → get rcsTemplateId; poll for approval |
| `SMS` | `SMSPlugin` | DLT registration (sync/async via callback) |
| `EMAIL` | `EmailPlugin` | Platform-specific registration (if configured) |
| `PUSH` | `PushPlugin` | FCM topic/token registration |
| default | `NoOpPlugin` | No external action; direct APPROVED |

### 2.5 Scheduler Architecture (ShedLock)

Two scheduled jobs run with distributed locking (ShedLock + MongoDB `shedlock` collection):

| Scheduler | Cron | Purpose |
|-----------|------|---------|
| `ApprovalPollingScheduler` | `0 */15 * * * *` (every 15 min) | Poll external channel platforms for approval status of `CHANNEL_APPROVAL_PENDING` templates |
| `RegistrationRetryScheduler` | `0 */15 * * * *` (every 15 min) | Retry failed registrations for `CHANNEL_REG_PENDING` templates |
| `MediaRegistrationScheduler` | `0,30 * * ? * *` (every 30 sec) | Register pending media files to channel platforms |

ShedLock ensures that in a multi-pod deployment, only one pod executes each job per interval.

### 2.6 Media Upload Flow

```
POST /media/upload (multipart: file + CreateMediaDTO meta)
1. Compute checksum (MD5/SHA) of file
2. Check for existing media with same mname+tenant+department → DataAlreadyExistsException
3. Upload file to GCS (GCSFileService) or S3 (S3FileService):
   - GCS: storage.objects().insert() to bucket `airtel-uclm-mediacdn`
   - S3: s3Client.putObject() to bucket `audmgr-exports-test`
4. Set mediaUrl = CDN URL (uclm.cdn.url + object path)
5. Set state = L1_PENDING
6. Save MediaDAO to MongoDB
7. Return MediaDTO (with mediaUrl for immediate access)
```

### 2.7 File Service Provider Selection

Controlled by `uclm.file.service.provider` property:
- `GCS` → `GCSFileService` (Google Cloud Storage, credentials from `/opt/gcp_credentials.json`)
- `S3` → `S3FileService` (AWS S3 / MinIO-compatible)
- `S3_PRESIGNED` → `S3PresignedUrlService` (generates presigned upload URLs)

### 2.8 Audit Logging

`AuditHandler` is invoked after every state-changing operation:
1. `DeepDiffUtil.diff(oldEntity, newEntity)` → `List<FieldChange{fieldName, oldValue, newValue}>`
2. Build `AuditLog{entityId, entityType, action, changes, performedBy, performedOn}`
3. Save to `audit_log` MongoDB collection

### 2.9 Analytics Event Publishing

`AnalyticsKafkaHandler` publishes `AnalyticsEvent` to Kafka topic after template/media lifecycle events. Producer config: `acks=all`, `enable.idempotence=true`. Kerberos SASL_PLAINTEXT in UAT/Prod (toggle: `kafka.kerberos.enabled`).

### 2.10 MVEL Template Field Setter

`MvelTemplateFieldSetter` uses MVEL expression language to dynamically set fields on template data objects. This allows channel-specific field population without reflection.

### 2.11 Bundle Variant Toggle

`content.bundle.enable.variant=false` — when enabled, bundles can have multiple variants. Currently disabled in default config.
