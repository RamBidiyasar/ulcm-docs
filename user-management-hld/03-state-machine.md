# User Management — State Machines

> Three state machines govern the lifecycle of key domain objects in the user management platform.

---

## 1. User Account State Machine (uclm-auth-manager)

```mermaid
stateDiagram-v2
    [*] --> Active : POST /user (createUser)

    Active --> SoftDeleted : DELETE /user/delete/{id}\n(isDeleted=true, isActive=false)
    SoftDeleted --> Active : POST /user with same userName\n(re-activation: isDeleted=false, isActive=true)

    Active --> AccessRevoked : DELETE /user/permission\n(UserAccess record deleted)
    AccessRevoked --> Active : PATCH /user/permission\n(new access assignment)

    note right of Active
        User record in auth_user_details
        isActive=true, isDeleted=false
        Has ≥1 UserAccess record
    end note

    note right of SoftDeleted
        User record stays in DB
        isActive=false, isDeleted=true
        All UserAccess records deleted
        Hidden by Hibernate deletedUserFilter
    end note
    classDef active fill:#2980b9,color:#fff,stroke:#1f618d
    class Active active
```

### User State Transitions

| From State | Event | To State | Table / Column |
|-----------|-------|----------|----------------|
| — | `POST /user` | Active | `auth_user_details.is_active=true, is_deleted=false` |
| Active | `DELETE /user/delete/{id}` | SoftDeleted | `is_active=false, is_deleted=true`; all `auth_user_access` rows deleted |
| SoftDeleted | `POST /user` (same username) | Active | `is_active=true, is_deleted=false` (re-activated in-place) |
| Active | `DELETE /user/permission` | AccessRevoked | `auth_user_access` row deleted |
| AccessRevoked | `PATCH /user/permission` | Active | New `auth_user_access` row created |

---

## 2. SSO Session State Machine (uclm-auth-manager)

```mermaid
stateDiagram-v2
    [*] --> Unauthenticated : User visits app

    Unauthenticated --> SSOInitiated : POST /ssoLogin\n(gets ssoRedirectUrl)

    SSOInitiated --> IdPAuthentication : Browser navigates to IdP
    IdPAuthentication --> TokenIssued : SAML assertion POSTed\nto /user/saml/response\n(JWT generated, session stored in Aerospike)

    TokenIssued --> WorkspaceSwitched : POST /switch/workspace\n(new JWT issued)
    WorkspaceSwitched --> TokenIssued : (prior workspace)

    TokenIssued --> LogoutInitiated : GET /ssoLogout\n(token extracted, Aerospike session deleted)
    LogoutInitiated --> Unauthenticated : SAML logout to IdP completes\n(/user/saml/logout/response)

    TokenIssued --> SessionExpired : Aerospike TTL expires (default 300s)
    SessionExpired --> Unauthenticated : User must re-login

    note right of TokenIssued
        JWT stored client-side
        Aerospike session: TTL=300s
        Contains: user_id, session_id,
        idp_ses_idx, created_on, expires_on
    end note
    classDef init fill:#95a5a6,color:#000,stroke:#7f8c8d
    classDef fail fill:#e74c3c,color:#fff,stroke:#c0392b
    class SessionExpired fail
    class LogoutInitiated,SSOInitiated init
```

### JWT Token Claims

| Claim | Key | Type | Value |
|-------|-----|------|-------|
| Subject | `sub` | String | `user.username` (email) |
| User ID | `x-user-id` | String | `user.username` |
| User ID (alt) | `x-usr-id` | String | `user.username` |
| Tenant ID | `x-tenant-id` | String | `user.tenantId` |
| Workspace ID | `x-workspace-id` | String | `userAccess.nodeId` |
| User Hierarchy | `x-user-hierarchy` | String | `"tenantId-parentNodeId-nodeId"` |
| Session ID | `x-session-id` | String | UUID (preserved across workspace switches) |
| Issued At | `iat` | Long | Epoch seconds |
| Expiry | `exp` | Long | `iat + jwt.token.validity ms` (default 360,000 ms = 6 min) |

---

## 3. Template Approval State Machine (uclm-contentmgmt)

```mermaid
stateDiagram-v2
    [*] --> DRAFT : template created locally\n(not yet submitted)

    DRAFT --> L1_PENDING : POST /template\n(auto-set on creation)

    L1_PENDING --> L2_PENDING : PUT /template/approve/l1/{id}\n(L1 approver action)
    L1_PENDING --> REJECTED : PUT /template/reject/l1\n(with rejection reason)

    L2_PENDING --> CHANNEL_REG_PENDING : PUT /template/approve/l2/{id}\n(WhatsApp/RCS channels)\nPlugin submits to channel platform
    L2_PENDING --> CHANNEL_APPROVAL_PENDING : PUT /template/approve/l2/{id}\n(asynchronous channel approval)
    L2_PENDING --> APPROVED : PUT /template/approve/l2/{id}\n(SMS/Email/Push channels\nno external registration needed)
    L2_PENDING --> REJECTED : PUT /template/reject/l2\n(with rejection reason)

    CHANNEL_REG_PENDING --> CHANNEL_APPROVAL_PENDING : Registration successful\n(ApprovalPollingScheduler)
    CHANNEL_REG_PENDING --> CHANNEL_REG_PENDING : Registration retry\n(RegistrationRetryScheduler)

    CHANNEL_APPROVAL_PENDING --> APPROVED : Channel platform approves\n(ApprovalPollingScheduler polls every 15 min)
    CHANNEL_APPROVAL_PENDING --> REJECTED : Channel platform rejects

    APPROVED --> L1_PENDING : PUT /template (edit)\n(re-opens for revision)

    REJECTED --> L1_PENDING : (re-submit after edit — planned)

    note right of L1_PENDING
        Created by content author
        Awaiting L1 reviewer action
        State set by TemplateServiceImpl.addTemplate()
    end note

    note right of CHANNEL_REG_PENDING
        WhatsApp/RCS only
        Plugin.register() called
        Retry scheduler runs every 15 min
        Timeout: PT24H (configurable)
    end note

    note right of APPROVED
        Template ready for use in campaigns
        channelId set from plugin response
        Can be re-edited  returns to L1_PENDING
    end note
    classDef init fill:#95a5a6,color:#000,stroke:#7f8c8d
    classDef fail fill:#e74c3c,color:#fff,stroke:#c0392b
    class REJECTED fail
    class CHANNEL_APPROVAL_PENDING,CHANNEL_REG_PENDING,DRAFT,L1_PENDING,L2_PENDING init
```

### Template State Transitions

| From | Event / Trigger | To | Condition |
|------|----------------|-----|-----------|
| — | `POST /template` | `L1_PENDING` | Always; template created and auto-submitted |
| `L1_PENDING` | `PUT /approve/l1` | `L2_PENDING` | L1 approver approves |
| `L1_PENDING` | `PUT /reject/l1` | `REJECTED` | L1 approver rejects (reason required) |
| `L2_PENDING` | `PUT /approve/l2` | `CHANNEL_REG_PENDING` | WhatsApp/RCS channels with plugin |
| `L2_PENDING` | `PUT /approve/l2` | `APPROVED` | SMS/Email/Push (no external registration) |
| `L2_PENDING` | `PUT /reject/l2` | `REJECTED` | L2 approver rejects (reason required) |
| `CHANNEL_REG_PENDING` | Scheduler: register success | `CHANNEL_APPROVAL_PENDING` | Plugin returns registration ID |
| `CHANNEL_REG_PENDING` | Scheduler: retry (every 15 min) | `CHANNEL_REG_PENDING` | Retry until success or timeout (24h) |
| `CHANNEL_APPROVAL_PENDING` | Scheduler: poll approval | `APPROVED` | Channel platform returns APPROVED status |
| `CHANNEL_APPROVAL_PENDING` | Scheduler: poll rejection | `REJECTED` | Channel platform returns REJECTED status |
| `APPROVED` | `PUT /template` (edit) | `L1_PENDING` | Template edited by author; re-enters review cycle |

---

## 4. Media State Machine (uclm-contentmgmt)

```mermaid
stateDiagram-v2
    [*] --> L1_PENDING : POST /media/upload\n(file stored in GCS/S3)

    L1_PENDING --> L2_PENDING : PUT /media/approve/l1/{mediaId}
    L1_PENDING --> REJECTED : PUT /media/reject/{mediaId}\n(reason required, 10-300 chars)

    L2_PENDING --> APPROVED : PUT /media/approve/l2/{mediaId}
    L2_PENDING --> REJECTED : PUT /media/reject/{mediaId}

    APPROVED --> INACTIVE : PUT /media/delete/{mediaId}

    note right of APPROVED
        Media ready for use in templates
        channelMedia map populated
        (per-channel media registration IDs)
    end note
    classDef init fill:#95a5a6,color:#000,stroke:#7f8c8d
    classDef active fill:#2980b9,color:#fff,stroke:#1f618d
    classDef fail fill:#e74c3c,color:#fff,stroke:#c0392b
    class INACTIVE active
    class REJECTED fail
    class L1_PENDING,L2_PENDING init
```

### Media State Transitions

| From | Event | To | Notes |
|------|-------|----|-------|
| — | `POST /media/upload` | `L1_PENDING` | File stored in GCS/S3; metadata in MongoDB |
| `L1_PENDING` | `PUT /approve/l1` | `L2_PENDING` | L1 review pass |
| `L1_PENDING` | `PUT /reject` | `REJECTED` | Reason: 10–300 characters |
| `L2_PENDING` | `PUT /approve/l2` | `APPROVED` | Final approval |
| `L2_PENDING` | `PUT /reject` | `REJECTED` | Reason: 10–300 characters |
| `APPROVED` | `PUT /delete` | `INACTIVE` | Soft deactivation |
