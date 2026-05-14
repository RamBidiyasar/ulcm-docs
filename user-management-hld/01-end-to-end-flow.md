# User Management — End-to-End Flow

> Full integrated flow from initial login through token-based authorization to content management operations.

---

## Flow Overview

```mermaid
flowchart TD
    USER([ User / Browser])

    subgraph Step1["Step 1 — Initiate SSO Login"]
        L1["POST /auth-manager/api/v1/ssoLogin\n{userName: user@example.com}"]
    end

    subgraph Step2["Step 2 — SAML Redirect"]
        L2["auth-manager looks up user's tenantId\nResolves SAML config from TenantConfig\nBuilds AuthnRequest  deflate+base64+URLencode\nReturns ssoRedirectUrl"]
        SAML_IDP["SAML Identity Provider\n(AD FS / Corporate SSO)"]
    end

    subgraph Step3["Step 3 — SAML Assertion Callback"]
        L3["POST /auth-manager/api/v1/user/saml/response\n(Form data: SAMLResponse=...)"]
        L3B["auth-manager parses SAML assertion\nExtracts email from emailaddress attribute\nValidates signature + timing + audience\nFetches User from Oracle DB\nGenerates JWT (HS256)\nStores session in Aerospike (TTL=300s)\nHTTP 302  UI with base64 JWT"]
    end

    subgraph Step4["Step 4 — Authenticated API Calls"]
        L4["Client includes IAM headers:\nx-tenant-id, x-workspace-id\nx-user-id, x-user-hierarchy\n(extracted from JWT)"]
    end

    subgraph Step5["Step 5 — Workspace Switch (Optional)"]
        L5["POST /auth-manager/api/v1/switch/workspace?workspaceId=X\nAuthorization: Bearer <token>"]
        L5B["Validates user access to target workspace\nRe-issues JWT with new workspace scope\nPreserves existing session_id"]
    end

    subgraph Step6["Step 6 — Content Management"]
        L6["Content API calls to uclm-contentmgmt\nwith IAM headers for auth context"]
        L6B["Create/List Templates\nApprove L1  L2\nRegister to WhatsApp/RCS\nUpload Media\nManage Bundles"]
    end

    USER --> L1 --> L2
    L2 -->|"ssoRedirectUrl"| USER
    USER -->|"Authenticate"| SAML_IDP
    SAML_IDP -->|"POST SAMLResponse"| L3
    L3 --> L3B
    L3B -->|"HTTP 302 + JWT"| USER
    USER --> L4 --> L5 --> L5B
    USER --> L6 --> L6B
    classDef svc fill:#2980b9,color:#fff,stroke:#1f618d
    classDef db fill:#27ae60,color:#fff,stroke:#1e8449
    classDef ext fill:#8e44ad,color:#fff,stroke:#6c3483
    classDef channel fill:#d35400,color:#fff,stroke:#a04000
    classDef user fill:#34495e,color:#fff,stroke:#2c3e50
    class L6B channel
    class L3B db
    class L2,L3,SAML_IDP ext
    class L5,L6 svc
    class L1,L4,L5B,USER user
```

---

## Detailed Flow: Login to Content Creation

```mermaid
sequenceDiagram
    participant Browser
    participant AuthManager as uclm-auth-manager
    participant OracleDB as Oracle DB
    participant AerospikeDB as Aerospike
    participant SAML_IdP as SAML IdP
    participant FrontendUI as Frontend UI
    participant ContentMgr as uclm-contentmgmt
    participant MongoDB as MongoDB

    Note over Browser,FrontendUI:  Phase 1: SSO Login Initiation 
    Browser->>AuthManager: POST /ssoLogin {userName: "user@corp.com"}
    AuthManager->>OracleDB: findByUsername("user@corp.com")  User{tenantId=1}
    OracleDB-->>AuthManager: User entity
    AuthManager->>OracleDB: findByTenantId(1)  TenantConfig{authConfig}
    OracleDB-->>AuthManager: SAML config (loginUrl, entityId, certificate, acsUrl…)
    AuthManager->>AuthManager: Build OpenSAML AuthnRequest\nDeflate  Base64  URLEncode
    AuthManager-->>Browser: {ssoRedirectUrl: "https://idp.corp.com/saml?SAMLRequest=..."}

    Note over Browser,SAML_IdP:  Phase 2: IdP Authentication 
    Browser->>SAML_IdP: Navigate to ssoRedirectUrl
    SAML_IdP->>Browser: Present corporate login form
    Browser->>SAML_IdP: Submit credentials
    SAML_IdP->>SAML_IdP: Authenticate user, build SAML Assertion
    SAML_IdP-->>Browser: HTTP POST form to ACS URL

    Note over Browser,AerospikeDB:  Phase 3: SAML Callback & JWT Issuance 
    Browser->>AuthManager: POST /user/saml/response (SAMLResponse=base64...)
    AuthManager->>AuthManager: Decode SAMLResponse (OpenSAML)\nExtract email from assertion attribute
    AuthManager->>OracleDB: findByUsername(email)  User
    OracleDB-->>AuthManager: User{tenantId, userId}
    AuthManager->>OracleDB: findByTenantId(tenantId)  TenantConfig
    OracleDB-->>AuthManager: SAML config for tenant
    AuthManager->>AuthManager: validateSAMLLoginResponse()\nCheck signature, timing, audience, conditions
    AuthManager->>OracleDB: findById(tenantId)  OrgParentHierarchy (root node)
    OracleDB-->>AuthManager: Tenant root org node
    AuthManager->>AuthManager: generateToken(user, orgNode, accessList, sessionId)\nHS256 JWT with: sub, x-user-id, x-tenant-id,\nx-workspace-id, x-user-hierarchy, x-session-id, roles
    AuthManager->>AerospikeDB: put(key=username, TTL=300s)\nbins: user_id, session_id, idp_ses_idx, created_on, expires_on
    AerospikeDB-->>AuthManager: OK
    AuthManager-->>Browser: HTTP 302  uiRedirectUrl?loginResponse=base64JWT

    Note over Browser,FrontendUI:  Phase 4: Authenticated Session 
    Browser->>FrontendUI: Navigate with JWT in query param
    FrontendUI->>FrontendUI: Parse JWT, extract IAM headers\nStore token for API calls

    Note over FrontendUI,ContentMgr:  Phase 5: Content Management 
    FrontendUI->>ContentMgr: POST /content-manager/api/v1/template\nHeaders: x-tenant-id: 1, x-workspace-id: 5\nx-user-id: user@corp.com, x-user-hierarchy: 1-3-5\nBody: {templateName, channel: "SMS", ...}
    ContentMgr->>ContentMgr: AppFilter: validate IAM headers, set RequestContext
    ContentMgr->>ContentMgr: Validate template data (channel-specific validator)\nEnrich (normalize params, set DLT fields for SMS)\nSet state=L1_PENDING, tenant, department, variant="v1"
    ContentMgr->>MongoDB: save(TemplateDAO)
    MongoDB-->>ContentMgr: TemplateDAO{templateId}
    ContentMgr-->>FrontendUI: TemplateDAO (HTTP 200)

    Note over FrontendUI,ContentMgr:  Phase 6: Template Approval Workflow 
    FrontendUI->>ContentMgr: PUT /template/approve/l1/{templateId}
    ContentMgr->>ContentMgr: WorkflowEngine: L1_PENDING  L2_PENDING transition
    ContentMgr->>MongoDB: update state=L2_PENDING
    ContentMgr-->>FrontendUI: Updated TemplateDAO

    FrontendUI->>ContentMgr: PUT /template/approve/l2/{templateId}
    ContentMgr->>ContentMgr: WorkflowEngine: L2_PENDING  CHANNEL_REG_PENDING\n(for WhatsApp/RCS) or APPROVED (for SMS/Email)
    ContentMgr->>MongoDB: update state
    ContentMgr-->>FrontendUI: Updated TemplateDAO
```

---

## IAM Header Flow

Every request to both services (except whitelisted paths) must carry these headers:

| Header | Source | Used For |
|--------|--------|----------|
| `x-tenant-id` | JWT claim | Tenant data isolation |
| `x-workspace-id` | JWT claim | Workspace/department scoping |
| `x-user-id` | JWT claim | Audit trails, self-service prevention |
| `x-user-hierarchy` | JWT claim (format: `1-3-5`) | Hierarchical access checks |

### Whitelisted Paths (No IAM Headers Required)

| Service | Path | Reason |
|---------|------|--------|
| auth-manager | `POST /ssoLogin` | Login initiation — user not yet authenticated |
| auth-manager | `POST /user/saml/response` | SAML callback from IdP |
| auth-manager | `GET /tenant/config/**` | Public tenant config lookup |
| auth-manager | `GET /user/saml/logout/response` | SAML logout callback |
| contentmgmt | `POST /template/callback/sms` | DLT webhook callback from SMS provider |

---

## Workspace Switch Sub-Flow

```mermaid
sequenceDiagram
    participant Client
    participant AuthManager as uclm-auth-manager
    participant OracleDB as Oracle DB

    Client->>AuthManager: POST /switch/workspace?workspaceId=7\nAuthorization: Bearer <existing-JWT>
    AuthManager->>AuthManager: Extract username, tenantId, currentWorkspaceId from JWT
    alt currentWorkspaceId == 7
        AuthManager-->>Client: {accessToken: <same JWT>}
    else switching to new workspace
        AuthManager->>OracleDB: findByUser_UsernameAndNode_IdAndTenantId(username, 7, tenantId)
        OracleDB-->>AuthManager: UserAccess{role, node}
        AuthManager->>OracleDB: findById(tenantId)  OrgParentHierarchy root
        OracleDB-->>AuthManager: root org node
        AuthManager->>AuthManager: generateToken(user, rootNode, newAccess, existingSessionId)
        AuthManager-->>Client: {accessToken: <new JWT with workspaceId=7>}
    end
```
