# User Management — REST API Endpoints

> All REST endpoints across both services. All paths under `/auth-manager/api/v1` and `/content-manager/api/v1` are wrapped in `AppResponse<T>` by `CustomResponseBodyAdvice`, which adds `data`, `errors`, and `meta` fields plus `X-Correlation-ID` header.

---

## uclm-auth-manager

**Base URL:** `/auth-manager/api/v1`  
**IAM Headers Required (unless noted):** `x-tenant-id`, `x-workspace-id`, `x-user-id`, `x-user-hierarchy`

### SSO / Authentication Endpoints

| Method | Path | IAM Required | Request | Response | Description |
|--------|------|:------------:|---------|----------|-------------|
| `POST` | `/ssoLogin` | ❌ | `SSOLoginRequest{userName}` | `{ssoRedirectUrl: string}` | Initiate SAML SSO — returns IdP redirect URL |
| `POST` | `/user/saml/response` | ❌ | Form params: `SAMLResponse=base64...` | HTTP 302 redirect | SAML IdP assertion callback — issues JWT, redirects to UI |
| `GET` | `/user/saml/logout/response` | ❌ | Query params (SAML logout) | HTTP 302 redirect | SAML IdP logout callback |
| `GET` | `/ssoLogout` | ✅ | `Authorization: Bearer <token>` header | `{message, logoutUrl}` | Initiate SSO logout — returns IdP logout URL |

#### SSOLoginRequest Schema
```json
{
  "userName": "user@corp.com"
}
```

#### AuthDecisionResponse (internal) → ssoLogin response
```json
{
  "ssoRedirectUrl": "https://idp.corp.com/adfs/ls?SAMLRequest=PHNhbWxwOkF1dGhu..."
}
```

---

### User Management Endpoints

| Method | Path | IAM Required | Request | Response | Description |
|--------|------|:------------:|---------|----------|-------------|
| `POST` | `/user` | ✅ | `UserRequestDto` | `UserResponseDto` | Create / onboard user with role in workspace |
| `GET` | `/user` | ✅ | — | `UserResponseDto` | Get profile of the calling user |
| `PATCH` | `/user/permission` | ✅ | `PatchUserPermissionRequestDto` | `UserUpdatedResponseDto` | Update user's role or workspace |
| `DELETE` | `/user/permission` | ✅ | `PatchUserPermissionRequestDto` | `UserResponseDto` | Revoke a user's access record |
| `DELETE` | `/user/delete/{id}` | ✅ | `id` (path var) | `{message: "User deleted successfully"}` | Soft-delete user |
| `POST` | `/tenant/users` | ✅ | `UserQuery` | `PageDataWrapper<UserResponseDto>` | Paginated list of users in tenant |
| `POST` | `/workspace/users` | ✅ | `UserQuery` | `PageDataWrapper<UserResponseDto>` | Paginated list of users in workspace |

#### UserRequestDto Schema
```json
{
  "userName": "newuser@corp.com",
  "workspaceId": 5,
  "roleId": 2
}
```

#### UserResponseDto Schema
```json
{
  "userId": 101,
  "userName": "newuser@corp.com",
  "isActive": true,
  "tenantId": 1,
  "accessInfo": [
    {
      "accessId": 15,
      "workspaceId": 5,
      "workspaceName": "Marketing",
      "roleId": 2,
      "roleName": "EDITOR"
    }
  ]
}
```

#### UserQuery Schema
```json
{
  "keyword": "john",
  "page": 0,
  "size": 20
}
```

---

### Workspace / Org Hierarchy Endpoints

| Method | Path | IAM Required | Request | Response | Description |
|--------|------|:------------:|---------|----------|-------------|
| `POST` | `/org` | ✅ | `OrgParentHierarchyRequest` | `OrgParentHierarchyResponse` | Create org/workspace node |
| `GET` | `/org/{id}` | ✅ | — | `OrgParentHierarchyResponse` | Get org node by ID |
| `GET` | `/org` | ✅ | — | `ListResponse<OrgParentHierarchyResponse>` | List all org nodes for tenant |
| `GET` | `/org/hierarchy` | ✅ | — | `OrgTreeResponseDto` | Get full org tree rooted at tenant |
| `PATCH` | `/org/rename` | ✅ | `OrgRenameRequestDto` | `OrgParentHierarchyResponse` | Rename an org node |
| `POST` | `/switch/workspace` | ✅ | `?workspaceId=N` + `Authorization: Bearer` | `JwtResponse{accessToken}` | Re-issue JWT scoped to new workspace |

#### OrgParentHierarchyRequest Schema
```json
{
  "orgName": "West Region",
  "parentId": 3
}
```

#### OrgTreeResponseDto Schema (excerpt)
```json
{
  "id": 1,
  "orgName": "RootTenant",
  "orgLevel": 0,
  "children": [
    {
      "id": 3,
      "orgName": "West Region",
      "orgLevel": 1,
      "children": []
    }
  ]
}
```

---

### Role Management Endpoints

| Method | Path | IAM Required | Request | Response | Description |
|--------|------|:------------:|---------|----------|-------------|
| `POST` | `/role` | ✅ | `RoleRequest{roleName}` | `RoleResponse` | Create a new role |
| `GET` | `/role/{id}` | ✅ | — | `RoleDetailResponseDto` | Get role details with mapped endpoints |
| `GET` | `/role` | ✅ | — | `List<RoleDetailResponseDto>` | List all roles with endpoint details |
| `GET` | `/roles` | ✅ | — | `Map<Long, Map<String, List<String>>>` | Role → resource → subModule mapping |
| `POST` | `/role/endpoints` | ✅ | `RoleEndpointMappingRequestDto` | `RoleEndpointMappingResponseDto` | Map API endpoints to a role |

#### RoleEndpointMappingRequestDto Schema
```json
{
  "roleId": 2,
  "endpoints": [10, 11, 12]
}
```

---

### Resource / Endpoint Management

| Method | Path | IAM Required | Request | Response | Description |
|--------|------|:------------:|---------|----------|-------------|
| `POST` | `/resource` | ✅ | `ResourceRequestDto` | `ResourceResponseDto` | Register a resource (e.g. "Campaign Manager") |
| `POST` | `/resource/endpoint` | ✅ | `EndPointRequestDto` | `ResourceEndpointRespDto` | Register an endpoint under a resource |
| `GET` | `/resource/endpoint/{resourceId}` | ✅ | — | `List<ResourceEndpointResponseDto>` | Get all endpoints for a resource |
| `GET` | `/resource` | ✅ | — | `Map<String,Object>` | Get all active resources with endpoints |

---

### Tenant Config Endpoints

| Method | Path | IAM Required | Request | Response | Description |
|--------|------|:------------:|---------|----------|-------------|
| `GET` | `/tenant/config/{tenantId}` | ❌ | — | `Map<String,Object>` | Get tenant account config (public) |

---

## uclm-contentmgmt

**Base URL:** `/content-manager/api/v1`  
**IAM Headers Required (unless noted):** `x-tenant-id`, `x-workspace-id`, `x-user-id`, `x-user-hierarchy`

### Template Endpoints

| Method | Path | IAM Required | Request | Response | Description |
|--------|------|:------------:|---------|----------|-------------|
| `POST` | `/template` | ✅ | `CreateTemplateDTO` | `TemplateDAO` | Create new template (starts at L1_PENDING) |
| `GET` | `/template/{id}` | ✅ | — | `TemplateDAO` | Get template by ID |
| `GET` | `/templates/l1_pending` | ✅ | `?page=&size=` | `PageResponse<TemplateDAO>` | List all L1_PENDING templates |
| `GET` | `/templates/l2_pending` | ✅ | `?page=&size=` | `PageResponse<TemplateDAO>` | List all L2_PENDING templates |
| `POST` | `/filter/template` | ✅ | `TemplateQuery` | `PageResponse<TemplateDAO>` | Filter/search templates by various criteria |
| `PUT` | `/template/approve/l1/{id}` | ✅ | — | `TemplateDAO` | Approve template at L1 (L1_PENDING → L2_PENDING) |
| `PUT` | `/template/approve/l2/{id}` | ✅ | — | `TemplateDAO` | Approve template at L2 (L2_PENDING → CHANNEL_REG_PENDING / APPROVED) |
| `PUT` | `/template/reject/l1` | ✅ | `RejectionRequest{templateId, rejectionReason}` | `TemplateDAO` | Reject template at L1 → REJECTED |
| `PUT` | `/template/reject/l2` | ✅ | `RejectionRequest{templateId, rejectionReason}` | `TemplateDAO` | Reject template at L2 → REJECTED |
| `PUT` | `/template` | ✅ | `EditTemplateDTO` | `TemplateDAO` | Edit a template (APPROVED → L1_PENDING for re-review) |
| `POST` | `/template/callback/sms` | ❌ | `DLTCallBackResponse` | `DLTCallBackResponse` | Webhook from DLT platform for SMS template registration |

#### CreateTemplateDTO Schema
```json
{
  "templateName": "OTP Notification",
  "channel": "SMS",
  "language": "ENGLISH",
  "templateCategory": "TRANSACTIONAL",
  "channelCategory": "OTP",
  "sender": "AIRTEL",
  "paramDetails": [{"paramName": "otp", "paramType": "DYNAMIC"}],
  "templateData": {
    "message": "Your OTP is {otp}. Valid for 10 minutes.",
    "dltTemplateId": "1234567890"
  }
}
```

#### TemplateQuery Schema
```json
{
  "templateName": "OTP",
  "channel": "SMS",
  "state": "L1_PENDING",
  "pageRequest": {"page": 0, "size": 20}
}
```

---

### Media Endpoints

| Method | Path | IAM Required | Request | Response | Description |
|--------|------|:------------:|---------|----------|-------------|
| `POST` | `/media/upload` | ✅ | `multipart/form-data: file + meta(CreateMediaDTO)` | `MediaDTO` | Upload media file (to GCS/S3) → L1_PENDING |
| `GET` | `/media/{mediaId}` | ✅ | — | `MediaDTO` | Get media asset by ID |
| `POST` | `/media/filter` | ✅ | `MediaQuery` | `PageResponse<MediaDTO>` | Filter/search media assets |
| `PUT` | `/media/approve/l1/{mediaId}` | ✅ | — | `MediaDTO` | Approve media at L1 (L1_PENDING → L2_PENDING) |
| `PUT` | `/media/approve/l2/{mediaId}` | ✅ | — | `MediaDTO` | Approve media at L2 (L2_PENDING → APPROVED) |
| `PUT` | `/media/reject/{mediaId}` | ✅ | `?reason=<string>` | `MediaDTO` | Reject media (→ REJECTED) |
| `PUT` | `/media/delete/{mediaId}` | ✅ | — | `MediaDTO` | Deactivate media (→ INACTIVE) |
| `PUT` | `/media` | ✅ | `UpdateMediaDTO` | `MediaDTO` | Update media metadata |

---

### Bundle Endpoints

| Method | Path | IAM Required | Request | Response | Description |
|--------|------|:------------:|---------|----------|-------------|
| `POST` | `/bundle` | ✅ | `CreateBundleDTO` | `ViewBundleDTO` | Create a template bundle |
| `GET` | `/bundle/{id}` | ✅ | — | `ViewBundleDTO` | Get bundle by ID |
| `POST` | `/bundle/filter` | ✅ | `BundleQuery` | `PageResponse<ViewBundleDTO>` | Filter/search bundles |
| `PATCH` | `/bundle` | ✅ | `UpdateBundleDTO` | `ViewBundleDTO` | Update a bundle |

---

### Asset / Channel Config Endpoints

| Method | Path | IAM Required | Request | Response | Description |
|--------|------|:------------:|---------|----------|-------------|
| `GET` | `/assets` | ✅ | `?channel=SMS` | `ViewConfigDTO` | Get channel-specific asset configuration |
| `GET` | `/assets/tenant` | ✅ | — | `List<ViewConfigDTO>` | Get all channel configurations for tenant |

---

## Common Response Wrapper

All responses are wrapped (by `CustomResponseBodyAdvice`) in:

```json
{
  "data": { ... },
  "errors": null,
  "meta": {
    "traceId": "abc123",
    "requestId": "req-uuid",
    "app": "auth-manager",
    "timestamp": "2024-01-15T10:30:00.000Z"
  }
}
```

And `X-Correlation-ID` header is added to every response.
