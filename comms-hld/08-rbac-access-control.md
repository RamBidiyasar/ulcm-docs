# 08 — RBAC & Access Control

> Covers how Role-Based Access Control (RBAC) is implemented across **uclm-auth-manager** and all **comms services**.

---

## Table of Contents

1. [Architecture Overview](#architecture-overview)
2. [Auth-Manager — Core RBAC Model](#auth-manager--core-rbac-model)
   - [Entity Descriptions](#entity-descriptions)
   - [Database Schema Relationships](#database-schema-relationships)
   - [Repositories](#repositories)
3. [JWT Token — RBAC Claims](#jwt-token--rbac-claims)
4. [RBAC Enforcement Logic (Auth-Manager)](#rbac-enforcement-logic-auth-manager)
5. [Auth-Manager API Endpoints](#auth-manager-api-endpoints)
6. [Comms Services — Access Control Integration](#comms-services--access-control-integration)
   - [Security Configuration](#security-configuration)
   - [RequestContextInterceptor](#requestcontextinterceptor)
   - [RequestContext ThreadLocal](#requestcontext-threadlocal)
   - [Data Segregation](#data-segregation)
   - [Header Propagation to Downstream Services](#header-propagation-to-downstream-services)
7. [RBAC Attribute Reference](#rbac-attribute-reference)
8. [Access Control Decision Matrix](#access-control-decision-matrix)

---

## Architecture Overview

```
┌──────────────────────────────────────────────────────────────┐
│                     uclm-auth-manager                        │
│                                                              │
│  USER ──→ UserAccess ──→ ROLE ──→ RoleEndPoint ──→ Endpoint  │
│                    └──→ OrgParentHierarchy (Workspace)       │
│                                                              │
│  JWT Issued Claims:                                          │
│  ├─ x-user-id        ├─ x-role-id                           │
│  ├─ x-tenant-id      ├─ x-workspace-id                      │
│  ├─ x-tenant-name    ├─ x-user-hierarchy                    │
│  └─ session_id                                               │
└──────────────────────────────────────────────────────────────┘
                           │
                    JWT verified by
                      API Gateway
                           │
                    Claims → HTTP Headers
                           │
              ┌────────────▼─────────────┐
              │       Comms Services      │
              │                          │
              │  RequestContextInterceptor│
              │  ├─ Validate headers      │
              │  └─ Populate ThreadLocal  │
              │                          │
              │  All DB queries filter by │
              │  tenantId + deptId        │
              └──────────────────────────┘
```

**Key principle:**
- **Role-to-endpoint permission** checks → handled by `uclm-auth-manager` (upstream)
- **Tenant + Workspace data isolation** → handled by each comms service (via headers)

---

## Auth-Manager — Core RBAC Model

### Entity Descriptions

#### `User` — `auth_user_details`

| Field | Type | Description |
|---|---|---|
| `userId` | Long (PK) | Auto-generated sequence |
| `username` | String (unique) | User identifier |
| `password` | String | Encrypted password |
| `tenantId` | Long | Multi-tenant scope |
| `isActive` | Boolean | Active status |
| `isDeleted` | Boolean | Soft delete flag |
| `accessList` | OneToMany | Links to `UserAccess` |
| `createdOn/By`, `updatedOn/By` | Audit fields | |

---

#### `Role` — `auth_role_details`

| Field | Type | Description |
|---|---|---|
| `roleId` | Long (PK) | Auto-generated sequence |
| `name` | String (unique) | Role name (e.g., Admin, Manager, Viewer) |
| `description` | String | Role description |
| `isActive` | Boolean | Role availability flag |

---

#### `UserAccess` — `auth_user_access` ⭐ Core RBAC Bridge

Composite unique constraint: `user_id + node_id`

| Field | Type | Description |
|---|---|---|
| `id` | Long (PK) | Auto-generated sequence |
| `user` | ManyToOne → `User` | Linked user |
| `role` | ManyToOne → `Role` | Assigned role |
| `node` | ManyToOne → `OrgParentHierarchy` | Assigned workspace |
| `tenantId` | Long | Tenant scope |

> This is the **User → Role → Workspace** mapping table.

---

#### `Resource` — `auth_resource_details`

| Field | Type | Description |
|---|---|---|
| `id` | Long (PK) | Auto-generated sequence |
| `name` | String (unique) | Resource name (e.g., "Campaign Management") |
| `resourceKey` | String (unique) | Resource identifier key |
| `endpoints` | OneToMany → `ResourceEndPoint` | Linked endpoints |

---

#### `ResourceEndPoint` — `auth_resource_endpoint`

Composite unique constraint: `resource_id + url + httpMethod`

| Field | Type | Description |
|---|---|---|
| `id` | Long (PK) | Auto-generated sequence |
| `name` | String | Endpoint display name |
| `url` | String | API endpoint URL path |
| `httpMethod` | String | HTTP method (GET, POST, PUT, DELETE, PATCH) |
| `subModule` | String | Sub-module categorization |
| `description` | String | Endpoint description |
| `isActive` | Boolean | Active status |
| `resource` | ManyToOne → `Resource` | Parent resource |
| `createdOn/By` | Audit fields | |

---

#### `RoleEndPoint` — `auth_role_endpoint` ⭐ Permission Mapping

Composite unique constraint: `role_id + endpoint_id`

| Field | Type | Description |
|---|---|---|
| `id` | Long (PK) | Auto-generated sequence |
| `role` | ManyToOne → `Role` | Assigned role |
| `endpoint` | ManyToOne → `ResourceEndPoint` | Permitted endpoint |
| `createdOn/By` | Audit fields | |

> This is the **Role → Endpoint** permission grant table.

---

#### `OrgParentHierarchy` — `auth_org_hierarchy`

| Field | Type | Description |
|---|---|---|
| `id` | Long (PK) | Auto-generated sequence |
| `orgName` | String (unique) | Workspace / Organisation name |
| `parent` | ManyToOne (self-ref) | Parent node for hierarchy |
| `tenantId` | Long | Tenant scope |
| `orgLevel` | Integer | Hierarchy depth level |
| `createdOn/By`, `updatedOn/By` | Audit fields | |

> Enables nested workspace/department hierarchies. Users inherit access based on their assigned node in this tree.

---

### Database Schema Relationships

```
User (1) ──── (M) UserAccess (M) ──── (1) Role
                             (M) ──── (1) OrgParentHierarchy

Role (1) ──── (M) RoleEndPoint (M) ──── (1) ResourceEndPoint
Resource (1) ──── (M) ResourceEndPoint
```

---

### Repositories

| Repository | Key Query Methods |
|---|---|
| `UserAccessRepository` | `findByUser_UsernameAndNode_IdAndTenantId(username, nodeId, tenantId)` |
| | `existsByUser_UserIdAndNode_Id(userId, nodeId)` |
| | `findByIdAndUser_UserIdAndTenantId(accessId, userId, tenantId)` |
| `RoleEndpointRepository` | `findAllByRole_RoleId(roleId)` |
| | `findAllWithRoleAndEndpointAndResource()` |
| `OrgParentHierarchyRepository` | `findByIdAndTenantId(id, tenantId)` |

---

## JWT Token — RBAC Claims

**Generation method:** `JwtUtility.generateToken(User, OrgParentHierarchyResponse, UserAccess, sessionId)`

```json
{
  "sub":               "username",
  "iss":               "<issued_at>",
  "exp":               "<expiration>",

  "x-user-id":         "john.doe",
  "x-usr-id":          "john.doe",
  "x-tenant-id":       "123",
  "x-tenant-name":     "AIRTEL",
  "x-role-id":         "1",
  "x-workspace-id":    "456",
  "x-user-hierarchy":  "1->5->456",
  "session_id":        "uuid"
}
```

**Token constants (`TokenConstants.java`):**

| Constant | Value |
|---|---|
| `USER_ID_KEY` | `x-user-id` |
| `ROLE_ID_KEY` | `x-role-id` |
| `WORKSPACE_ID_KEY` | `x-workspace-id` |
| `TENANT_ID_KEY` | `x-tenant-id` |
| `TENANT_NAME_KEY` | `x-tenant-name` |
| `USER_HIERARCHY_KEY` | `x-user-hierarchy` |
| `HIERARHCY_SEPARATOR` | `->` |

---

## RBAC Enforcement Logic (Auth-Manager)

### `RoleEndpointServiceImpl` — Permission Assignment

```
mapEndpointsToRole(RoleEndpointMappingRequestDto):
  1. Validate role exists
  2. For each endpoint:
     a. Validate endpoint exists
     b. Check for duplicate (existsByRole_RoleIdAndEndpoint_Id)
     c. Create RoleEndPoint mapping record
  3. Return mapped endpoints list
```

### `UserServiceImpl` — User Access Management

```
createUser(UserRequestDto):
  1. Validate username against tenant regex
  2. Check user not already onboarded to workspace
  3. Fetch Role by ID
  4. Fetch OrgParentHierarchy (workspace) by ID
  5. Create UserAccess (user + role + workspace + tenantId)

updateUserPermission(PatchUserPermissionRequestDto):
  1. Fetch UserAccess by ID + userId + tenantId
  2. Prevent user from editing own permissions
  3. Update role OR workspace in UserAccess
  4. Validate no duplicate (same role + workspace)

revokeAccess(PatchUserPermissionRequestDto):
  1. Find UserAccess record
  2. Delete access mapping
  3. Return updated user info
```

---

## Auth-Manager API Endpoints

### Role Management — `RoleController`

| Method | Path | Description |
|---|---|---|
| `POST` | `/auth/role` | Create role |
| `POST` | `/auth/role/endpoints` | Map endpoints to role |
| `GET` | `/auth/role/{id}` | Get role with endpoints |
| `GET` | `/auth/role` | List all roles |
| `GET` | `/auth/roles` | Fetch roles with endpoint mappings |

### User Access Management — `UserController`

| Method | Path | Description |
|---|---|---|
| `POST` | `/auth/user` | Create user & assign workspace + role |
| `PATCH` | `/auth/user/permission` | Update user's role / workspace |
| `DELETE` | `/auth/user/permission` | Revoke user access |
| `GET` | `/auth/user` | Get current user details |
| `POST` | `/auth/tenant/users` | List users by tenant (paginated) |
| `POST` | `/auth/workspace/users` | List users by workspace (paginated) |
| `DELETE` | `/auth/user/delete/{id}` | Delete user |

### Resource / Endpoint Management — `ResourceController`

| Method | Path | Description |
|---|---|---|
| `POST` | `/auth/resource` | Create resource |
| `POST` | `/auth/resource/endpoint` | Create endpoint |
| `GET` | `/auth/resource/endpoint/{resourceId}` | List endpoints by resource |
| `GET` | `/auth/resource` | List all resources with endpoints |

---

## Comms Services — Access Control Integration

### Security Configuration

All comms services share an **identical** `SecurityConfig`:

```java
http.csrf(csrf -> csrf.disable())
    .sessionManagement(s -> s.sessionCreationPolicy(SessionCreationPolicy.STATELESS))
    .authorizeHttpRequests(auth -> auth.anyRequest().permitAll());
```

- CSRF disabled (stateless REST API)
- All requests permitted at Spring Security level
- **No `@PreAuthorize` / `@Secured` / `@RolesAllowed`** annotations anywhere in comms services
- Authorization is fully **delegated upstream** (API Gateway + auth-manager)

---

### RequestContextInterceptor

**File:** `uclm-campaign-manager/RequestContextInterceptor.java`

```
preHandle(request, response, handler):
  1. Validate x-tenant-id   (required, must be numeric > 0)
  2. Validate x-workspace-id (required, must be numeric > 0)
  3. Validate x-user-id     (required, max 100 chars)
  4. Validate x-user-hierarchy (max 200 chars)
  5. Failure → throw AuthorizationException
  6. Populate RequestContext:
     tenantId, deptId, userId, userHierarchy, requestId, correlationId
  7. Store in ThreadLocal via RequestContextHolder.set(ctx)
  8. Populate MDC for logging
```

**`TenantContextFilter`** (used in `uclm-campaign-time-validation`):
- Same header extraction but logs warnings instead of failing for missing headers
- Skips validation for `/actuator`, `/health`, `/swagger`, `/api-docs` paths

---

### RequestContext ThreadLocal

```java
// RequestContext.java
@Data
public class RequestContext {
    private Long   tenantId;       // from x-tenant-id
    private Long   deptId;         // from x-workspace-id
    private String userHierarchy;  // from x-user-hierarchy
    private String userId;         // from x-user-id
    private String requestId;
    private String correlationId;
}

// RequestContextHolder.java
private static final ThreadLocal<RequestContext> CONTEXT = new ThreadLocal<>();
```

---

### Data Segregation

The `CampaignMaster` entity stores context attributes at the record level:

```java
@Column(name = "TENANT_ID")   private Long   tenantId;
@Column(name = "DEPT_ID")     private Long   deptId;
@Column(name = "USER_HIERARCHY", length = 200) private String userHierarchy;
```

Service layer pattern:
```java
RequestContext ctx = RequestContextHolder.get();

campaign.setTenantId(ctx.getTenantId());
campaign.setDeptId(ctx.getDeptId());
campaign.setUserHierarchy(ctx.getUserHierarchy());

// All read queries filter by:
campaignRepository.findBy(ctx.getTenantId(), ctx.getDeptId());
```

---

### Header Propagation to Downstream Services

**Feign Interceptor** (`uclm-campaign-time-validation`):
```java
template.header("x-tenant-id",    TenantContextHolder.getTenantId());
template.header("x-workspace-id", TenantContextHolder.getWorkspaceId());
```

**RestTemplate** (`uclm-campaign-audience-push`):
```java
headers.set("x-tenant-id",    String.valueOf(tenantId));
headers.set("x-workspace-id", String.valueOf(workspaceId));
```

---

## RBAC Attribute Reference

| Attribute | Header / Claim | Enforced By | Purpose |
|---|---|---|---|
| Role ID | `x-role-id` | Auth-Manager | Determines permitted endpoints |
| Workspace | `x-workspace-id` | Auth-Manager + Comms | Workspace scoping |
| Tenant | `x-tenant-id` | Auth-Manager + Comms | Multi-tenant data isolation |
| Org hierarchy | `x-user-hierarchy` | Auth-Manager + Comms | Hierarchical org access path (`1->5->456`) |
| User ID | `x-user-id` | Comms | Audit trail |
| Endpoint URL + Method | DB only | Auth-Manager (`RoleEndPoint`) | Per-endpoint permission grants |
| Org level + parent | DB only | Auth-Manager (`OrgParentHierarchy`) | Nested workspace hierarchy |

---

## Access Control Decision Matrix

| Layer | What Is Checked | How |
|---|---|---|
| **Auth-Manager** | User → Role existence | `UserAccessRepository` DB query |
| **Auth-Manager** | Role → Endpoint permissions | `RoleEndpointRepository` DB query |
| **Auth-Manager** | Workspace hierarchy | `OrgParentHierarchy` self-ref tree |
| **Auth-Manager** | Token generation | `JwtUtility.generateToken()` |
| **API Gateway** *(assumed)* | JWT signature + role-endpoint matching | JWT verification + claim routing |
| **Comms Services** | Header presence & format | `RequestContextInterceptor` |
| **Comms Services** | Tenant data isolation | Auto-filter by `tenantId` in all queries |
| **Comms Services** | Workspace data isolation | Auto-filter by `deptId` in all queries |
| **Comms Services** | Role-level access | ❌ Not checked — delegated upstream |
