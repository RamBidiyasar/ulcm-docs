# User Management — Deployment Guide

> Deployment guide based on application properties, environment variable injection, Spring profiles, and infrastructure dependencies.

---

## Prerequisites

| Dependency | Version | Required By |
|------------|---------|-------------|
| Java | 17 | Both services |
| Spring Boot | 3.5.x | Both services |
| Oracle DB | 12c+ | uclm-auth-manager |
| Aerospike | 7.0.0 | uclm-auth-manager |
| MongoDB | 5.0+ | uclm-contentmgmt |
| Google Cloud Storage | — | uclm-contentmgmt (primary media storage) |
| AWS S3 / MinIO | — | uclm-contentmgmt (alternative media storage) |
| Apache Kafka | 3.x | uclm-contentmgmt (analytics + events) |
| Kubernetes / OCP | 4.x | Both services (production) |

---

## Environment Variables

### uclm-auth-manager

| Variable | Description | Example |
|----------|-------------|---------|
| `PORT` | HTTP listen port | `8080` |
| `ACTIVE_PROFILE` | Spring profile | `aurora`, `oracle`, `dev`, `h2` |
| `LOG_LEVEL` | Root log level | `INFO`, `DEBUG` |

> All DB credentials, SAML keys, Aerospike host, JWT secret, and notification API config are set in the profile-specific properties files (not environment variables, except `PORT`, `ACTIVE_PROFILE`, `LOG_LEVEL`).

**Credential file (Aerospike):** `/opt/getsec_data.txt` — JSON file with `AS_USR_NAME` and `AS_USR_PWD` keys (path configurable via `cred.read.path`).

### uclm-contentmgmt

| Variable | Description | Example |
|----------|-------------|---------|
| `PORT` | HTTP listen port | `8080` |
| `ACTIVE_PROFILE` | Spring profile | `dev`, `uat`, `local` |
| `LOG_LEVEL` | Root log level | `INFO` |
| `CM_SECRET` | JASYPT encryption key for sensitive props | `my-secret-key` |

**GCS Credentials file:** `/opt/gcp_credentials.json` — Google Cloud service account key JSON (path configurable via `uclm.gcs.credentials.path`).

---

## Spring Profiles

### uclm-auth-manager Profiles

| Profile | File | Database |
|---------|------|----------|
| `oracle` | `application-oracle.properties` | Oracle DB |
| `aurora` | `application-aurora.properties` | Amazon Aurora (MySQL-compat) |
| `dev` | `application-dev.properties` | Development Oracle or MySQL |
| `h2` | `application-h2.properties` | H2 in-memory (unit tests / local dev) |

Activate via: `spring.profiles.active=${ACTIVE_PROFILE}` or environment variable.

### uclm-contentmgmt Profiles

| Profile | File | Purpose |
|---------|------|---------|
| `dev` | `application-dev.properties` | Development environment |
| `uat` | `application-uat.properties` | UAT / staging |
| `local` | `application-local.properties` | Local developer setup |

---

## Configuration Reference

### uclm-auth-manager

```properties
# Server
server.port=${PORT}
application.name=auth-manager
auth.api.base.url=/auth-manager/api/v1

# IAM header bypass
ignore.iam.header.apis=\
  /auth-manager/api/v1/ssoLogin,\
  /auth-manager/api/v1/user/saml/response,\
  /auth-manager/api/v1/tenant/config/**,\
  /auth-manager/api/v1/user/saml/logout/response

# JWT
jwt.secret=<base64-hmac-secret>
jwt.token.validity=360000

# Aerospike
auth.aerospike.host=10.5.247.156:3000
auth.aerospike.nameSpace=arch
cred.read.path=/opt/getsec_data.txt
session.inactive.interval=300

# CORS & UI
auth.app.allowed.origin=https://adtechadrendering.wynk.in,...
uclm.ui.url=https://adtechadrendering.wynk.in/login

# Notification Service
notification.api.base-url=https://uclm-test-campaign-...
notification.api.endpoint=/test-campaign/api/v1/campaigns/{campaignId}/test
notification.api.campaign-id=14295
notification.api.x-tenant-id=1
notification.api.x-workspace-id=8
notification.api.x-user-id=authManager@airtel.com
notification.api.x-user-hierarchy=1-3-2

# JPA
spring.jpa.hibernate.ddl-auto=update
spring.jpa.show-sql=true
spring.main.allow-circular-references=true
```

### uclm-contentmgmt

```properties
# Server
server.port=${PORT}
application.name=content-manager
content.api.base.url=/content-manager/api/v1

# IAM header bypass
ignore.iam.header.apis=/content-manager/api/v1/template/callback/sms

# MongoDB
spring.data.mongodb.auto-index-creation=true

# Media Storage
uclm.file.service.provider=GCS
uclm.cdn.url=https://uclm-mcdn-preprod.wynk.in/
uclm.gcs.bucket.name=airtel-uclm-mediacdn
uclm.gcs.credentials.path=/opt/gcp_credentials.json

# S3 (alternative)
s3.bucket=audmgr-exports-test
s3.endpoint.url=http://10.223.74.55:10443
s3.region=us-east-1
s3.access.key=<access-key>
s3.secret.key=<secret-key>

# Multipart
spring.servlet.multipart.max-file-size=101MB
spring.servlet.multipart.max-request-size=111MB

# WhatsApp integration
iq.whatsapp.base.url=https://iqconversation.airtel.in/...
wa.request.basicAuth=<base64-basic-auth>

# Schedulers
approval.poll.cron=0 */15 * * * *
registration.retry.cron=0 */15 * * * *
media.registration.cron=0,30 * * ? * *

# Kafka
spring.kafka.bootstrap-servers=<kafka-brokers>
spring.kafka.producer.acks=all
spring.kafka.producer.properties.enable.idempotence=true
kafka.kerberos.enabled=true
kafka.kerberos.keytab=/opt/clm_api_user_tnd.keytab

# JASYPT
jasypt.encryptor.password=${CM_SECRET}
```

---

## Kubernetes Deployment Configuration

### Service Deployment Pattern

Both services follow the standard OCP/Kubernetes deployment pattern:

```yaml
# Generic pattern — adapt per service
apiVersion: apps/v1
kind: Deployment
metadata:
  name: uclm-auth-manager
spec:
  replicas: 2
  selector:
    matchLabels:
      app: uclm-auth-manager
  template:
    spec:
      containers:
        - name: uclm-auth-manager
          image: <registry>/uclm-auth-manager:<tag>
          ports:
            - containerPort: 8080
          env:
            - name: PORT
              value: "8080"
            - name: ACTIVE_PROFILE
              value: "aurora"
            - name: LOG_LEVEL
              value: "INFO"
          volumeMounts:
            - name: cred-volume
              mountPath: /opt
          livenessProbe:
            httpGet:
              path: /health
              port: 8080
            initialDelaySeconds: 60
            periodSeconds: 30
          readinessProbe:
            httpGet:
              path: /health
              port: 8080
            initialDelaySeconds: 30
            periodSeconds: 10
      volumes:
        - name: cred-volume
          secret:
            secretName: auth-manager-credentials
```

### Health Endpoints

Both services expose actuator health at `/health`:

```properties
management.endpoints.web.base-path=/
management.endpoints.web.path-mapping.health=health
management.endpoint.health.show-details=always
```

Probes: `GET /health` → `{"status": "UP", "components": {...}}`

---

## Startup Checklist

### uclm-auth-manager

- [ ] Oracle DB accessible and schema initialized (`ddl-auto=update` handles this on first start)
- [ ] Aerospike cluster reachable at `auth.aerospike.host`
- [ ] Credential file exists at `cred.read.path` (`/opt/getsec_data.txt`)
- [ ] `jwt.secret` set in properties (long enough for HS256 — 64+ chars Base64)
- [ ] SAML tenant config seeded in `auth_tenant_config` table for each tenant
- [ ] `uclm.ui.url` points to correct frontend URL
- [ ] Notification service reachable (non-critical; async, won't block startup)
- [ ] CORS origins configured in `auth.app.allowed.origin`

### uclm-contentmgmt

- [ ] MongoDB accessible with correct URI and credentials
- [ ] MongoDB indexes created (`spring.data.mongodb.auto-index-creation=true`)
- [ ] GCS credentials file at `uclm.gcs.credentials.path` (`/opt/gcp_credentials.json`)
- [ ] GCS bucket `airtel-uclm-mediacdn` exists and is writable
- [ ] Kafka brokers reachable (if analytics enabled)
- [ ] Kerberos keytab available at `kafka.kerberos.keytab` path (if Kerberos enabled)
- [ ] Channel configurations seeded in `channel_configurations` MongoDB collection per tenant
- [ ] `CM_SECRET` environment variable set for JASYPT decryption
- [ ] WhatsApp IQ API accessible at `iq.whatsapp.base.url` (if WhatsApp templates used)

---

## Inter-Service Communication

```
uclm-auth-manager  ──────────────────────────  No direct HTTP calls to contentmgmt
uclm-contentmgmt   ──────────────────────────  No direct HTTP calls to auth-manager

Both services validate IAM headers independently.
The Frontend UI is responsible for:
  1. Obtaining JWT from auth-manager (via SAML flow)
  2. Passing IAM headers to contentmgmt on every request
```

The two services are **loosely coupled** — they share the IAM header contract but do not call each other directly. Authentication is entirely handled by the frontend passing the JWT-derived headers.

---

## Build & Run Locally

```bash
# uclm-auth-manager (H2 in-memory)
cd user-management/uclm-auth-manager
mvn clean package -DskipTests
java -jar target/auth-manager-*.jar \
  --spring.profiles.active=h2 \
  --server.port=8081 \
  --LOG_LEVEL=DEBUG

# uclm-contentmgmt (local profile)
cd user-management/uclm-contentmgmt
mvn clean package -DskipTests
java -jar target/content-manager-*.jar \
  --spring.profiles.active=local \
  --server.port=8082 \
  --LOG_LEVEL=DEBUG \
  --CM_SECRET=localdev
```

Swagger UI available at:
- Auth Manager: `http://localhost:8081/swagger-ui.html`
- Content Manager: `http://localhost:8082/swagger-ui.html`
