# Deployment Guide

Build, containerize, and deploy all 6 Bummlebee services.

---

## Prerequisites

| Tool | Version | Purpose |
|------|---------|---------|
| Java | 17 (DLR services) / 21 (VG, Orchestrator) | Build + runtime |
| Maven | 3.9+ | Build tool |
| Docker / Podman | Latest | Container builds |
| Kubernetes (OCP) | 1.25+ | Orchestration |
| Helm | 3.x | K8s package management |
| Kafka | 3.x | Message broker |
| Aerospike | 7.x | Cache (DLR services) |
| PostgreSQL | 14+ | Rate Controller DB |

---

## 1. uclm-rate-controller-service

### Build

```bash
git clone https://code.airtelworld.in:7990/bitbucket/scm/uclm/uclm-rate-controller-service.git
cd uclm-rate-controller-service
mvn clean install
# JAR output: target/rate-controller-service-1.0.0.jar
```

### Docker

```bash
docker build --platform=linux/amd64 -t rate-controller-service:latest .
```

### Environment Variables (Key)

| Env Var | Property | Description |
|---------|----------|-------------|
| `SPRING_DATASOURCE_URL` | `spring.datasource.url` | PostgreSQL JDBC URL |
| `SPRING_DATASOURCE_USERNAME` | `spring.datasource.username` | DB username |
| `SPRING_DATASOURCE_PASSWORD` | `spring.datasource.password` | DB password |
| `KAFKA_BOOTSTRAP_SERVERS` | `kafka.bootstrap.servers` | Kafka brokers |
| `KAFKA_TOPIC` | `kafka.topic` | Input topic (`comms-input`) |
| `KAFKA_DISPATCH_TOPIC` | `kafka.dispatch.topic` | Output topic (`event`) |
| `CHANNEL_GLOBAL_TPS` | `channel.global.tps` | Global TPS limit |
| `CHANNEL` | `channel` | Channel type |

### Run Local

```bash
java -jar target/rate-controller-service-1.0.0.jar --spring.profiles.active=local
```

---

## 2. uclm-validation-governance-service

### Build

```bash
git clone https://code.airtelworld.in:7990/bitbucket/scm/uclm/uclm-validation-governance-service.git
cd uclm-validation-governance-service
mvn clean install
# JAR output: target/validation-governance-service-0.0.1.jar
```

### Docker

```bash
podman build --platform=linux/amd64 -t validation-governance-service:latest . --tls-verify=false
# Pull from registry:
podman pull quay-registry-td.india.airtel.itm/clm/channel-manager/validation-governance-service:0.0.1.jar-develop
```

### Lookup Files Setup

```bash
# Create lookup directories
mkdir -p ./lookups/raw ./lookups/schema ./lookups/normalized

# Mount or copy lookup files
cp /path/to/lookup/files/* ./lookups/raw/
```

### Key Environment Variables

| Env Var | Property | Description |
|---------|----------|-------------|
| `CLM_KAFKA_CHANNEL_PARTNER_KAFKA_BOOTSTRAP_SERVERS` | `kafka.bootstrap.servers` | Kafka brokers |
| `CLM_KAFKA_CHANNEL_PARTNER_INPUT_KAFKA_TOPIC` | `input.kafka.topic` | Input topic (`event`) |
| `CLM_KAFKA_CHANNEL_PARTNER_KAFKA_DISPATCH_TOPIC` | `kafka.dispatch.topic` | Output topic (`dispatch`) |
| `CLM_CHANNEL_PARTNER_DLT_URL` | `dlt.url` | DLT scrubbing API endpoint |
| `CLM_CHANNEL_PARTNER_LOOKUP_RAW_FOLDER` | `lookup.raw.folder` | Path to raw lookup files |
| `CLM_CHANNEL_PARTNER_LOOKUP_SCHEMA_FOLDER` | `lookup.schema.folder` | Path to schema files |
| `CLM_CHANNEL_PARTNER_LOOKUP_NORMALIZED_FOLDER` | `lookup.normalized.folder` | Path to normalized files |

---

## 3. uclm-orchestrator-service

### Build

```bash
git clone https://code.airtelworld.in:7990/bitbucket/scm/uclm/uclm-orchestrator-service.git
cd uclm-orchestrator-service
mvn clean install
# JAR output: target/orchestrator-service-1.0.0.jar
```

### Docker

```bash
podman build --platform=linux/amd64 -t orchestrator-service:latest . --tls-verify=false
# Pull from registry:
podman pull quay-registry-td.india.airtel.itm/clm/channel-manager/orchestrator-service:0.0.1.jar-develop
```

### Key Environment Variables

| Env Var | Property | Description |
|---------|----------|-------------|
| `CLM_KAFKA_CHANNEL_PARTNER_KAFKA_BOOTSTRAP_SERVERS` | `kafka.bootstrap.servers` | Kafka brokers |
| `CLM_CHANNEL_PARTNER_APP_CHANNELS_SMS_ENABLED` | `app.channels.sms-enabled` | Enable SMS |
| `CLM_CHANNEL_PARTNER_APP_CHANNELS_EMAIL_ENABLED` | `app.channels.email-enabled` | Enable Email |
| `CLM_CHANNEL_PARTNER_APP_CHANNELS_WHATSAPP_ENABLED` | `app.channels.whatsapp-enabled` | Enable WA |
| `CLM_CHANNEL_PARTNER_APP_CHANNELS_SMS_URL` | `app.channels.sms.url` | SMS provider URL |
| `CLM_CHANNEL_PARTNER_APP_CHANNELS_EMAIL_URL` | `app.channels.email.url` | Email provider URL |
| `CLM_CHANNEL_PARTNER_APP_CHANNELS_WHATSAPP_URL` | `app.channels.whatsapp.url` | WA provider URL |

---

## 4. uclm-dlr-api-service

### Build

```bash
git clone https://code.airtelworld.in:7990/bitbucket/projects/UCLM/repos/channelmanager/browse/iq-generic-dlr
cd dlr-api-service
mvn clean package
# JAR output: target/dlr-api-service-1.0.0.jar
```

### Docker

```bash
docker build -t dlr-api-service:latest .
```

### Run Local

```bash
java -jar target/dlr-api-service-1.0.0.jar
```

### Run UAT/Prod (Kerberos)

```bash
# Mount keytab
docker run -v /path/to/keytabs:/tmp/keytabs \
  -e SPRING_PROFILES_ACTIVE=uat \
  dlr-api-service:latest
```

### Key Environment Variables

| Env Var | Property | Description |
|---------|----------|-------------|
| `KAFKA_BOOTSTRAP_SERVERS` | `kafka.bootstrap.servers` | Kafka brokers |
| `KAFKA_TOPIC` | `kafka.topic` | Output topic (`iq_channel_dlr_raw`) |
| `KAFKA_PRINCIPAL_USER` | `kafka.principal.user` | Kerberos principal |
| `KEY_TAB_VALUE` | `key.tab.value` | Path to keytab file |

---

## 5. uclm-dlr-aerospike-cache-loader

### Build

```bash
mvn clean package
# JAR: target/aerospike-loader-service-1.0.0.jar
```

### Create Kafka Topics

```bash
kafka-topics --create --topic wa_main_service \
  --bootstrap-server localhost:9092 --partitions 3

kafka-topics --create --topic wa_main_service_dlq \
  --bootstrap-server localhost:9092 --partitions 3
```

### Docker

```bash
docker build -t aerospike-loader-service:latest .
```

### Helm Deployment

```bash
cd helm/
helm upgrade --install aerospike-loader-service . \
  --set kafka.bootstrapServers="10.92.36.44:9092" \
  --set aerospike.host="aerospike-cluster" \
  --set aerospike.port="3000"
```

---

## 6. uclm-dlr-enricher

### Build

```bash
mvn clean package
# JAR: target/dlr-enricher-service-1.0.0.jar
```

### Docker

```bash
docker build -t dlr-enricher-service:latest .
```

### Helm Deployment

```bash
cd helm/
helm upgrade --install dlr-enricher-service . \
  --set kafka.bootstrapServers="10.92.36.44:9092" \
  --set aerospike.host="aerospike-cluster" \
  --set retry.initialDelay="15" \
  --set retry.maxAttempts="3"
```

---

## Kubernetes Manifests

All services include a `deployment.yaml` in their root directory. Deploy with:

```bash
kubectl apply -f deployment.yaml -n <namespace>
```

### Deployment Order (recommended)

```
1. uclm-dlr-aerospike-cache-loader  (Aerospike must be available)
2. uclm-dlr-enricher                (Aerospike must be available)
3. uclm-dlr-api-service             (Kafka must be available)
4. uclm-rate-controller-service     (PostgreSQL + Kafka must be available)
5. uclm-validation-governance-service (Kafka + lookup files must be mounted)
6. uclm-orchestrator-service        (Kafka + all channel provider URLs must be set)
```

---

## Kafka Topic Setup

```bash
# Delivery pipeline topics
kafka-topics --create --topic comms-input --partitions 6 --replication-factor 3
kafka-topics --create --topic event --partitions 6 --replication-factor 3
kafka-topics --create --topic dispatch --partitions 6 --replication-factor 3
kafka-topics --create --topic cs_raw_reporting_topic --partitions 3 --replication-factor 3

# Response / error topics
kafka-topics --create --topic dispatch-response --partitions 3 --replication-factor 3
kafka-topics --create --topic orchestrator-exceptions --partitions 3 --replication-factor 3
kafka-topics --create --topic exceptions --partitions 3 --replication-factor 3

# DLR topics
kafka-topics --create --topic iq_channel_dlr_raw --partitions 6 --replication-factor 3
kafka-topics --create --topic wa_main_service --partitions 6 --replication-factor 3
kafka-topics --create --topic enriched-dlr-topic --partitions 6 --replication-factor 3

# DLQ topics
kafka-topics --create --topic wa_main_service_dlq --partitions 3 --replication-factor 3
kafka-topics --create --topic dlr-enricher-dlq --partitions 3 --replication-factor 3
```
