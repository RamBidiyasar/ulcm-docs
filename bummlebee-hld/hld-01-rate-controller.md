# HLD — uclm-rate-controller-service

**Role:** Per-team, per-channel TPS throttle gate between upstream UCLM output and the validation pipeline.

---

## 1. Purpose & Responsibilities

| Responsibility | Detail |
|---------------|--------|
| **Rate Limiting** | Enforces per-team, per-channel TPS (Transactions Per Second) using Resilience4j RateLimiter |
| **Capacity Guard** | On startup and every 10 s, checks team status in DB; only starts/resumes consuming if team is `APPROVED` and TPS fits within global + tenant budget |
| **Backpressure** | `checkDownstreamHealthAndControlConsumption()` can pause Kafka if downstream lag exceeds threshold (wired but currently not called in the periodic check — available for manual activation) |
| **TPS Auto-Scaling** | Every 10 s, `CapacityIncreaseScheduler` applies approved TPS increase requests sequentially from DB |
| **TPS Refresh** | Every 10 s, `RefreshTpsScheduler` syncs team and tenant TPS from PostgreSQL into Hazelcast cache |
| **Health Exposure** | `DownstreamKafkaHealthIndicator` exposes downstream lag status via Spring Boot Actuator `/actuator/health` |

---

## 2. High-Level Architecture

```mermaid
flowchart TB
    %%  External systems 
    KIN([Kafka: channel-partner-rate-controller-input\nUpstream comms-input events])
    KOUT([Kafka: event topic\nDownstream to Val-Gov])
    PG[(PostgreSQL\nteam_onboarding\ntenant_details)]

    %%  Schedulers 
    subgraph SCHED["Schedulers (every 10 s)"]
        direction LR
        RTS["RefreshTpsScheduler\nReads DB  pushes to Hazelcast\nRecreates RateLimiter on team TPS change"]
        CIS["CapacityIncreaseScheduler\n1. availableTps = GLOBAL - allocated\n2. Fetch INCREASING_APPROVED teams\n3. Apply increase if TPS fits\n4. Reject if no TPS available"]
    end

    %%  Capacity Guard 
    subgraph KCC["KafkaConsumptionController (@Scheduled every 10 s)"]
        direction TB
        EVA["evaluateCapacityAndAct()\ngetTeamStatus(channel, teamId)"]
        ACTIVE["ACTIVE  no-op\n(consumer already running)"]
        APPROVED["APPROVED  isCapacityAvailable?"]
        PENDING["PENDING  container.pause()"]
        YES["YES  container.start() / resume()\nmarkTenantActive(teamId, channel)"]
        NO["NO  container.pause()"]

        EVA --> ACTIVE
        EVA --> APPROVED
        EVA --> PENDING
        APPROVED -->|capacity ok| YES
        APPROVED -->|over limit| NO
    end

    %%  Kafka Listener 
    subgraph KEL["KafkaEventListener  @KafkaListener"]
        direction TB
        LISTEN["listen(EventRequestDTO, Acknowledgment)\ntpsCache.getTeamTps(teamId) — Hazelcast IMap\nchannelThrottleProcessor.tryAcquire(teamId, tps, uuid)"]
        GRANT["GRANTED\nrequestDispatcher.dispatch(event)\nkafkaTemplate.send — event topic\nack.acknowledge()"]
        DENY["DENIED\nack.nack(500 ms)\nKafka redelivers message"]

        LISTEN -->|permit acquired| GRANT
        LISTEN -->|rate limit hit| DENY
    end

    %%  In-process components 
    subgraph HZ["Hazelcast — embedded, single-node\ncluster: rate-controller-cluster"]
        direction LR
        TC["tenant-tps-cache\ntenantId  tenantChannelTps"]
        TMC["team-tps-cache\nteamId  capacityAllocated"]
    end

    RL["TeamRateLimiterManager\nRateLimiter per team\nname: channel-teamId\nlimitForPeriod = teamTps\nlimitRefreshPeriod = 1 s\ntimeoutDuration = 0 non-blocking"]

    subgraph LAG["KafkaConsumerLagMonitor"]
        direction TB
        MON["AdminClient\nlistConsumerGroupOffsets + listOffsets\nlag = latest - committed per partition\ntotalLag = sum of all partitions\nhealthy = totalLag <= 10 000"]
        HEALTH["DownstreamKafkaHealthIndicator\nGET /actuator/health\nvalgov: HEALTHY or LAGGING\norchestrator: HEALTHY or LAGGING"]
        MON --> HEALTH
    end

    %%  Data flow 
    KIN -->|consume events| KEL
    KCC -->|pause / resume container| KEL
    SCHED -->|refresh TPS values| HZ
    SCHED -->|read DB| PG
    HZ -->|getTeamTps| KEL
    HZ -->|getCapacity| KCC
    RL -->|tryAcquire| KEL
    GRANT -->|produce| KOUT
    KCC -->|read team status| PG
    LAG -->|monitor lag| KIN
    classDef kafka fill:#f39c12,color:#000,stroke:#d68910
    classDef svc fill:#2980b9,color:#fff,stroke:#1f618d
    classDef db fill:#27ae60,color:#fff,stroke:#1e8449
    classDef channel fill:#d35400,color:#fff,stroke:#a04000
    classDef user fill:#34495e,color:#fff,stroke:#2c3e50
    classDef stop fill:#e74c3c,color:#fff,stroke:#c0392b
    class EVA,KIN,YES channel
    class LISTEN,PG,RTS,TC,TMC db
    class DENY,GRANT,HEALTH,KOUT kafka
    class CIS stop
    class ACTIVE svc
    class MON user
```

---

## 3. Detailed Processing Flow

```mermaid
sequenceDiagram
    participant UP as Upstream (comms-input)
    participant KC as KafkaConsumptionController
    participant CS as ChannelCapacityService
    participant EL as KafkaEventListener
    participant TP as ChannelThrottleProcessor
    participant RL as TeamRateLimiterManager
    participant HZ as Hazelcast TpsCache
    participant RD as RequestDispatcher
    participant EV as Kafka (event topic)
    participant PG as PostgreSQL

    Note over KC: ApplicationReadyEvent + every 10 s
    KC->>PG: getTeamStatus(channel, teamId)
    PG-->>KC: ACTIVE / APPROVED / PENDING
    alt status = ACTIVE
        KC->>KC: skip (already running)
    else status = APPROVED
        KC->>CS: isCapacityAvailable(tenantId, teamId)
        CS->>PG: getTotalCapacityByChannel(channel)
        CS->>PG: getTotalCapacityByTenantAndChannel(tenantId, channel)
        CS->>PG: isTeamApprovedForTenant(tenantId, teamId)
        CS->>HZ: getTeamTps(teamId) + getTenantTps(tenantId)
        alt capacity fits
            KC->>EL: container.start() or resume()
            KC->>PG: activateTeamForChannel(teamId, channel)
        else no capacity
            KC->>EL: container.pause()
        end
    else status = PENDING
        KC->>EL: container.pause()
    end

    UP->>EL: EventRequestDTO (from comms-input topic)
    EL->>HZ: getTeamTps(teamId)
    HZ-->>EL: teamTps
    EL->>TP: tryAcquire(teamId, teamTps, uuid)
    TP->>RL: getOrCreateLimiter(teamId, teamTps)
    RL-->>TP: RateLimiter "channel-teamId"
    TP->>TP: rateLimiter.acquirePermission()
    alt Permit GRANTED
        TP-->>EL: true
        EL->>RD: dispatch(event)
        RD->>EV: kafkaTemplate.send("event", event)
        EL->>UP: ack.acknowledge()
    else Permit DENIED
        TP-->>EL: false
        EL->>UP: ack.nack(500ms)
        Note over UP: Kafka redelivers after 500 ms
    end
```

---

## 4. Capacity Guard Logic

`evaluateCapacityAndAct()` runs on startup (`ApplicationReadyEvent`) and every 10 s:

```
getTeamStatus(channel, teamId)
│
├── ACTIVE  → no-op (consumer already running)
│
├── APPROVED → isCapacityAvailable(tenantId, teamId)?
│               │
│               ├── Check 1: SUM(capacity_allocated) for channel (all ACTIVE teams)
│               │            + this team's TPS  <=  CHANNEL_GLOBAL_TPS
│               │
│               ├── Check 2: SUM(capacity_allocated) for tenant+channel (ACTIVE teams)
│               │            + this team's TPS  <=  tenant_channel_tps (from tenant_details)
│               │
│               ├── Check 3: isTeamApprovedForTenant(tenantId, teamId) == true
│               │
│               ├── ALL PASS → container.start() or resume()
│               │              activateTeamForChannel(teamId, channel)  [status → ACTIVE]
│               └── ANY FAIL → container.pause()
│
└── PENDING → container.pause()
```

---

## 5. TPS Refresh (every 10 s)

`RefreshTpsSchedulerImpl` syncs from PostgreSQL into Hazelcast. Runs for the single configured `team.id` / `tenant.id` of this instance:

```
refreshTenantTps():
  SELECT tenant_id, tenant_channel_tps FROM tenant_details WHERE tenant_id=? AND status='ACTIVE'
  if DB value differs from Hazelcast → hazelcast.updateTenantTps(tenantId, newTps)

refreshTeamTps():
  SELECT workspace_id, capacity_allocated FROM team_onboarding WHERE workspace_id=? AND tenant_id=? AND status='ACTIVE'
  if DB value differs from Hazelcast →
    recreate RateLimiter via TeamRateLimiterManager (new limitForPeriod)
    hazelcast.updateTeamTps(teamId, newTps)
```

---

## 6. Capacity Increase Scheduler (every 10 s)

`CapacityIncreaseSchedulerImpl.processApprovedCapacityIncreases()`:

```
1. globalAllocated = SUM(capacity_allocated) WHERE channel=? AND status='ACTIVE'
   availableTps = CHANNEL_GLOBAL_TPS − globalAllocated

2. if availableTps <= 0:
     rejectCapacityIncreaseRequest(teamId, channel)  [capacity_change_state → REJECTED]
     return

3. candidates = SELECT workspace_id, capacity_allocated, requested_capacity
                FROM team_onboarding
                WHERE status='ACTIVE'
                  AND requested_capacity > 0
                  AND capacity_change_state IN ('INCREASING_APPROVED', 'REJECTED')
                  AND channel=?
                ORDER BY updated_at ASC
                FOR UPDATE

4. for each candidate (sequential, order matters):
     if candidate.requestedCapacity <= availableTps:
       capacity_allocated += requested_capacity
       requested_capacity = 0
       capacity_change_state = 'NONE'
       availableTps -= requested_capacity
     else:
       rejectCapacityIncreaseRequest(teamId, channel)
       break
```

---

## 7. Downstream Lag Monitoring

```
KafkaConsumerLagMonitor.fetchLag(groupId):
  1. AdminClient.listConsumerGroupOffsets(groupId)  → committed offsets per partition
  2. AdminClient.listOffsets(OffsetSpec.latest())   → latest offset per partition
  3. lag(partition) = max(latest − committed, 0)
  4. totalLag = sum(all partitions)
  5. healthy = totalLag <= maxAllowedLag (10 000)

Groups monitored:
  - valgov-consumer-group   (Validation Governance)
  - orch-consumer-group     (Orchestrator)

Exposed via Actuator:
  GET /actuator/health →
    { "valgov": "HEALTHY|LAGGING", "orchestrator": "HEALTHY|LAGGING" }

Note: checkDownstreamHealthAndControlConsumption() (pause/resume based on lag)
      exists but is currently commented out of periodicCheck(). It can be
      re-enabled by uncommenting one line in KafkaConsumptionController.
```

---

## 8. Data Models

### EventRequestDTO (consumed from input topic)

| Field | Type | Description |
|-------|------|-------------|
| `uuid` | String | Unique message ID |
| `moc` | String | Mode of Communication: SMS / EMAIL / WHATSAPP / PUSH / RCS |
| `si_id` | String | Subscriber identity (mobile number) |
| `email_id` | String | Email address (for EMAIL channel) |
| `campaign_group` | String | Campaign group identifier |
| `campaign_name` | String | Campaign name |
| `campaign_type` | String | Campaign type |
| `category` | String | PROMOTIONAL / TRANSACTIONAL / SERVICE_IMPLICIT / SERVICE_EXPLICIT etc. |
| `event_type` | String | NRT / SCHEDULE / ONETIME / EVENT / RECURRING |
| `tenant_id` | String | Tenant identifier |
| `workspace_id` | String | Team (workspace) identifier |
| `script_body` | String | Message content |
| `script_sub` | String | Message subject (for email) |
| `dlt_id` | String | DLT template ID (SMS compliance) |
| `expire_timestamp` | String | Message expiry (format: `yyyy-MM-dd HH:mm:ss.SSSSSS`) |
| `source_timestamp` | String | When the event was created upstream |
| `dynmc_prm` | List | Dynamic parameters for template personalisation |
| `dynmc_prm_sub` | List | Dynamic parameters for subject line |
| `additional_fields` | List | Extra key-value pairs |
| `mediaAttachment` | List | Media attachments (URL, MIME type, file name) |
| `cmsRequired` | boolean | Whether CMS quota decrement is needed |
| `frequency_capping` | String | Frequency capping config |
| `lob` | String | Line of business |
| `circle_id` | String | Telecom circle identifier |

### team_onboarding (PostgreSQL)

| Column | Type | Description |
|--------|------|-------------|
| `workspace_id` | BIGINT PK | Team identifier (used as `teamId` throughout) |
| `tenant_id` | BIGINT FK | Parent tenant → `tenant_details.tenant_id` |
| `team_name` | VARCHAR | Human-readable team name |
| `channel` | VARCHAR | WHATSAPP / SMS / RCS / EMAIL / PUSH |
| `capacity_allocated` | INT | Currently allocated TPS for this team |
| `requested_capacity` | INT | TPS increase requested (pending approval) |
| `capacity_change_state` | VARCHAR | `NONE` / `INCREASING_APPROVED` / `REJECTED` |
| `status` | VARCHAR | `ACTIVE` / `APPROVED` / `PENDING` |
| `environment` | VARCHAR | PROD / UAT |
| `priority` | INT | Allocation priority for capacity increase ordering |
| `provider` | VARCHAR | Channel provider name |
| `created_at` / `updated_at` | TIMESTAMP | Audit timestamps |

### tenant_details (PostgreSQL)

| Column | Type | Description |
|--------|------|-------------|
| `tenant_id` | BIGINT PK | Tenant identifier |
| `tenant_name` | VARCHAR | Tenant name |
| `status` | VARCHAR | `ACTIVE` / `SUSPENDED` |
| `tenant_channel_tps` | BIGINT | Max total TPS allowed for this tenant across all teams on the channel |

---

## 9. Component Map

| Class | Responsibility |
|-------|---------------|
| `KafkaEventListener` | Kafka consumer entry point; calls throttle processor; acks or nacks |
| `ChannelThrottleProcessor` | Acquires Resilience4j RateLimiter permit for the team |
| `TeamRateLimiterManager` | Creates/updates per-team RateLimiter (`{channel}-{teamId}`) |
| `TpsCacheImpl` | Hazelcast `IMap` wrapper — `tenant-tps-cache` and `team-tps-cache` |
| `HazelCastConfig` | Configures embedded Hazelcast cluster (`rate-controller-cluster`), clustering disabled |
| `KafkaConsumptionController` | Startup + periodic (10 s) capacity evaluation; pauses/resumes consumer |
| `ChannelCapacityServiceImpl` | Three-condition capacity check against PostgreSQL + Hazelcast |
| `KafkaConsumerLagMonitor` | Reads consumer group lag via Kafka `AdminClient` |
| `DownstreamKafkaHealthIndicator` | Spring Boot Actuator `HealthIndicator` for downstream lag |
| `RequestDispatcherImpl` | Publishes `EventRequestDTO` to `event` topic via `KafkaTemplate` |
| `RefreshTpsSchedulerImpl` | Every 10 s: syncs tenant + team TPS from DB into Hazelcast |
| `CapacityIncreaseSchedulerImpl` | Every 10 s: applies approved TPS increase requests from DB |
| `TeamOnboardingRepository` | All queries against `team_onboarding` table |
| `TenantOnboardingRepository` | Queries against `tenant_details` table |
| `RateLimiterConfig` | Registers Resilience4j `RateLimiterRegistry` |
| `KafkaConsumerConfiguration` | Kafka consumer factory, deserialiser, listener container factory |
| `KafkaProducerConfiguration` | Kafka producer factory and `KafkaTemplate` |
| `KafkaSecurityConfig` | Kerberos JAAS configuration for UAT/Prod |
| `EventRequestDTODeserializer` | Custom Kafka deserialiser for `EventRequestDTO` |

---

## 10. Configuration Reference

| Property | Default (local) | Dev value | Description |
|----------|----------------|-----------|-------------|
| `server.port` | `8081` | `8081` | HTTP port |
| `kafka.topic` | `comms-input` | `channel-partner-rate-controller-input` | Input topic |
| `kafka.dispatch.topic` | `event` | `event` | Output topic (→ Validation Governance) |
| `kafka.consumer.group-id` | `channel-tenant-throttle-listener` | `channel-partner-rate-controller-svc` | Kafka consumer group ID (also used as listener ID) |
| `kafka.topic.exception` | `exceptions` | `exceptions` | Exception topic |
| `channel.name` | `WHATSAPP` | `WHATSAPP` | Channel this instance handles |
| `channel.global.tps` | `20000` | `2000` | Global max TPS for the channel |
| `tenant.id` | — | — | Tenant ID this instance is configured for |
| `team.id` | — | — | Team (workspace) ID this instance is configured for |
| `downstream.valgov.consumer-group` | `valgov-consumer-group` | `valgov-consumer-group` | Lag-check group for Validation Governance |
| `downstream.orch.consumer-group` | `orch-consumer-group` | `orch-consumer-group` | Lag-check group for Orchestrator |
| `downstream.max.allowed.lag` | `10000` | `10000` | Max consumer lag before pausing |
| `spring.kafka.bootstrap-servers` | `localhost:9092` | `10.92.36.44:9092,...` | Kafka brokers |
