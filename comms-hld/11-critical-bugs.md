# 11 — Critical Bug Report: UCLM Comms-Planner Campaign System

> **Analysis date:** 2026-05-06  
> **Scope:** Campaign lifecycle services only (audience-push, processor, campaign-manager, time-validation, data-file-download)  
> **Method:** Deep static code analysis across all service repos  
> **Severity legend:** 🔴 Critical &nbsp;|&nbsp; 🟠 High &nbsp;|&nbsp; 🟡 Medium

---

## Table of Contents

1. [Top 5 Most Critical (Cross-Cutting)](#top-5-most-critical)
2. [Campaign Audience Push](#service-1-uclm-campaign-audience-push)
3. [Campaign Processor](#service-2-uclm-campaign-processor)
4. [Campaign Manager](#service-3-uclm-campaign-manager)
5. [Campaign Time Validation](#service-4-uclm-campaign-time-validation)
6. [Campaign Data File Download](#service-5-uclm-campaign-data-file-download)
7. [Summary Table](#summary-table)

---

## Top 5 Most Critical

These bugs have the highest blast radius and should be addressed first.

| Priority | ID | Service | Bug | Why it ranks #1–5 |
|----------|----|---------|-----|-------------------|
| 🥇 1 | M2 | campaign-manager | Kafka event published **before** DB transaction completes in `pause()`/`kill()` | Permanent DB ↔ external-system desync; no automatic recovery possible |
| 🥈 2 | P4 | campaign-processor | Enum name mismatch — processor filters on `AUDIENCE_PUSHED`; Campaign Manager writes `AUDIENCE_REQUESTED` | Could cause the processor to **never pick up any campaign** from the queue |
| 🥉 3 | T4 | time-validation | `CampaignExecutionRequest` built from **only the first child detail** UUID | All other children of a multi-child campaign silently orphaned in `PROCESSING_START` |
| 4 | D2 | data-file-download | `transactionId` is not unique per campaign — bulk UPDATE hits **unrelated campaigns** | Wrong campaign state advanced; correct one stays stuck |
| 5 | A4 / M1 | audience-push + campaign-manager | **No concurrency guard** on either Airflow-triggered endpoint | Duplicate messages sent to users on every Airflow schedule overlap |

---

## Service 1: `uclm-campaign-audience-push`

### 🔴 A1 — 23:30 Hard Cutoff: Campaigns Scheduled After 23:30 Never Trigger

**File:** `CampaignTriggerServiceImpl.java` lines 62–68  

```java
ZonedDateTime endOfDayCutoff = todayInZone.atTime(23, 30).atZone(zoneId);

Instant currentTime = nowInZone.isAfter(endOfDayCutoff)
        ? endOfDayCutoff.toInstant()   // ← clamped to 23:30
        : nowInZone.toInstant();

// Query: cd.actualScheduleTs <= :currentTime
```

**Impact:** Any campaign scheduled between 23:30 and midnight is permanently missed for that day. Next day it is outside its schedule window — silently dropped forever.

---

### 🔴 A2 — Race Condition: `AUDIENCE_REQUESTED` Stuck Permanently

**File:** `AudiencePushServiceImpl.java` lines 85–114  

**Sequence:**
1. Audience Manager push call → **succeeds** (HTTP 200, transactionId returned)
2. `saveAll()` to write `AUDIENCE_REQUESTED` + transactionId → **DB write fails**
3. No retry path exists for a campaign that is still in `AUDIENCE_PENDING` with an unknown external transactionId

**Impact:** Campaign frozen in `AUDIENCE_PENDING` forever. The AM side has an audience already allocated; the campaign side never knows.

---

### 🔴 A3 — No `@Transactional` on Trigger Loop

**File:** `CampaignTriggerServiceImpl.java` lines 100–127  

The outer loop that iterates over eligible campaigns has no transaction boundary. A failure mid-batch commits earlier updates and leaves later ones undone, producing a partially-processed batch with no indication of which campaigns were affected.

**Impact:** Inconsistent DB state after any mid-loop exception.

---

### 🔴 A4 — No Concurrency Guard: Duplicate Audience Manager Push

**File:** `CampaignTriggerServiceImpl.java`  

No distributed lock or idempotency check on `triggerEligibleCampaigns()`. If two Airflow runs overlap (network retry, DAG re-trigger) both fetch the **same eligible campaigns** and push each one twice to Audience Manager.

**Impact:** Users receive duplicate communications. AM may allocate two separate audiences for the same campaign.

---

### 🔴 A5 — Synthetic transactionId Masks AM Failures

**File:** `AudienceManagementClientImpl.java`  

When Audience Manager returns HTTP 200 with an **empty body**, the client fabricates a synthetic transactionId instead of treating the response as a failure. The campaign advances to `AUDIENCE_REQUESTED` on a fake ID — no real audience was allocated.

**Impact:** Campaign appears healthy in the DB but has no actual audience backing it. Downstream steps (processor, data-file-download) will eventually fail with misleading errors.

---

### 🔴 A6 — Timezone Fallback to `Asia/Kolkata`

**File:** `CampaignTriggerServiceImpl.java` line 62  

If the Auth Manager call to resolve tenant timezone fails, the service silently falls back to `Asia/Kolkata`. For tenants in a different timezone this produces the wrong `startOfDay`/`endOfDay` bounds, causing the wrong day's campaigns to be selected.

**Impact:** Cross-tenant deployments pick campaigns from the wrong calendar day.

---

### 🟠 A7 — Rollback `saveAll` Inside `@Transactional`

**File:** `AudiencePushServiceImpl.java` lines 121–138  

The rollback path calls `saveAll` inside a `@Transactional` block. If the rollback write itself fails, the outer transaction is rolled back as well, leaving the campaign in whatever state it was in before the transaction started — which may differ from the intended rollback target.

**Impact:** Undefined final DB state after a rollback failure.

---

## Service 2: `uclm-campaign-processor`

### 🔴 P1 — `processCallback()` Has No `@Transactional`

**File:** `ControlFileProcessorServiceImpl.java` line 65  

The callback processing method performs **4 separate state updates** with no enclosing transaction. A crash between any two updates commits the earlier ones and loses the rest.

**Impact:** Partially-committed callback results; campaigns in inconsistent intermediate states.

---

### 🔴 P2 — Kafka Publish Failure Sets Wrong Rollback State

**File:** `ControlFileProcessorServiceImpl.java` line 146  

When Kafka publish fails, the code sets campaign state to `PUBLISHED` (the comment in the log even says `CONTROL_FILE_DOWNLOADED`). The correct rollback state would be `CONTROL_FILE_DOWNLOADED`. With `PUBLISHED` set, no retry query can pick the campaign back up because no service polls for campaigns in the wrong-state.

**Impact:** Campaign permanently stuck; unretriable without manual DB correction.

---

### 🔴 P3 — MASTER and DETAILS Updated in Separate Non-Atomic Loops

**File:** `ControlFileProcessorServiceImpl.java` lines 123–138  

`updateCampaignStates()` loops MASTER records and DETAILS records in two separate JPA calls with no wrapping transaction. A crash between the two leaves MASTER and DETAILS in different states.

**Impact:** Split-brain DB state; downstream queries that join MASTER+DETAILS produce inconsistent results.

---

### 🔴 P4 — Enum Name Mismatch: `AUDIENCE_PUSHED` vs `AUDIENCE_REQUESTED`

**File:** Processor `CampaignStateEnum` vs Campaign Manager `CampaignState`  

The processor's eligibility filter queries for campaigns in state `AUDIENCE_PUSHED`. Campaign Manager writes the state as `AUDIENCE_REQUESTED`. If these string values differ across repos the `WHERE state = 'AUDIENCE_PUSHED'` query returns **zero rows** — the processor silently processes nothing.

**Impact:** Entire campaign pipeline stalls; no error surfaced, no alert triggered.

---

### 🔴 P5 — Kafka `send()` Is Fire-and-Forget

**File:** `ControlFileProcessorServiceImpl.java`  

`kafkaTemplate.send()` is called asynchronously. The code immediately updates campaign state to `CONTROL_FILE_PUBLISHED` **before** the broker confirms the message. If the broker rejects the message, the state has already advanced.

**Impact:** Campaign state advanced on Kafka failure; control file event never received by downstream.

---

## Service 3: `uclm-campaign-manager`

### 🔴 M1 — No Concurrency Guard on `/trigger/state-transition`

**File:** `CampaignStateTransitionController.java` lines 25–31  

```java
@PostMapping("/trigger/state-transition")
public Map<String, Object> triggerStateTransition() {
    return stateTransitionService.processStateTransitions();
}
```

Zero concurrency protection. Concurrent Airflow calls (or HTTP retries) both execute `processStateTransitions()` in parallel, fetch the same PUBLISHED campaigns, transition them twice, and emit duplicate Kafka events.

**Impact:** Duplicate state-change events; downstream consumers (analytics, processor) process the same campaign twice; duplicate communications to users.

---

### 🔴 M2 — Kafka Event Published Before DB Transaction Completes

**File:** `CampaignLifecycleServiceImpl.java`

**In `pause()`:**
```java
saveCampaignMaster(master);                                      // line 90 — master saved
campaignAnalyticsService.publishCampaignStatusChanged(...);      // line 92 — Kafka emitted ⚠️
// ...
saveAllCampaignDetails(details);                                 // line 114 — if this throws,
                                                                 //   @Transactional rolls back
                                                                 //   but Kafka event is gone
```

Same pattern exists in `kill()` (lines 189, 191, 210).

**Impact:** If `saveAllCampaignDetails()` throws, the DB transaction rolls back but the Kafka event is already emitted and **cannot be recalled**. External systems (analytics, audit) permanently believe the campaign is paused/killed while the DB still shows it as active. No automated reconciliation path exists.

---

### 🟠 M3 — Frequency Capping Bulk Update Overwrites Unrelated Rules

**File:** `FrequencyCappingServiceImpl.java` lines 206–224  

Validation checks that **one specific rule** exists, but the update query has no `ruleName` filter — it updates **all rows** matching `(channel, goal, subgoal, lob, eventType)`. Multiple unrelated rules sharing those dimensions all get overwritten.

**Impact:** Unintended mass capping override across campaigns sharing any dimension combination.

---

## Service 4: `uclm-campaign-time-validation`

### 🔴 T1 — `PROCESSING_START` Stuck Forever on Server Restart

**File:** `CampaignTriggerServiceImpl.java` lines 125–143  

```java
detailsForParent.forEach(d -> d.setState("PROCESSING_START"));
detailsRepository.saveAll(detailsForParent);   // ← committed to DB

orchestrator.execute(request, ...);            // ← @Async — different thread
```

If the server restarts after `saveAll` but before the async orchestrator completes, the campaign details are permanently stuck in `PROCESSING_START`. The eligibility query only looks for `DATA_FILE_DOWNLOADED` — `PROCESSING_START` records are **never retried**.

**Impact:** Campaigns permanently lost on any pod restart or deployment during execution. Requires manual DB UPDATE to recover.

---

### 🔴 T2 — Orchestrator Failure Leaves Records in `PROCESSING_START`

**File:** `CampaignExecutionOrchestrator.java` lines 270–304  

The catch block logs analytics and re-throws a `RuntimeException`. No code path reverts campaign details back to `DATA_FILE_DOWNLOADED`. The `@Async` context swallows the exception — `CampaignTriggerServiceImpl` never sees it.

**Impact:** Any orchestrator failure (validation error, downstream timeout) irrecoverably orphans campaign details in `PROCESSING_START`.

---

### 🔴 T3 — No Atomic Transaction Around State Change + Orchestrator Dispatch

**File:** `CampaignTriggerServiceImpl.java` lines 125–143  

`CampaignTriggerServiceImpl` has **no `@Transactional` annotation**. Each JPA call uses its own implicit transaction. State is committed before the async call is dispatched — there is no compensating transaction if orchestration fails.

**Impact:** Violates atomicity guarantee; state change is permanent even if execution never starts.

---

### 🔴 T4 — Only First Child Detail UUID Used in Execution Request

**File:** `CampaignTriggerServiceImpl.java` lines 133–135, `buildCampaignRequest()`  

```java
CampaignDetails firstDetail = detailsForParent.get(0);
CampaignExecutionRequest request = buildCampaignRequest(master, firstDetail, ...);
// request.getUuid() == firstDetail.uuid ONLY
```

The orchestrator's state update query filters `WHERE transactionId = :uuid`. Only the first child's UUID matches — all other children's UUIDs do not match and remain in `PROCESSING_START`.

**Impact:** Silent data loss for every campaign with more than one detail record. Scales with campaign volume.

---

### 🔴 T5 — 23:30 Cutoff (Same as Audience-Push, Independently Confirmed)

**File:** `CampaignTriggerServiceImpl.java` lines 66–88  

Identical cutoff logic as A1. Campaigns scheduled after 23:30 are excluded from the eligibility window. Both services independently implement the same defect.

**Impact:** Last 30 minutes of each day's campaigns dropped by both audience-push and time-validation.

---

### 🟠 T6 — `CallerRunsPolicy` Silently Blocks HTTP Thread Under Load

**File:** `AsyncConfig.java` lines 16–26  

```java
executor.setCorePoolSize(5);
executor.setMaxPoolSize(10);
executor.setQueueCapacity(50);
executor.setRejectedExecutionHandler(new ThreadPoolExecutor.CallerRunsPolicy());
```

When all 10 threads are busy and the queue holds 50 items, the 61st call executes **synchronously in the HTTP request thread**. The Airflow trigger appears to hang; campaigns are dropped from async execution without any alert.

**Impact:** Server non-responsiveness under load; silent campaign drops.

---

### 🟡 T7 — Rollback Uses Hardcoded `fallbackState`, Loses Original State

**File:** `CampaignTriggerServiceImpl.java` lines 150–178  

`rollbackCampaignState()` always resets to `triggerProps.getEligibleStates().get(0)` (`DATA_FILE_DOWNLOADED`) regardless of what state each detail was in before. Combines with T1 — if state was already something other than `DATA_FILE_DOWNLOADED`, the rollback introduces a new incorrect state.

---

## Service 5: `uclm-campaign-data-file-download`

### 🔴 D1 — Kafka ACK Before DB Transaction Commits

**File:** `ControlFileListener.java` lines 62–63, 113  

```java
processor.process(payload);   // spawns parallel downloads
ack.acknowledge();             // ← ACK sent immediately after download
// @Transactional commit happens asynchronously
```

If the JVM crashes after `ack.acknowledge()` but before the `@Transactional` block commits:
- Files are on disk / in S3
- Kafka has already marked the message as consumed — **no redelivery**
- DB still shows `CONTROL_FILE_PUBLISHED`

**Impact:** Files orphaned in storage; campaign stuck in `CONTROL_FILE_PUBLISHED` with no trigger to retry.

---

### 🔴 D2 — `transactionId` Collision Updates Wrong Campaigns

**File:** `CampaignStatusServiceImpl.java` lines 37–58  

The UPDATE query:
```sql
WHERE state = 'CONTROL_FILE_PUBLISHED'
  AND transaction_id = :transactionId
  AND parent_campaign_id IN (SELECT id FROM campaign_master WHERE audience_id = :audienceId)
```

Two different parent campaigns can share the same `transactionId`. Both rows match and both are advanced to `DATA_FILE_DOWNLOADED` — even though only one had files downloaded.

**Impact:** Incorrect campaign state advanced; unrelated campaign enters data-processing pipeline without its files.

---

### 🔴 D3 — Partial Batch Failure Causes Cascade State Corruption

**File:** `ControlFileProcessorImpl.java` lines 76–169  

When one of N part-files fails:
- Successfully downloaded parts (1–N-1) are already removed from the `uncommitted` set
- `markDataFileDownloadFailed()` marks **all parts** as FAILED in the DB — including the ones that succeeded
- Successfully downloaded files are not deleted (only the `uncommitted` set is cleaned up)

**Impact:** Wasted bandwidth + orphaned files + incorrect FAILED state on campaigns that were partially successful.

---

### 🔴 D4 — Silent Drop on State Mismatch

**File:** `CampaignStatusServiceImpl.java` lines 60–62  

```java
if (updated == 0) {
    log.warn("No CAMPAIGN_DETAILS rows found ...");
    // ← no exception, no retry, no alert
}
```

If another process has already moved the campaign to a different state, the `WHERE state = 'CONTROL_FILE_PUBLISHED'` filter matches zero rows. The method returns silently. Files are downloaded, Kafka message is acknowledged, but the DB is never updated.

**Impact:** Files downloaded, resources consumed, campaign stuck in wrong state indefinitely. No monitoring alert.

---

### 🟠 D5 — Unsafe Iteration on `Collections.synchronizedSet`

**File:** `ControlFileProcessorImpl.java` lines 79, 140–145  

```java
Set<String> uncommitted = Collections.synchronizedSet(new LinkedHashSet<>());

// Executor threads: uncommitted.remove(handle)        ← concurrent modification
// Main thread:      uncommitted.forEach(h -> delete)  ← iterating simultaneously
```

`Collections.synchronizedSet` is thread-safe for single operations but **not for iteration**. Concurrent modification during `forEach` can throw `ConcurrentModificationException` or silently skip elements.

**Impact:** Incomplete cleanup; orphaned files in storage.

---

## Summary Table

| ID | Service | Severity | Category | Short Description |
|----|---------|----------|----------|-------------------|
| A1 | audience-push | 🔴 Critical | Logic | 23:30 cutoff — last 30 min of day always missed |
| A2 | audience-push | 🔴 Critical | Race condition | AM success + DB failure → `AUDIENCE_REQUESTED` stuck forever |
| A3 | audience-push | 🔴 Critical | Transaction | No `@Transactional` on trigger loop |
| A4 | audience-push | 🔴 Critical | Concurrency | No lock → duplicate AM push on Airflow overlap |
| A5 | audience-push | 🔴 Critical | Error masking | Fake transactionId on empty AM response |
| A6 | audience-push | 🔴 Critical | Timezone | Fallback to `Asia/Kolkata` on Auth Manager failure |
| A7 | audience-push | 🟠 High | Transaction | Rollback `saveAll` inside `@Transactional` |
| P1 | processor | 🔴 Critical | Transaction | `processCallback()` missing `@Transactional` |
| P2 | processor | 🔴 Critical | State machine | Kafka failure sets wrong rollback state (`PUBLISHED`) |
| P3 | processor | 🔴 Critical | Transaction | MASTER + DETAILS updated in separate non-atomic loops |
| P4 | processor | 🔴 Critical | Enum mismatch | Filter on `AUDIENCE_PUSHED` vs actual `AUDIENCE_REQUESTED` |
| P5 | processor | 🔴 Critical | Reliability | Kafka `send()` is fire-and-forget; state advanced before ACK |
| M1 | campaign-manager | 🔴 Critical | Concurrency | No lock on `/trigger/state-transition` → duplicate transitions |
| M2 | campaign-manager | 🔴 Critical | Transaction | Kafka emitted before DB commit in `pause()`/`kill()` |
| M3 | campaign-manager | 🟠 High | Logic | Frequency capping bulk update overwrites unrelated rules |
| T1 | time-validation | 🔴 Critical | State machine | `PROCESSING_START` stuck forever on pod restart |
| T2 | time-validation | 🔴 Critical | Error handling | Orchestrator failure leaves records in `PROCESSING_START` |
| T3 | time-validation | 🔴 Critical | Transaction | No atomic TX wrapping state change + async orchestrator |
| T4 | time-validation | 🔴 Critical | Logic | Only first child detail UUID used — other children orphaned |
| T5 | time-validation | 🔴 Critical | Logic | 23:30 cutoff (same defect as A1, independently confirmed) |
| T6 | time-validation | 🟠 High | Concurrency | `CallerRunsPolicy` blocks HTTP thread under load |
| T7 | time-validation | 🟡 Medium | Error handling | Rollback uses hardcoded `fallbackState` |
| D1 | data-file-download | 🔴 Critical | Race condition | Kafka ACK before `@Transactional` commit |
| D2 | data-file-download | 🔴 Critical | Data integrity | `transactionId` collision updates unrelated campaigns |
| D3 | data-file-download | 🔴 Critical | Transaction | Partial batch failure corrupts state of successful parts |
| D4 | data-file-download | 🔴 Critical | Error handling | Silent drop when state mismatch — no exception, no retry |
| D5 | data-file-download | 🟠 High | Concurrency | Unsafe iteration on `synchronizedSet` |

**Totals:** 20 × 🔴 Critical &nbsp;|&nbsp; 5 × 🟠 High &nbsp;|&nbsp; 1 × 🟡 Medium &nbsp;|&nbsp; **26 total**
