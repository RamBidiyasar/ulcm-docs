# Campaign State Machine

All state transitions across the campaign lifecycle, which service drives each transition, and failure paths.

---

## State Diagram

```mermaid
stateDiagram-v2
    [*] --> DRAFT : Campaign Created\n(Campaign Manager)

    DRAFT --> APPROVAL_PENDING : Submit for Approval\n(Campaign Manager)

    APPROVAL_PENDING --> PUBLISHED : Approved — details created\n(Campaign Manager REST API)
    APPROVAL_PENDING --> REJECTED : Rejected\n(Campaign Manager REST API)
    REJECTED --> [*]

    PUBLISHED --> IN_PROGRESS : Airflow DAG\ncampaign_state_transition_scheduler\n(startTimestamp passed)
    IN_PROGRESS --> COMPLETED : Airflow DAG\ncampaign_state_transition_scheduler\n(endTimestamp passed)
    COMPLETED --> [*]

    IN_PROGRESS --> PAUSED : Manual pause\n(Campaign Manager lifecycle API)
    PAUSED --> IN_PROGRESS : Manual resume\n(Campaign Manager lifecycle API)

    PUBLISHED --> KILLED : Manual kill\n(Campaign Manager lifecycle API)
    IN_PROGRESS --> KILLED : Manual kill\n(Campaign Manager lifecycle API)
    PAUSED --> KILLED : Manual kill\n(Campaign Manager lifecycle API)
    KILLED --> [*]

    PUBLISHED --> AUDIENCE_REQUESTED : Airflow DAG\naudience_push_trigger_scheduler
    AUDIENCE_REQUESTED --> AUDIENCE_PUSHED : AM acknowledges\nrequest
    AUDIENCE_REQUESTED --> PUBLISHED : AM API error\n(Audience Push rollback)

    AUDIENCE_PUSHED --> CALLBACK_RECEIVED : Audience Manager\nsends callback\n(Campaign Processor)
    AUDIENCE_PUSHED --> CALLBACK_FAILED : Callback invalid\nor parsing error\n(Campaign Processor)
    CALLBACK_FAILED --> [*]
    CALLBACK_RECEIVED --> CONTROL_FILE_DOWNLOADED : .ctrl.gz parsed\n(Campaign Processor)
    CONTROL_FILE_DOWNLOADED --> CONTROL_FILE_PUBLISHED : Published to Kafka\n(Campaign Processor)
    CONTROL_FILE_DOWNLOADED --> PUBLISHED : Kafka publish failed\n(Campaign Processor rollback)

    CONTROL_FILE_PUBLISHED --> DATA_FILE_DOWNLOADED : Part files downloaded\n(Data File Download)
    CONTROL_FILE_PUBLISHED --> DATA_FILE_DOWNLOAD_FAILED : Download error\n(Data File Download)

    DATA_FILE_DOWNLOADED --> PROCESSING_START : Dispatch started\n(Time Validation)
    PROCESSING_START --> DATA_FILE_DOWNLOADED : Dispatch error\n(Time Validation rollback)

    note right of APPROVAL_PENDING
        On approval: CampaignDetails rows
        are created and state goes DIRECTLY
        to PUBLISHED. APPROVED is the action
        name in the request — not a persisted state.
    end note

    note right of PUBLISHED
        Two parallel paths can run:
        1. Airflow  state-transition DAG
           (IN_PROGRESS for schedule-based)
        2. Airflow  audience-push DAG
           (AUDIENCE_REQUESTED for audience campaigns)
        EVENT campaigns  IN_PROGRESS immediately
        (no startTimestamp check)
        On Kafka publish failure in processor,
        state reverts from CONTROL_FILE_DOWNLOADED
        back to PUBLISHED for retry.
    end note

    note right of IN_PROGRESS
        EVENT campaigns are NEVER auto-transitioned
        to COMPLETED. They must be manually KILLED.
        Only ONETIME/RECURRING campaigns move to
        COMPLETED when endTimestamp passes.
    end note

    note right of CONTROL_FILE_PUBLISHED
        Kafka message published to:
        control_file_request topic
    end note

    note right of DATA_FILE_DOWNLOADED
        Files stored in:
        local / S3 / in-memory
    end note
    classDef init fill:#95a5a6,color:#000,stroke:#7f8c8d
    classDef active fill:#2980b9,color:#fff,stroke:#1f618d
    classDef success fill:#27ae60,color:#fff,stroke:#1e8449
    classDef fail fill:#e74c3c,color:#fff,stroke:#c0392b
    classDef running fill:#f39c12,color:#000,stroke:#d68910
    class PAUSED active
    class CALLBACK_FAILED,DATA_FILE_DOWNLOAD_FAILED,REJECTED fail
    class APPROVAL_PENDING,DRAFT init
    class IN_PROGRESS,PROCESSING_START running
    class COMPLETED success
```

---

## State Transition Ownership Table

| From State | To State | Service Responsible | Mechanism |
|------------|----------|--------------------|-----------| 
| *(new)* | `DRAFT` | Campaign Manager | REST API call |
| `DRAFT` | `APPROVAL_PENDING` | Campaign Manager | REST API |
| `APPROVAL_PENDING` | `PUBLISHED` | Campaign Manager | REST API — APPROVED action creates CampaignDetails rows, goes directly to PUBLISHED |
| `APPROVAL_PENDING` | `REJECTED` | Campaign Manager | REST API |
| `PUBLISHED` | `IN_PROGRESS` | **Campaign Manager** | Airflow DAG `campaign_state_transition_scheduler` (every 15 min) — startTimestamp passed |
| `PUBLISHED` | `IN_PROGRESS` | **Campaign Manager** | EVENT campaigns: immediate on next DAG cycle (no startTimestamp) |
| `IN_PROGRESS` | `COMPLETED` | **Campaign Manager** | Airflow DAG `campaign_state_transition_scheduler` (every 15 min) — endTimestamp passed |
| `IN_PROGRESS` | `PAUSED` | Campaign Manager | Lifecycle REST API |
| `PAUSED` | `IN_PROGRESS` | Campaign Manager | Lifecycle REST API (resume) |
| `PUBLISHED` / `IN_PROGRESS` / `PAUSED` | `KILLED` | Campaign Manager | Lifecycle REST API |
| `PUBLISHED` | `AUDIENCE_REQUESTED` | **Audience Push** | Airflow DAG `audience_push_trigger_scheduler` (every 15 min) |
| `AUDIENCE_REQUESTED` | `AUDIENCE_PUSHED` | **Audience Push** | AM API response |
| `AUDIENCE_REQUESTED` | `PUBLISHED` (rollback) | **Audience Push** | AM API error — direct rollback, no intermediate state |
| `AUDIENCE_PUSHED` | `CALLBACK_RECEIVED` | **Campaign Processor** | AM callback POST |
| `AUDIENCE_PUSHED` | `CALLBACK_FAILED` | **Campaign Processor** | Callback invalid / parsing error (terminal) |
| `CALLBACK_RECEIVED` | `CONTROL_FILE_DOWNLOADED` | **Campaign Processor** | .ctrl.gz parsed |
| `CONTROL_FILE_DOWNLOADED` | `CONTROL_FILE_PUBLISHED` | **Campaign Processor** | Kafka produce success |
| `CONTROL_FILE_DOWNLOADED` | `PUBLISHED` (rollback) | **Campaign Processor** | Kafka publish failed — reverts for retry |
| `CONTROL_FILE_PUBLISHED` | `DATA_FILE_DOWNLOADED` | **Data File Download** | All parts downloaded |
| `CONTROL_FILE_PUBLISHED` | `DATA_FILE_DOWNLOAD_FAILED` | **Data File Download** | Download error |
| `DATA_FILE_DOWNLOADED` | `PROCESSING_START` | **Time Validation** | Airflow DAG `campaign_execution_trigger_scheduler` — dispatch started |
| `PROCESSING_START` | `DATA_FILE_DOWNLOADED` (rollback) | **Time Validation** | Dispatch error — reverts for retry |

---

## Failure & Compensation Flow

```mermaid
flowchart TD
    PUB[PUBLISHED]

    PUB -->|Audience Push calls AM| AR[AUDIENCE_REQUESTED]
    AR -->|AM success| AP[AUDIENCE_PUSHED]
    AR -->|AM error| PUB

    AP -->|AM sends callback| CR[CALLBACK_RECEIVED]
    AP -->|Callback invalid / parsing error| CBF[CALLBACK_FAILED]
    CR -->|ctrl.gz parsed| CD[CONTROL_FILE_DOWNLOADED]
    CD -->|Kafka publish success| CP[CONTROL_FILE_PUBLISHED]
    CD -->|Kafka publish failed| PUB

    CP -->|All parts downloaded| DD[DATA_FILE_DOWNLOADED]
    CP -->|Any part fails| DDF[DATA_FILE_DOWNLOAD_FAILED]

    DD -->|Dispatch started| PS[PROCESSING_START]
    PS -->|Dispatch error| DD

    CBF --> STOP1([Terminal Error ])
    DDF --> STOP2([Terminal Error ])

    classDef success fill:#27ae60,color:#fff,stroke:#1e8449
    classDef failure fill:#e74c3c,color:#fff,stroke:#c0392b
    classDef normal fill:#4a90d9,color:#fff,stroke:#2c6ea8

    class CBF,DDF,STOP1,STOP2 failure
    class PUB,AR,AP,CR,CD,CP,DD,PS normal
    classDef stop fill:#e74c3c,color:#fff,stroke:#c0392b
```
