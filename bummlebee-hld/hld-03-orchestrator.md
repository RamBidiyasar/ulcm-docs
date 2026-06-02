# HLD — uclm-orchestrator-service

**Role:** Multi-channel dispatch engine. Consumes validated payloads from Kafka, validates message expiry, routes to the first enabled channel provider via Feign clients, manages CMS quota decrements, and publishes structured success/error responses plus analytics events.

---

## 1. Purpose & Responsibilities

| Responsibility | Detail |
|---------------|--------|
| **Time Validation** | Rejects messages whose `expire_timestamp` is in the past (configurable timezone, multi-format support) |
| **Channel Routing** | Priority-based single-winner routing: SMS IQ → Email → WhatsApp → Push → SMS Lobby → RCS |
| **Provider Dispatch** | Calls external channel provider REST APIs via Spring Cloud OpenFeign clients |
| **Retry & Resilience** | Resilience4j `@Retry` wraps every provider call — retries only on HTTP 5xx / connection errors |
| **CMS Quota Decrement** | Decrements quota via CMS REST API on expiry or provider failure when `cmsRequired=true` |
| **Account Resolution** | Resolves sender account from Hazelcast-backed in-memory lookup; supports `SINGLE` and `ROUND_ROBIN` strategies |
| **Response Publishing** | Publishes `FullKafkaDml` to success/error topics using a configurable publish mode (`FULL_ONLY` / `APB_ONLY` / `BOTH`) |
| **APB Contract** | Builds and publishes `ApbKafkaContract` to APB success/error topics for campaign tracking |
| **Analytics** | Builds `AnalyticsEventDTO` and publishes asynchronously to the analytics Kafka topic |
| **Exception Forwarding** | On Kafka deserialization or unhandled exceptions, forwards `ExceptionEvent` to `orchestrator-exceptions` topic |

---

## 2. High-Level Architecture

```mermaid
flowchart TB
    DT(["📨 Kafka: dispatch topic\ngroup: dispatch-request-consumer-group\nconcurrency: 4"])

    subgraph ORCH["uclm-orchestrator-service  ·  port 8080  ·  Kafka-driven, no REST"]
        direction TB

        subgraph KCG["① KafkaConsumer  @KafkaListener"]
            direction TB
            PARSE["Parse JSON\nobjectMapper.readTree()"]
            TRACE["Extract uuid\nMDC.put(traceId, requestId)"]
            NULL{eventRequest\nnull?}
            TV{expire_timestamp\nexpired?}
            EXP_H["handleExpiredMessage()\ncmsRequired? → decrementQuota()\npublishErrorResponse(410)"]
            ROUTE["routeMessage()\nfirst-enabled flag wins"]
        end

        subgraph CHN["② Channel Routing — only ONE flag active per deployed pod"]
            direction LR
            SVC_SMS["SmsService\nSMS IQ"]
            SVC_EMAIL["EmailService\nNetcore / SMTP"]
            SVC_WA["WhatsAppService\nAirtel IQ WA"]
            SVC_PUSH["PushService\nFCM"]
            SVC_LOBBY["SmsLobbyService\nSMS Lobby"]
            SVC_RCS["RcsService\nRCS IQ"]
        end

        subgraph BASE["③ BaseChannelService — Template Method"]
            direction TB
            RETRY_W["ProviderRetryService\nmaxAttempts=3 · backoff 1s×2.0 · max 10s"]
            OK_P["handleSuccess()"]
            FAIL_P["handleFailure()\ncmsRequired? → cmsService.decrementQuota(PROVIDER_FAILED)"]
        end

        CMS_SVC["CmsService\nPOST /counter/decrement · Bearer token"]
        HZ["Hazelcast In-Memory Lookup\nAccount creds by sender_id\nAccountContextHolder ThreadLocal"]

        subgraph PIPELINE["④ Response Publishing Pipeline"]
            direction LR
            RP_PUB["ResponseKafkaPublisher"]
            FDB["FullDmlBuilder\nbuildFullKafkaDml()"]
            PSF_N["PublishStrategyFactory\nFULL_ONLY / APB_ONLY / BOTH"]
            ANA_B["AnalyticsEventBuilder\n+ AnalyticsEventPublisher async"]
            FULL_K["FullDmlKafkaPublisher\nasync"]
            APB_K["ApbKafkaPublisher\n+ ApbContractBuilder async"]
        end
    end

    subgraph EXT["External Channel Providers — Spring Cloud OpenFeign"]
        direction LR
        E_SMS["Airtel SMS IQ\nPOST /api/v1/send-sms\nBasic auth"]
        E_EMAIL["Netcore CPaaS\nPOST /v5.1/mail/send  x-api-key\nOR  SMTP / JavaMail"]
        E_WA["Airtel IQ WhatsApp\nPOST /api/v2/message/nc\nBearer token"]
        E_PUSH["Firebase FCM\nOAuth2 client creds"]
        E_RCS["Airtel IQ Conversation\nPOST .../rcs/message/send\nBasic auth"]
        E_CMS["CMS Service\nPOST /counter/decrement\nBearer token"]
    end

    T_SUCC(["✅ dispatch-response\nsuccess topic"])
    T_ERR(["❌ error topic\nfailure responses"])
    T_APB_S(["apb-success-log"])
    T_APB_E(["apb-log"])
    T_ANA(["analytics topic"])
    T_EX(["orchestrator-exceptions\ndeserialization / unhandled errors"])

    DT --> PARSE
    PARSE --> TRACE --> NULL
    NULL -->|"yes → EMPTY_REQUEST"| T_ERR
    NULL -->|no| TV
    TV -->|expired| EXP_H
    TV -->|valid| ROUTE

    EXP_H --> CMS_SVC
    EXP_H --> RP_PUB

    ROUTE -->|smsEnabled| SVC_SMS
    ROUTE -->|emailEnabled| SVC_EMAIL
    ROUTE -->|waEnabled| SVC_WA
    ROUTE -->|pushEnabled| SVC_PUSH
    ROUTE -->|smsLobbyEnabled| SVC_LOBBY
    ROUTE -->|rcsEnabled| SVC_RCS
    ROUTE -->|"none enabled"| T_ERR

    SVC_SMS & SVC_EMAIL & SVC_WA & SVC_PUSH & SVC_LOBBY & SVC_RCS --> RETRY_W
    SVC_SMS -.->|Feign| E_SMS
    SVC_EMAIL -.->|Feign| E_EMAIL
    SVC_WA -.->|Feign| E_WA
    SVC_WA -.->|account lookup| HZ
    SVC_PUSH -.->|Feign| E_PUSH
    SVC_RCS -.->|Feign| E_RCS

    RETRY_W -->|2xx| OK_P
    RETRY_W -->|"4xx/5xx after retries"| FAIL_P
    FAIL_P --> CMS_SVC
    CMS_SVC -.->|Feign| E_CMS

    OK_P --> RP_PUB
    FAIL_P --> RP_PUB

    RP_PUB --> FDB --> PSF_N
    PSF_N -->|FULL_ONLY| FULL_K
    PSF_N -->|APB_ONLY| APB_K
    PSF_N -->|BOTH| FULL_K & APB_K
    RP_PUB --> ANA_B

    FULL_K --> T_SUCC
    FULL_K --> T_ERR
    APB_K --> T_APB_S
    APB_K --> T_APB_E
    ANA_B --> T_ANA
    DT -->|"DeserializationException / unhandled"| T_EX

    classDef kafka fill:#f39c12,color:#000,stroke:#d68910
    classDef svc fill:#2980b9,color:#fff,stroke:#1f618d
    classDef db fill:#27ae60,color:#fff,stroke:#1e8449
    classDef channel fill:#d35400,color:#fff,stroke:#a04000
    classDef user fill:#34495e,color:#fff,stroke:#2c3e50
    classDef stop fill:#e74c3c,color:#fff,stroke:#c0392b

    class DT,T_SUCC,T_ERR,T_APB_S,T_APB_E,T_ANA,T_EX kafka
    class E_SMS,E_EMAIL,E_WA,E_PUSH,E_RCS,E_CMS channel
    class SVC_SMS,SVC_EMAIL,SVC_WA,SVC_PUSH,SVC_LOBBY,SVC_RCS,RETRY_W,RP_PUB,PSF_N,FULL_K,APB_K svc
    class CMS_SVC,HZ,OK_P db
    class PARSE,TRACE,ROUTE,FDB,ANA_B user
    class NULL,TV,EXP_H,FAIL_P stop
```

---

## 3. Detailed Processing Flow

```mermaid
sequenceDiagram
    participant KF as Kafka (dispatch topic)
    participant KC as KafkaConsumer
    participant TV as TimeValidationService
    participant CS as ChannelService (SMS/Email/WA/Push/RCS/Lobby)
    participant BS as BaseChannelService
    participant PR as ProviderRetryService
    participant FC as FeignClient (channel provider)
    participant CMS as CmsService
    participant RP as ResponseKafkaPublisher
    participant FD as FullDmlBuilder
    participant PS as PublishStrategyFactory
    participant AE as AnalyticsEventPublisher
    participant SK as Kafka (success/error topics)
    participant AK as Kafka (analytics topic)
    participant EX as ExceptionKafkaProducer

    KF->>KC: ChannelDispatchRequest JSON string
    KC->>KC: objectMapper.readTree(msg)
    KC->>KC: extract eventRequest node
    KC->>KC: MDC.put(traceId) + MDC.put(requestId=uuid)

    alt eventRequest is null
        KC->>RP: publishErrorResponse(4109, EMPTY_REQUEST)
        RP->>SK: error topic
    else eventRequest present
        KC->>TV: isExpired(json)?
        Note over TV: Parse expire_timestamp (multi-format)<br/>Compare with LocalDateTime.now(IST)
        alt Message expired
            TV-->>KC: true
            KC->>CMS: decrementQuota(payload, TIME_VALIDATION_FAILED) if cmsRequired
            KC->>RP: publishErrorResponse(410, TIME_VALIDATION_FAILED)
            RP->>FD: buildFullKafkaDml(payload, null)
            FD-->>RP: FullKafkaDml{status=FAILED}
            RP->>PS: getStrategy(publishMode)
            PS-->>RP: PublishModeStrategy
            RP->>SK: publish to error topic
            RP->>AE: publish AnalyticsEventDTO
            AE->>AK: analytics topic
        else Message valid
            TV-->>KC: false
            KC->>CS: channelService.send(payload)
            CS->>BS: send(payload) [inherited]
            BS->>PR: execute(payload, () → sendToProvider())
            Note over PR: @Retry(name=providerRetry)<br/>maxAttempts=3, backoff=1s×2.0<br/>retries on HTTP 5xx or connection error
            PR->>FC: POST to channel provider API
            alt Provider SUCCESS (2xx)
                FC-->>PR: HTTP 2xx + response body
                PR-->>BS: success
                BS->>RP: publishSuccessResponse(uuid, response, statusCode, payload)
                RP->>FD: buildFullKafkaDml(payload, response)
                FD-->>RP: FullKafkaDml{status=SUCCESS, endpointRequestId, responseData}
                RP->>PS: getStrategy(publishMode)
                PS-->>RP: PublishModeStrategy
                RP->>SK: publish to success topic (dispatch-response + apb-success-log)
                RP->>AE: publish AnalyticsEventDTO [non-WhatsApp only]
                AE->>AK: analytics topic
            else Provider FAILURE (FeignException after retries)
                FC-->>PR: HTTP 4xx/5xx
                PR-->>BS: FeignException (after exhausting retries)
                BS->>CMS: decrementQuota(payload, PROVIDER_FAILED) if cmsRequired
                BS->>RP: publishErrorResponse(statusCode, PROVIDER_FAILED)
                RP->>SK: publish to error topic
                RP->>AE: publish AnalyticsEventDTO
                AE->>AK: analytics topic
            end
        end
    end

    alt Kafka deserialization or unhandled exception
        KC->>EX: buildExceptionEvent(record, exception)
        EX->>KF: orchestrator-exceptions topic
    end
```

---

## 4. Channel Routing & Feature Flags

```mermaid
flowchart TB
    IN([Kafka: dispatch topic\nChannelDispatchRequest JSON])
    TV{Time\nValidation\nExpired?}
    CH_SMS{sms-enabled?}
    CH_EMAIL{email-enabled?}
    CH_WA{whatsapp-enabled?}
    CH_PUSH{push-enabled?}
    CH_LOBBY{sms-lobby-enabled?}
    CH_RCS{rcs-enabled?}
    NONE[publishError\nNO_CHANNEL_ENABLED]
    EXP[handleExpiredMessage\nCMS decrement if cmsRequired\npublishError 410]

    SMS["SmsService\nSMS IQ via Airtel API"]
    EMAIL["EmailService\nNetcore or SMTP fallback"]
    WA["WhatsAppService\nAirtel IQ WhatsApp"]
    PUSH["PushService\nFirebase FCM"]
    LOBBY["SmsLobbyService\nSMS Lobby HTTP"]
    RCS["RcsService\nAirtel IQ Conversation"]

    OUT_OK([publishSuccessResponse\ndispatch-response topic])
    OUT_ERR([publishErrorResponse\nerror topic])

    IN --> TV
    TV -->|yes| EXP
    TV -->|no| CH_SMS
    EXP --> OUT_ERR
    CH_SMS -->|yes| SMS
    CH_SMS -->|no| CH_EMAIL
    CH_EMAIL -->|yes| EMAIL
    CH_EMAIL -->|no| CH_WA
    CH_WA -->|yes| WA
    CH_WA -->|no| CH_PUSH
    CH_PUSH -->|yes| PUSH
    CH_PUSH -->|no| CH_LOBBY
    CH_LOBBY -->|yes| LOBBY
    CH_LOBBY -->|no| CH_RCS
    CH_RCS -->|yes| RCS
    CH_RCS -->|no| NONE
    SMS --> OUT_OK
    EMAIL --> OUT_OK
    WA --> OUT_OK
    PUSH --> OUT_OK
    LOBBY --> OUT_OK
    RCS --> OUT_OK
    NONE --> OUT_ERR

    classDef kafka fill:#f39c12,color:#000,stroke:#d68910
    classDef svc fill:#2980b9,color:#fff,stroke:#1f618d
    classDef db fill:#27ae60,color:#fff,stroke:#1e8449
    classDef channel fill:#d35400,color:#fff,stroke:#a04000
    classDef user fill:#34495e,color:#fff,stroke:#2c3e50
    classDef stop fill:#e74c3c,color:#fff,stroke:#c0392b

    class IN,OUT_OK,OUT_ERR kafka
    class SMS,EMAIL,WA,PUSH,LOBBY,RCS svc
    class TV,CH_SMS,CH_EMAIL,CH_WA,CH_PUSH,CH_LOBBY,CH_RCS user
    class NONE,EXP stop
```

> **Important:** Only **one channel is active per deployed instance**. The flags are mutually exclusive at runtime — the first `true` flag wins. Different channel instances are deployed as separate pods in Kubernetes, each with exactly one flag enabled.

---

## 5. BaseChannelService — Template Method Pattern

All channel services extend `BaseChannelService` which provides the common lifecycle:

```mermaid
flowchart TD
    ENTRY["BaseChannelService.send(JsonNode payload)"]
    LOG1["Log: Channel={name} processing uuid={uuid}\nstart = System.currentTimeMillis()"]
    RETRY["ProviderRetryService.execute(payload, sendToProvider)\n@Retry name=providerRetry"]
    ABSTRACT["sendToProvider(payload)\nAbstract — implemented by each subclass:\nSmsService · EmailService · WhatsAppService\nPushService · RcsService · SmsLobbyService"]
    PROV{Provider\nresponse?}
    RETRY_AGAIN["Retry with exponential backoff\nattempts 1 → 2 → 3"]
    SUCCESS["handleSuccess(payload, response, statusCode)\nLog: completed in latency ms"]
    PUB_SUCC["responseKafkaPublisher\n.publishSuccessResponse()"]
    FEIGN_EX["FeignException — retries exhausted\nLog: failed after latency ms with status=N"]
    FAIL["handleFailure(payload, error, statusCode, response)"]
    CMS_CHK{cmsRequired?}
    CMS_DECR["cmsService.decrementQuota\n(payload, PROVIDER_FAILED)"]
    PUB_ERR["responseKafkaPublisher\n.publishErrorResponse()"]

    ENTRY --> LOG1 --> RETRY --> ABSTRACT --> PROV
    PROV -->|"HTTP 2xx"| SUCCESS --> PUB_SUCC
    PROV -->|"5xx / conn error"| RETRY_AGAIN --> RETRY
    PROV -->|"4xx — not retried"| FEIGN_EX
    PROV -->|"retries exhausted"| FEIGN_EX
    FEIGN_EX --> FAIL --> CMS_CHK
    CMS_CHK -->|yes| CMS_DECR --> PUB_ERR
    CMS_CHK -->|no| PUB_ERR

    classDef kafka fill:#f39c12,color:#000,stroke:#d68910
    classDef svc fill:#2980b9,color:#fff,stroke:#1f618d
    classDef db fill:#27ae60,color:#fff,stroke:#1e8449
    classDef user fill:#34495e,color:#fff,stroke:#2c3e50
    classDef stop fill:#e74c3c,color:#fff,stroke:#c0392b

    class PUB_SUCC,PUB_ERR kafka
    class RETRY,ABSTRACT,RETRY_AGAIN svc
    class CMS_DECR,SUCCESS db
    class ENTRY,LOG1,FAIL user
    class FEIGN_EX,CMS_CHK,PROV stop
```

| Abstract Method | Description |
|----------------|-------------|
| `sendToProvider(JsonNode payload)` | Channel-specific API call logic — each subclass implements this |
| `getChannelName()` | Returns the channel enum name for logging |

---

## 6. Channel Service Implementations

### 6.1 SmsService → Airtel SMS IQ

```
Feign Client  : SmsClient
URL           : ${app.channels.sms.url}/api/v1/send-sms
Auth          : Basic (username:password)
Success code  : HTTP 200

Payload transformation (transformToSmsFormat):
  Reads from payload.channelRequest → extracts SMS API fields:
  customerId, destinationAddress, sourceAddress, messageType,
  entityId, message, dltTemplateId, urlShortenerParams, metaData
  (strips all non-SMS fields before forwarding)
```

### 6.2 EmailService → Netcore / SMTP (dual-provider)

```
Providers injected as List<EmailProvider> — Spring auto-discovers all beans

Provider 1 — NetcoreEmailProvider (when netcore.enabled=true):
  Feign Client  : NetcoreEmailClient
  URL           : ${app.channels.email.url}/v5.1/mail/send
  Auth          : x-api-key header
  Success code  : HTTP 202 (ACCEPTED)

Provider 2 — SmtpEmailProvider (when smtp.enabled=true):
  Framework     : Spring Boot Mail (JavaMail)
  Host          : ${spring.mail.host}:${spring.mail.port}
  Auth          : ${spring.mail.properties.mail.smtp.auth}
  Success code  : HTTP 202 (synthetic — SMTP has no HTTP response)

Fallback: Iterates providers in order; first to succeed wins.
```

### 6.3 WhatsAppService → Airtel IQ WhatsApp

```
Feign Client  : WhatsAppClient
URL           : ${app.channels.whatsapp.url}/api/v2/message/nc
Auth          : Bearer token (${app.channels.whatsapp.token})
Default from  : ${app.channels.whatsapp.from}

Account lookup (AccountContextHolder):
  1. Extract sender_id from eventRequest
  2. lookupService.get(wa.creds.file, sender_id) → Hazelcast IMap
  3. If not found → RuntimeException("No Account Details Found")
  4. AccountContextHolder.set(accountDetails) → picked up by WhatsAppClient interceptor
  5. Finally → AccountContextHolder.clear()

Payload: templateId, to, message, from, mediaAttachment (optional)
```

### 6.4 PushService → Firebase FCM

```
Feign Client  : PushClient
URL           : ${app.channels.push.provider-config.url}/${endpoint}
Auth          : OAuth2 (client_id + client_secret)
Success code  : responseBody.statusValue == 200

Response parsing:
  HTTP 2xx → check inner statusValue
    200 → handleSuccess()
    else → extract data.message → handleFailure()
```

### 6.5 RcsService → Airtel IQ Conversation

```
Feign Client  : RcsClient
URL           : ${app.channels.rcs.url}/gateway/.../rcs/message/send
Auth          : Basic (username:password)
Headers       : customer-id, sub-account-id, agent-id, app-id
Success code  : HTTP 200

Payload normalization:
  1. templateId present in channelRequest → use as-is (flattened structure)
  2. channelRequest.rcs nested → unwrap to rcs node (legacy)
  3. Fallback → use full payload
  4. Populate customerId + subAccountId if blank
  5. msisdn normalization: prefix "+91" if no country code
```

### 6.6 SmsLobbyService → SMS Lobby

```
Feign Client  : SmsLobbyClient
URL           : ${app.channels.sms-lobby.url}  (e.g. http://10.92.230.97:10200/cgi-bin/sendsms)
Auth          : None (internal network)
Alternate SMS dispatch route when main SMS IQ is not enabled
```

---

## 7. Retry & Resilience

```mermaid
flowchart TB
    CALL["providerRetryService.execute()\n@Retry(name=providerRetry)"]
    PRED["ProviderRetryPredicate\ntest(Throwable)"]
    HTTP5XX["FeignException\nstatus >= 500"]
    CONNFAIL["FeignException\nstatus == -1\n(connection refused)"]
    HTTP4XX["FeignException\nstatus 4xx\n(client error)"]
    RETRY["Retry attempt\nbackoff: 1 s × 2.0\nmax: 10 s\nmaxAttempts: 3"]
    FALLBACK["fallback()\nre-throw exception\n→ BaseChannelService.handleFailure()"]
    SUCCESS["Provider responded\n→ handleSuccess()"]
    NORETRY["No retry\nthrow immediately\n→ handleFailure()"]

    CALL --> PRED
    PRED --> HTTP5XX
    PRED --> CONNFAIL
    PRED --> HTTP4XX
    HTTP5XX -->|retryable=true| RETRY
    CONNFAIL -->|retryable=true| RETRY
    HTTP4XX -->|retryable=false| NORETRY
    RETRY -->|attempt 1/2/3| CALL
    RETRY -->|exhausted| FALLBACK
    CALL -->|2xx| SUCCESS

    classDef kafka fill:#f39c12,color:#000,stroke:#d68910
    classDef svc fill:#2980b9,color:#fff,stroke:#1f618d
    classDef stop fill:#e74c3c,color:#fff,stroke:#c0392b
    classDef user fill:#34495e,color:#fff,stroke:#2c3e50
    classDef db fill:#27ae60,color:#fff,stroke:#1e8449

    class SUCCESS,RETRY db
    class FALLBACK,NORETRY stop
    class CALL,PRED svc
    class HTTP5XX,CONNFAIL,HTTP4XX user
```

| Property | Value | Description |
|----------|-------|-------------|
| `app.retry.max-attempts` | `3` | Total attempts (1 original + 2 retries) |
| `app.retry.delay-ms` | `1000` | Initial backoff delay |
| `app.retry.multiplier` | `2.0` | Exponential backoff multiplier |
| `app.retry.max-delay-ms` | `10000` | Cap on backoff delay |
| Retryable | `status == -1` or `status >= 500` | Connection errors and server errors |
| Non-retryable | `status 4xx` | Client errors — not retried |

---

## 8. Account Resolution (Hazelcast Lookup)

```mermaid
flowchart TB
    RESOLVE["ChannelAccountResolver\nresolveAccount(channel)"]
    LOOKUP["InMemoryLookupService\ngetAll(accountDetailsFileName)\n→ Hazelcast IMap lookup-{domain}"]
    PROPS["AccountRoutingProperties\nmultipleEnabled: true/false\nstrategy: ROUND_ROBIN / SINGLE"]
    FACT["AccountSelectionStrategyFactory\ngetStrategy(strategyType)"]
    SINGLE["SingleAccountStrategy\nreturns first account always"]
    RR["RoundRobinAccountStrategy\nAtomicLong counter\nindex = counter++ mod accounts.size()\nstable order via key sort"]
    ACCT["JsonNode accountDetails\n→ injected via AccountContextHolder\n→ read by Feign request interceptor"]
    HZ["Hazelcast\nembedded, single-node\ncluster: orchestrator-cluster\nIMap: lookup-{domain}"]

    RESOLVE --> LOOKUP
    LOOKUP --> HZ
    RESOLVE --> PROPS
    PROPS --> FACT
    FACT -->|SINGLE| SINGLE
    FACT -->|ROUND_ROBIN| RR
    SINGLE --> ACCT
    RR --> ACCT

    classDef kafka fill:#f39c12,color:#000,stroke:#d68910
    classDef svc fill:#2980b9,color:#fff,stroke:#1f618d
    classDef db fill:#27ae60,color:#fff,stroke:#1e8449
    classDef user fill:#34495e,color:#fff,stroke:#2c3e50

    class HZ db
    class RESOLVE,FACT svc
    class SINGLE,RR user
    class PROPS,LOOKUP,ACCT kafka
```

- Hazelcast is **embedded** (single-node, no clustering) — purely an in-memory data structure host.
- Lookup data is loaded via `InMemoryLookupService.load(domain, map)` at startup.
- `AccountContextHolder` uses a `ThreadLocal<JsonNode>` so account credentials are scoped per request thread.

---

## 9. CMS Integration

```mermaid
flowchart TD
    TRIGGER["CmsService.decrementQuota(payload, reason)\nCalled on: ① expiry  ② provider failure"]
    EXTRACT["Extract uuid from eventRequest\nExtract CmsDecrementRequest from payload.cmsRequest\n(pre-built by Validation-Governance)"]
    FEIGN["CmsClient Feign\nPOST /counter/decrement\nHeaders: Authorization: Bearer token\ntenant_id · dept_id · user_id"]
    CMS_EXT[("CMS Service\nexternal")]
    RESP{CmsDecrementResponse\nstatus?}
    SUCC["Log: CMS decrement successful\nreturn SUCCESS JSON"]
    BIZ_FAIL["Log: CMS business error\nreturn FAILURE JSON with reason"]
    FEIGN_EX["FeignException\nLog error\nreturn FAILURE JSON"]
    GEN_EX["Generic Exception\nLog error\nreturn FAILURE JSON"]
    MAIN_FLOW["Main flow continues\nCMS response appended to FullKafkaDml\nDoes NOT block or fail the dispatch"]

    TRIGGER --> EXTRACT --> FEIGN
    FEIGN -.->|HTTP POST| CMS_EXT
    CMS_EXT -.->|response| RESP
    RESP -->|SUCCESS| SUCC
    RESP -->|FAILURE| BIZ_FAIL
    FEIGN -->|FeignException| FEIGN_EX
    FEIGN -->|Exception| GEN_EX
    SUCC --> MAIN_FLOW
    BIZ_FAIL --> MAIN_FLOW
    FEIGN_EX --> MAIN_FLOW
    GEN_EX --> MAIN_FLOW

    classDef kafka fill:#f39c12,color:#000,stroke:#d68910
    classDef svc fill:#2980b9,color:#fff,stroke:#1f618d
    classDef db fill:#27ae60,color:#fff,stroke:#1e8449
    classDef channel fill:#d35400,color:#fff,stroke:#a04000
    classDef user fill:#34495e,color:#fff,stroke:#2c3e50
    classDef stop fill:#e74c3c,color:#fff,stroke:#c0392b

    class CMS_EXT channel
    class TRIGGER,EXTRACT,FEIGN svc
    class SUCC,MAIN_FLOW db
    class BIZ_FAIL,FEIGN_EX,GEN_EX stop
    class RESP user
```

---

## 10. Response Publishing Strategy

```mermaid
flowchart TB
    RP["ResponseKafkaPublisher\npublishSuccessResponse() / publishErrorResponse()"]
    FDB["FullDmlBuilder\nbuildFullKafkaDml(requestPayload, responsePayload)\n→ maps EventRequest fields + provider response"]
    PSF["PublishStrategyFactory\ngetStrategy(app.publish-mode)"]
    FULL["FullDmlKafkaPublisher\npublishSuccess → dispatch-response\npublishError   → error topic\nasync CompletableFuture"]
    APB["ApbKafkaPublisher\nbuildSuccessEvent()/buildErrorEvent()\npublishSuccess → apb-success-log\npublishError   → apb-log\nasync CompletableFuture"]
    BOTH["BothPublishStrategy\ncalls FullDml + APB publishers"]
    AEB["AnalyticsEventBuilder\nbuildAnalyticsEvent(fullKafkaDml, response)\n→ maps channel, status, campaign fields"]
    AEP["AnalyticsEventPublisher\nasync CompletableFuture\n→ analytics topic"]
    AK["Kafka: analytics topic"]
    SK_S["Kafka: dispatch-response\n(success topic)"]
    SK_E["Kafka: error topic"]
    SK_APB["Kafka: apb-success-log\napb-log"]

    RP --> FDB
    FDB --> PSF
    PSF -->|FULL_ONLY| FULL
    PSF -->|APB_ONLY| APB
    PSF -->|BOTH| BOTH
    BOTH --> FULL
    BOTH --> APB
    FULL --> SK_S
    FULL --> SK_E
    APB --> SK_APB
    RP --> AEB
    AEB --> AEP
    AEP --> AK

    classDef kafka fill:#f39c12,color:#000,stroke:#d68910
    classDef svc fill:#2980b9,color:#fff,stroke:#1f618d
    classDef user fill:#34495e,color:#fff,stroke:#2c3e50
    classDef db fill:#27ae60,color:#fff,stroke:#1e8449

    class SK_S,SK_E,SK_APB,AK kafka
    class RP,PSF,FULL,APB,BOTH svc
    class FDB,AEB,AEP user
```

> **Analytics exclusion:** `AnalyticsEventDTO` is **not** published for the WhatsApp channel — WA analytics are handled separately.

---

## 11. FullKafkaDml Construction

`FullDmlBuilder.buildFullKafkaDml()` maps request + provider response into a unified DML record:

```mermaid
flowchart TD
    IN["buildFullKafkaDml\n(requestPayload JsonNode, responsePayload ResponseEntity)"]
    NULL_CHK{requestPayload\nnull?}
    MINIMAL["Return minimal FullKafkaDml\nstatus=FAILED · error_code=4109\nerror_message=Invalid or missing request payload"]
    EXTRACT_ER["Extract eventRequest node\nConvert to EventRequest POJO\n(objectMapper.convertValue)"]
    EXTRACT_RESP["Parse responsePayload.body → JsonNode\n(skip if smsLobbyEnabled or SMTP email)"]
    EXTRACT_ID["Extract endpointRequestId per MOC\nsms / whatsapp / rcs → messageReqId\nemail → data.messageId\npush_bank / push_thanks → data.tid"]
    BUILD["Build FullKafkaDml builder\n① Campaign: name, group, type, category, lob, si_id, moc\n② Timestamps: source, start, expiry, dashboard\n③ Content: script_body, script_sub, msg_lng_cd\n④ Tracking: uuid, circle_id, email_id, workspace_id\n⑤ Lists: dynmc_prm, dynmc_prm_sub, additional_fields\n⑥ Meta: module_name=orchestrator-service, event_date, input_topic\n⑦ msg_typ_cd from additional_fields[template_id]"]
    STATUS["Caller sets:\nstatus = SUCCESS or FAILED\nerror_code, error_message"]
    RESP_MAP["response_data = Map moc → responsePayload\nendpoint_request_id = extracted above"]
    OUT(["FullKafkaDml ready\n→ PublishModeStrategy.publishSuccess/Error()"])

    IN --> NULL_CHK
    NULL_CHK -->|yes| MINIMAL
    NULL_CHK -->|no| EXTRACT_ER
    EXTRACT_ER --> EXTRACT_RESP
    EXTRACT_RESP --> EXTRACT_ID
    EXTRACT_ID --> BUILD
    BUILD --> STATUS
    STATUS --> RESP_MAP --> OUT

    classDef kafka fill:#f39c12,color:#000,stroke:#d68910
    classDef svc fill:#2980b9,color:#fff,stroke:#1f618d
    classDef db fill:#27ae60,color:#fff,stroke:#1e8449
    classDef user fill:#34495e,color:#fff,stroke:#2c3e50
    classDef stop fill:#e74c3c,color:#fff,stroke:#c0392b

    class OUT kafka
    class IN,EXTRACT_ER,EXTRACT_RESP,EXTRACT_ID,BUILD svc
    class STATUS,RESP_MAP db
    class NULL_CHK user
    class MINIMAL stop
```

| Field Group | Source | Fields |
|-------------|--------|--------|
| **Campaign metadata** | `eventRequest` | `campaign_group`, `campaign_name`, `campaign_type`, `category`, `lob`, `si_id`, `moc` |
| **Timestamps** | `eventRequest` | `source_timestamp`, `start_timestamp`, `expiry_timestamp`, `timestamps_for_dashboard` |
| **Message content** | `eventRequest` | `script_body`, `script_sub`, `msg_lng_cd`, `sender_id` |
| **Tracking** | `eventRequest` | `uuid`, `circle_id`, `email_id`, `cohort`, `event_type`, `workspace_id` |
| **Lists** | `eventRequest` | `dynmc_prm`, `dynmc_prm_sub`, `additional_fields` |
| **Status** | set by caller | `status` = `SUCCESS` / `FAILED` |
| **Provider response** | `responsePayload` | `endpointRequestId` extracted per MOC, `response_data` map |
| **Error** | set by caller | `error_code`, `error_message` |
| **Service meta** | constant | `module_name` = `orchestrator-service`, `input_topic`, `event_date` |

`endpointRequestId` extraction per MOC:

| MOC | JSON path in response |
|-----|----------------------|
| `sms`, `whatsapp`, `rcs` | `response.messageReqId` |
| `email` | `response.data.messageId` |
| `push_bank`, `push_thanks` | `response.data.tid` |

---

## 12. Analytics Event Construction

`AnalyticsEventBuilder.buildAnalyticsEvent()` normalizes fields for the analytics pipeline:

| Analytics Field | Source / Logic |
|----------------|----------------|
| `channel` | `eventRequest.moc.toUpperCase()` — PUSH_BANK / PUSH_THANKS → `PUSH` |
| `communicationStatus` | Provider response `status` field per MOC → normalized to `sent / read / delivered / initiated / failed` |
| `campaignContentType` | `category` → `promotion / service / transactional` |
| `campaignType` | `event_type` → `event / onetime / recurring` |
| `errorType` | Inferred from `error_message` content → `system / validation / governance / customer / scrubbing / template & media error` |
| `smsPart` | From `eventRequest.smsPart` (set by VG after Base64 decode) |
| `hasURL` | From `eventRequest.hasUrl` |
| `tenantID` | `${app.cms.tenant-id}` |
| `workspaceID` | `eventRequest.workspace_id` |

---

## 13. Time Validation Logic

```mermaid
flowchart TD
    START["TimeValidationService.isExpired(payload)"]
    CHK_EN{app.time.validation\n.enabled = true?}
    SKIP_EN["return false\nvalidation disabled — skip"]
    EXTRACT["Extract expire_timestamp\nfrom eventRequest node"]
    CHK_BLANK{timestamp\nblank or null?}
    SKIP_BLANK["return false\nno expiry configured"]
    TRY_DT["Try 8 datetime formats in order:\nyyyy-MM-dd HH:mm:ss.SSSSSS\nyyyy-MM-dd HH:mm:ss.SSS\nyyyy-MM-dd HH:mm:ss\nyyyy-MM-dd'T'HH:mm:ss.SSSSSS\nyyyy-MM-dd'T'HH:mm:ss.SSS\nyyyy-MM-dd'T'HH:mm:ss\nyyyy/MM/dd HH:mm:ss\ndd-MM-yyyy HH:mm:ss"]
    TRY_DO["Try date-only formats:\nyyyy-MM-dd · yyyy/MM/dd · dd-MM-yyyy\n→ treat as end-of-day 23:59:59"]
    CHK_PARSE{parse\nsucceeded?}
    WARN_LOG["Log WARN: unparseable timestamp\nreturn false — treat as not expired"]
    NOW["now = LocalDateTime.now\n(ZoneId: Asia/Kolkata)"]
    CHK_EXP{now.isAfter\n(expiry)?}
    EXPIRED["return true — EXPIRED\n→ handleExpiredMessage():\n  CMS decrement if cmsRequired\n  publishErrorResponse(410)"]
    VALID["return false — VALID\n→ routeMessage()"]

    START --> CHK_EN
    CHK_EN -->|false| SKIP_EN
    CHK_EN -->|true| EXTRACT
    EXTRACT --> CHK_BLANK
    CHK_BLANK -->|blank| SKIP_BLANK
    CHK_BLANK -->|present| TRY_DT
    TRY_DT --> TRY_DO --> CHK_PARSE
    CHK_PARSE -->|failed all formats| WARN_LOG
    CHK_PARSE -->|succeeded| NOW
    NOW --> CHK_EXP
    CHK_EXP -->|"yes — now > expiry"| EXPIRED
    CHK_EXP -->|"no — now ≤ expiry"| VALID

    classDef kafka fill:#f39c12,color:#000,stroke:#d68910
    classDef svc fill:#2980b9,color:#fff,stroke:#1f618d
    classDef db fill:#27ae60,color:#fff,stroke:#1e8449
    classDef user fill:#34495e,color:#fff,stroke:#2c3e50
    classDef stop fill:#e74c3c,color:#fff,stroke:#c0392b

    class EXPIRED stop
    class VALID,SKIP_EN,SKIP_BLANK db
    class START,EXTRACT,TRY_DT,TRY_DO,NOW svc
    class CHK_EN,CHK_BLANK,CHK_PARSE,CHK_EXP user
    class WARN_LOG kafka
```

On expiry:
1. Check `cmsRequired` flag in `eventRequest`
2. If `true` → `cmsService.decrementQuota(payload, TIME_VALIDATION_FAILED)`
3. `responseKafkaPublisher.publishErrorResponse(payload, null, 410, TIME_VALIDATION_FAILED, ...)`

---

## 14. Kafka Topic Map

| Topic | Direction | Description | Producer / Consumer |
|-------|-----------|-------------|---------------------|
| `dispatch` | IN | Validated channel dispatch requests from Val-Gov | KafkaConsumer (group: `dispatch-request-consumer-group`) |
| `dispatch-response` | OUT | `FullKafkaDml` success events | FullDmlKafkaPublisher |
| `${error topic}` | OUT | `FullKafkaDml` failure events | FullDmlKafkaPublisher |
| `apb-success-log` | OUT | `ApbKafkaContract` success events | ApbKafkaPublisher |
| `apb-log` | OUT | `ApbKafkaContract` error events | ApbKafkaPublisher |
| `analytics` | OUT | `AnalyticsEventDTO` communication events | AnalyticsEventPublisher |
| `orchestrator-exceptions` | OUT | `ExceptionEvent` for deserialization/unhandled errors | ExceptionKafkaProducer |

---

## 15. Feign Clients

| Client | Target | Endpoint | Auth |
|--------|--------|----------|------|
| `SmsClient` | Airtel SMS IQ | `POST /api/v1/send-sms` | Basic (user:pass) |
| `SmsLobbyClient` | SMS Lobby | `GET/POST /cgi-bin/sendsms` | None |
| `NetcoreEmailClient` | Netcore CPaaS | `POST /v5.1/mail/send` | `x-api-key` header |
| `WhatsAppClient` | Airtel IQ WA | `POST /api/v2/message/nc` | Bearer token |
| `PushClient` | Firebase FCM | `POST /{endpoint}` | OAuth2 client creds |
| `RcsClient` | Airtel IQ Conversation | `POST /gateway/.../rcs/message/send` | Basic (user:pass) |
| `CmsClient` | CMS Service | `POST /counter/decrement` | Bearer token |

---

## 16. Exception Handling

```
Kafka DefaultErrorHandler (FixedBackOff 0L interval, 0 retries):
  → no retry on Kafka-level errors
  → ConsumerRecordRecoverer → ExceptionKafkaProducer.sendExceptionEventAsync()

Non-retryable exceptions (go straight to recoverer):
  - GenericException
  - DeserializationException
  - ConstraintViolationException
  - MethodArgumentNotValidException
  - RecordDeserializationException

handler.setCommitRecovered(true) → offset is committed even on recovery
```

`ExceptionEvent` fields: `exceptionType`, `code`, `message`, `stackTrace`, `originalPayload`

---

## 17. Data Models

### Input: ChannelDispatchRequest (consumed from `dispatch` topic)

| Field | Type | Description |
|-------|------|-------------|
| `eventRequest` | `EventRequest` | Full enriched event from upstream pipeline |
| `channelRequest` | `Object` | Channel-specific payload built by Val-Gov |
| `cmsRequest` | `CmsDecrementRequest` | Pre-built CMS decrement payload (from Val-Gov) |

### EventRequest (inner object)

| Field | Type | Description |
|-------|------|-------------|
| `uuid` | String | Unique message identifier |
| `moc` | String | Mode of communication: `sms / email / whatsapp / push_bank / push_thanks / rcs` |
| `si_id` | String | Subscriber identity (mobile number) |
| `email_id` | String | Target email address |
| `campaign_name` | String | Campaign name |
| `campaign_type` | String | Campaign goal type |
| `category` | String | `PROMOTION / TRANSACTIONAL / SERVICE / SERVICE_EXPLICIT` |
| `event_type` | String | `event / onetime / recurring` |
| `expire_timestamp` | String | Message expiry (multi-format, validated by TimeValidationService) |
| `source_timestamp` | String | Event creation timestamp from upstream |
| `sender_id` | String | Sender ID (used for WA account lookup) |
| `cmsRequired` | boolean | Whether CMS quota decrement is needed |
| `workspace_id` | String | Workspace / tenant identifier |
| `dynmc_prm` | `List<DynamicParam>` | Template personalisation parameters |
| `additional_fields` | `List<AdditionalFields>` | Extra key-value pairs (includes `template_id`) |
| `mediaAttachment` | `List<MediaAttachments>` | Media attachments (URL, MIME type, extension) |

### FullKafkaDml (output to success/error topics)

| Field | Type | Description |
|-------|------|-------------|
| `uuid` | String | Message UUID |
| `moc` | String | Channel name |
| `status` | String | `SUCCESS` / `FAILED` |
| `error_code` | String | HTTP status code string on failure |
| `error_message` | String | Human-readable error description |
| `endpoint_request_id` | String | Provider-assigned message ID |
| `response_data` | `Map<String, Object>` | Raw provider response keyed by MOC |
| `module_name` | String | `orchestrator-service` |
| `event_date` | String | `yyyy-MM-dd` of processing |
| `input_topic` | String | `dispatch` |
| `input_received` | String | Raw JSON of the input payload |

### ApbKafkaContract (output to apb topics)

| Field | Type | Description |
|-------|------|-------------|
| `data.si` | String | Subscriber identity |
| `data.channel` | String | Channel/category |
| `data.name` | String | Campaign name |
| `data.status` | String | `SUCCESS` / `FAILED` |
| `data.templateId` | String | Template identifier |
| `data.type` | String | Campaign type |
| `errors` | `List<Errors>` | Error code + message (empty on success) |
| `meta.app` | String | `orchestrator-service` |
| `meta.requestId` | String | Provider endpoint request ID |
| `meta.traceId` | String | Message UUID |
| `meta.timestamp` | String | ISO-8601 instant |

---

## 18. Error Types & Failure Reasons

| ErrorType | HTTP Status | FailureReason | Trigger |
|-----------|-------------|---------------|---------|
| `TIME_VALIDATION_FAILED` | 410 | `TIME_VALIDATION_FAILED` | `expire_timestamp` is in the past |
| `NETWORK_ERROR` | provider status | `PROVIDER_FAILED` | Provider API fails after all retries |
| `CHANNEL_DISABLED` | 503 | `NO_CHANNEL_ENABLED` | All channel flags are `false` |
| `REQUEST_PARSE_ERROR` | 4109 | — | Kafka message JSON parse failure |
| `EMPTY_REQUEST` | 4109 | — | `eventRequest` node is null/missing |

---

## 19. Component Map

| Class | Package | Responsibility |
|-------|---------|----------------|
| `KafkaConsumer` | `kafka` | Entry point; time validation; channel routing dispatcher |
| `TimeValidationService` | `service.impl` | Multi-format `expire_timestamp` parsing and expiry check |
| `BaseChannelService` | `service.impl` | Template pattern: retry orchestration, success/failure lifecycle |
| `SmsService` | `service.impl` | Payload transformation + SMS IQ dispatch via `SmsClient` |
| `EmailService` | `service.impl` | Multi-provider iteration (Netcore → SMTP) |
| `NetcoreEmailProvider` | `service.impl` | Netcore API implementation of `EmailProvider` |
| `SmtpEmailProvider` | `service.impl` | JavaMail SMTP implementation of `EmailProvider` |
| `WhatsAppService` | `service.impl` | Account lookup + WA API dispatch via `WhatsAppClient` |
| `PushService` | `service.impl` | FCM push dispatch; inner-response status parsing |
| `RcsService` | `service.impl` | MSISDN normalization + RCS API dispatch via `RcsClient` |
| `SmsLobbyService` | `service.impl` | Alternative SMS route via `SmsLobbyClient` |
| `CmsService` | `service.impl` | CMS quota decrement via `CmsClient` |
| `ChannelAccountResolver` | `service.impl` | Resolves sender account from Hazelcast lookup |
| `AccountSelectionStrategyFactory` | `service.impl` | Strategy selector: `SINGLE` or `ROUND_ROBIN` |
| `RoundRobinAccountStrategy` | `service.impl` | Thread-safe round-robin via `AtomicLong` counter |
| `SingleAccountStrategy` | `service.impl` | Always returns the first account |
| `InMemoryLookupService` | `service.impl` | Hazelcast `IMap` wrapper for domain-keyed lookups |
| `AccountContextHolder` | `service.impl` | `ThreadLocal<JsonNode>` for per-request account scoping |
| `ProviderRetryService` | `resilience` | Resilience4j `@Retry` wrapper for provider calls |
| `ProviderRetryPredicate` | `resilience` | Retry predicate: retries on 5xx and connection failures |
| `ResponseKafkaPublisher` | `kafka` | Orchestrates `FullDmlBuilder` → `PublishStrategy` → `Analytics` |
| `FullDmlBuilder` | `service.impl` | Maps `EventRequest` + provider response into `FullKafkaDml` |
| `ApbContractBuilder` | `service.impl` | Builds `ApbKafkaContract` from `FullKafkaDml` |
| `AnalyticsEventBuilder` | `service.impl` | Builds `AnalyticsEventDTO` with normalized status/channel/type |
| `AnalyticsEventPublisher` | `service.impl` | Async Kafka publish to analytics topic |
| `FullDmlKafkaPublisher` | `kafka` | Async publish of `FullKafkaDml` to success/error topics |
| `ApbKafkaPublisher` | `kafka` | Async publish of `ApbKafkaContract` to APB topics |
| `PublishStrategyFactory` | `service.impl` | Returns `FULL_ONLY` / `APB_ONLY` / `BOTH` strategy |
| `BothPublishStrategy` | `service.impl` | Delegates to both `FullDmlKafkaPublisher` + `ApbKafkaPublisher` |
| `ExceptionKafkaProducer` | `kafka` | Async publish of `ExceptionEvent` to exceptions topic |
| `KafkaErrorHandler` | `exception` | Builds `ExceptionEvent` from `ConsumerRecord` + exception |
| `AppConfig` | `config` | All feature flags and configuration binding |
| `KafkaConsumerConfiguration` | `config` | Consumer factory, error handler, listener container factory |
| `KafkaProducerConfiguration` | `config` | Producer factory and `KafkaTemplate` |
| `KafkaSecurityConfig` | `config` | Kerberos JAAS config for UAT/Prod |
| `HazelCastConfig` | `config` | Embedded Hazelcast instance configuration |
| `AccountRoutingProperties` | `config` | `multipleEnabled` + `strategy` binding |

---

## 20. Configuration Reference

| Property | Default | Description |
|----------|---------|-------------|
| `server.port` | `8080` | HTTP port (Actuator only — no REST API) |
| `app.kafka.topics.request` | `dispatch` | Input topic from Val-Gov |
| `app.kafka.topics.success.response` | `dispatch-response` | Success output topic |
| `app.kafka.topics.error.response` | _(profile-specific)_ | Error output topic |
| `app.kafka.topics.apb.response` | `apb-log` | APB error topic |
| `app.kafka.topics.apb.success` | `apb-success-log` | APB success topic |
| `analytics.kafka.topic` | `analytics` | Analytics output topic |
| `kafka.topic.exception` | `orchestrator-exceptions` | Exception forwarding topic |
| `kafka.consumer.group-id` | `dispatch-request-consumer-group` | Kafka consumer group |
| `kafka.consumer.concurrency` | `4` | Number of parallel consumer threads |
| `app.channels.sms-enabled` | `false` | Enable SMS IQ channel routing |
| `app.channels.email-enabled` | `false` | Enable Email channel routing |
| `app.channels.whatsapp-enabled` | `false` | Enable WhatsApp channel routing |
| `app.channels.push-enabled` | `true` | Enable Push channel routing |
| `app.channels.rcs-enabled` | `false` | Enable RCS channel routing |
| `app.channels.sms-lobby-enabled` | `false` | Enable SMS Lobby routing |
| `app.channels.email.netcore.enabled` | `true` | Use Netcore as email provider |
| `app.channels.email.smtp.enabled` | `false` | Use SMTP as email provider |
| `app.publish-mode` | `BOTH` | `FULL_ONLY` / `APB_ONLY` / `BOTH` |
| `app.time.validation.enabled` | `true` | Enable expire_timestamp check |
| `app.time.validation.timezone` | `Asia/Kolkata` | Timezone for expiry comparison |
| `app.retry.max-attempts` | `3` | Max provider retry attempts |
| `app.retry.delay-ms` | `1000` | Initial retry delay (ms) |
| `app.retry.multiplier` | `2.0` | Exponential backoff multiplier |
| `app.retry.max-delay-ms` | `10000` | Max retry delay cap (ms) |
| `app.cms.api.base-url` | _(profile-specific)_ | CMS service base URL |
| `app.cms.api.auth-token` | _(profile-specific)_ | Bearer token for CMS API |
| `app.cms.tenant-id` | _(profile-specific)_ | Tenant ID for analytics events |
| `account.details.file.name` | _(profile-specific)_ | Hazelcast domain key for account lookup |
| `account.routing.multiple-enabled` | `false` | Enable multi-account round-robin |
| `account.routing.strategy` | `SINGLE` | Account selection: `SINGLE` / `ROUND_ROBIN` |
