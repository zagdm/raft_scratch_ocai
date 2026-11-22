# Состояние кода и контракты ключевых классов (my-raft)

Актуальный снимок реализации (election-only Raft, Spring Boot 3.3.5, Java 21). Документ фиксирует **контракты ключевых классов в текущем коде**, включая ответственность за атомарность/конкурентность.

## Стек и структура
- Java 21, Spring Boot 3.3.5, Maven (`spring-boot-starter-web`, `spring-boot-configuration-processor`, `spring-boot-starter-test`).
- Основные пакеты: `api`, `config`, `raft.model`, `raft.rpc`, `raft.rpc.dto`, `raft.storage`, `raft.timer`, `raft` (`RaftNode`).
- Конфигурация (`application.yml`, по умолчанию): `raft.election-timeout-min-ms=5000`, `raft.election-timeout-max-ms=8000`, `raft.heartbeat-interval-ms=1000`, `raft.storage-path=data/raft-state.json`, `RestTemplate` connect/read `200ms`.
- Тесты: `mvn test` — 19 тестов, BUILD SUCCESS (`MyRaftApplicationTests` (`@SpringBootTest` contextLoads), `StatusResponseJsonTest`, `RaftElectionTest`, `RaftNodeElectionTest`, `RaftStateAtomicityTest`, `raft.rpc.RaftRpcOutcomeTest`, `raft.rpc.RaftRpcServiceAtomicityTest`, `FileRaftStorageTest`).

## Контракты ключевых классов

### `raft.model.RaftState` (`src/main/java/com/myraft/raft/model/RaftState.java`)
**Назначение:** единый источник in-memory состояния узла (`currentTerm`, `votedFor`, `role`, `leaderId`, `startTime`) и точка атомарных переходов для персистентной пары term/vote.

**Контракт:**
- При создании загружает `PersistedState` из `RaftStorage` (`load()`), инициализируя `currentTerm/votedFor`.
- `startElection(String selfId): int` — **атомарно** (под `writeLock`): `newTerm = currentTerm + 1`, `storage.persist(newTerm, selfId)`, затем публикация `currentTerm=newTerm`, `votedFor=selfId`, возврат `newTerm`. **Persist-before-visible.**
- `grantVote(int candidateTerm, String candidateId): GrantVoteResult(granted, termIncreased, term)` — **атомарно** (под `writeLock`) решает выдачу голоса:
  - `candidateTerm < currentTerm` → отказ, терм не меняется;
  - `candidateTerm > currentTerm` → `persist(candidateTerm, candidateId)`, обновление `currentTerm/votedFor`, `granted=true, termIncreased=true`;
  - `candidateTerm == currentTerm` и `votedFor == null || votedFor.equals(candidateId)` → `persist(currentTerm, candidateId)`, установка/подтверждение `votedFor`, `granted=true`;
  - иначе отказ. **Persist-before-visible** при изменении/фиксации голоса; не более одного голоса в терме.
- `updateTermAndVote(int term, String votedFor): void` — **монотонная атомарная операция** (под `writeLock`): если `term < currentTerm` — no-op без изменения `votedFor`; иначе `persist(term, votedFor)` затем публикация полей.
- `snapshot(): RaftStateSnapshot(currentTerm, votedFor, role, leaderId, startTime)` — согласованный снимок всех полей под одним `readLock`; используется для логирования решений, `/status` и проверок «до/после». Внутренний лок (`getLock()`) наружу **не** экспонируется, чтобы не создавать обходной порядок блокировок.
- `getCurrentTerm/getVotedFor/getRole/getLeaderId` — чтения под `readLock`; `setRole/setLeaderId` — записи под `writeLock` (но **отдельно** от term/vote-переходов).

**Ответственность за атомарность:** отвечает за атомарность и монотонность **только** пары `currentTerm/votedFor` и их персистентной фиксации относительно видимости. **Не** обеспечивает атомарность составных переходов узла (`term/vote` + `role` + `leaderId` + таймеры + heartbeats) — их сериализует `RaftNode` через `nodeLock`.

### `raft.storage.RaftStorage` и реализации
**`RaftStorage` (`raft/storage/RaftStorage.java`) контракт:**
- `load()` — возвращает сохранённое состояние или `PersistedState.EMPTY` при отсутствии.
- `persist(int term, String votedFor)` — сохраняет персистентную часть состояния; по контракту запись должна завершиться (данные зафиксированы) до ответа на RPC/отправки RPC, меняющего терм.

**`FileRaftStorage` (`raft/storage/FileRaftStorage.java`, bean только при явном `raft.storage.type=file`, `matchIfMissing=false`):**
- `load()`: при отсутствии файла → `EMPTY`; при ошибке чтения/парсинга → `IllegalStateException` (запрет тихого старта с term 0).
- `persist(...)`: `synchronized` на экземпляре, запись в temp-файл, `force(true)`, atomic move (`ATOMIC_MOVE` с fallback на `REPLACE_EXISTING`). **Ответственен за атомарность/долговечность файловой записи в рамках одного процесса** (насколько позволяет ФС).

**`InMemoryRaftStorage` (`raft/storage/InMemoryRaftStorage.java`, bean при `raft.storage.type=memory` или при отсутствии свойства — режим по умолчанию, `matchIfMissing=true`):**
- Хранит `volatile PersistedState`; `load/persist` — `synchronized`. **Не обеспечивает персистентность между перезапусками**, обеспечивает потокобезопасный доступ в памяти.

### `raft.RaftNode` (`src/main/java/com/myraft/raft/RaftNode.java`)
**Назначение:** единственная точка принятия решений и мутаций состояния узла (старт, таймаут выборов, выборы, лидерство, heartbeats, обработка входящих RPC и ответов на исходящие RPC).

**Контракт:**
- **Единый `nodeLock`** (`private final Object`) сериализует все переходы узла: `role`/`leaderId`, election timer, запуск/остановку heartbeats, обработку входящих RPC и колбэков ответов.
- `init()` (`@PostConstruct`): под `nodeLock` переводит узел в `FOLLOWER`, сбрасывает `leaderId`, регистрирует callback election timer и запускает таймер.
- `handleRequestVote(RequestVoteRequest): RequestVoteOutcome` — под `nodeLock`: снимает `before`-снапшот, вызывает `RaftState.grantVote(...)`, применяет side-effects (`FOLLOWER`/`leaderId=null` при росте терма, иначе `reset()` таймера при **первом** голосе); событие узла — `GRANTED`, `GRANTED_TERM_ADVANCED`, `REJECTED_STALE_TERM`, `REJECTED_ALREADY_VOTED`. Повторный голос тому же кандидату возвращается как `GRANTED` и классифицируется в `GRANTED_REPEAT` сервисом; узел в этом случае таймер не трогает. **Не логирует** и не трогает MDC.
- `resetElectionTimerIfUnchanged(RaftStateSnapshot expected)` — под `nodeLock` сравнивает текущий снапшот с `after` из outcome (term/votedFor/role) и только при совпадении сбрасывает election timer. Используется сервисом для эффекта `GRANTED_REPEAT` без прямого доступа к таймеру и без сброса после параллельного step-down/смены роли.
- `handleAppendEntries(AppendEntriesRequest): AppendEntriesOutcome` — под `nodeLock`: `before`-снапшот; `request.term < currentTerm` → `REJECTED_STALE_TERM` без мутаций; **равный терм и уже известный `leaderId != null`, отличный от `request.leaderId` → `REJECTED_FOREIGN_LEADER` без мутаций**; иначе монотонный `updateTermAndVote(term, null)` при росте терма, `becomeFollowerLocked(leaderId, true)`; событие `ACCEPTED_TERM_ADVANCED` / `ACCEPTED_LEADER_DISCOVERED` / `ACCEPTED_IDEMPOTENT` (первые два приоритетнее). **Не логирует**.
- `onElectionTimeout()`: логирует `election timeout`, затем `startElection()`; если узел `LEADER` — только reset таймера.
- `startElection()`:
  - под `nodeLock`: проверка «не лидер», `raftState.startElection(nodeId)`, затем проверка (1) — терм и self-vote всё ещё актуальны; установка `role=CANDIDATE`, `leaderId=null`, логирование `starting election`, reset таймера;
  - если кворум уже достигнут (single-node) — `tryBecomeLeaderLocked` под тем же локом;
  - рассылка `RequestVote(newTerm, nodeId)` пирам — **вне `nodeLock`**.
- `handleRequestVoteResponse(...)`: под `nodeLock`; при `resp.term > currentTerm` — step-down (`updateTermAndVote`, `FOLLOWER`, `leaderId=null`, stop heartbeats, reset timer); ответ учитывается только если терм совпадает с термом кампании, узел `CANDIDATE` и голосовал за себя (проверка (4)); при кворуме — `tryBecomeLeaderLocked`.
- `tryBecomeLeaderLocked(...)` (проверка (2)): переводит в `LEADER` только если кворум собран **и** узел всё ещё `CANDIDATE` в ожидаемом терме с self-vote; логирует `became leader`, запускает heartbeats.
- `startHeartbeatsLocked()/stopHeartbeatsLocked()`: управляют единичной задачей в `raft-heartbeat` (`scheduleWithFixedDelay`, интервал `heartbeatIntervalMs`); вызываются только под `nodeLock`.
- `sendHeartbeats()`: под `nodeLock` проверяет (5) актуальность лидерства/терма, фиксирует `AppendEntries(currentTerm, nodeId)` и список пиров, иначе останавливает heartbeats; логирует `AppendEntries sent` и рассылает RPC **вне `nodeLock`**.
- `handleAppendEntriesResponse(...)`: под `nodeLock`; ответ учитывается только для актуального лидерства/терма (проверка (5)); при `resp.term > currentTerm` — step-down.
- `shutdown()` (`@PreDestroy`): под `nodeLock` останавливает таймер, heartbeats и `heartbeatScheduler`.

**Ответственность за атомарность/конкурентность:** обеспечивает атомарность **составных** переходов узла в рамках одного процесса: решение и все его side-effects (`role`, `leaderId`, timer, heartbeats) выполняются под `nodeLock`. Порядок блокировок: **`nodeLock` → лок `RaftState`**; обратный порядок запрещён (внутренний лок `RaftState` не экспонируется). Сетевые вызовы выполняются вне `nodeLock`, поэтому медленный/зависший пир не блокирует обработку входящих RPC и таймеры; актуальность контекста после сети проверяется повторно под локом.

### `raft.model` outcome/event-типы
- `StateTransition(before, after)` — пара `RaftStateSnapshot`, описывающая ровно один переход.
- `RequestVoteOutcome(event, state)` / `AppendEntriesOutcome(event, state)` — решение узла + переход; `granted()`/`accepted()` — производные предикаты для формирования ответа.
- `RequestVoteEvent`: `GRANTED`, `GRANTED_REPEAT` (`before.votedFor == candidate` при равном терме; определяется **сервисом**), `GRANTED_TERM_ADVANCED`, `REJECTED_STALE_TERM`, `REJECTED_ALREADY_VOTED`.
- `AppendEntriesEvent`: `ACCEPTED_TERM_ADVANCED` (приоритет), `ACCEPTED_LEADER_DISCOVERED` (смена роли и/или `leaderId`), `ACCEPTED_IDEMPOTENT`, `REJECTED_STALE_TERM`, `REJECTED_FOREIGN_LEADER` (равный терм, другой лидер). `accepted()` истинно только для `ACCEPTED_*`.

### `raft.rpc.RaftRpcService` (`src/main/java/com/myraft/raft/rpc/RaftRpcService.java`)
**Назначение:** тонкий адаптер «входящий RPC → `RaftNode` → лог/ответ». Состояние не мутирует напрямую; единственный побочный эффект — сброс election timer через узел при `GRANTED_REPEAT`.

**Контракт:**
- Конструктор (единственный, для DI): `(RaftNode, RaftState, RaftProperties)`.
- `handleRequestVote(RequestVoteRequest)`:
  - снимает `pre`-снапшот для MDC;
  - **prefilter** (синтетический outcome `StateTransition(pre, pre)`, узел не вызывается): `request.term < pre.currentTerm` → `REJECTED_STALE_TERM`; либо `request.term == pre.currentTerm && pre.votedFor != null && pre.votedFor != request.candidateId` → `REJECTED_ALREADY_VOTED`;
  - иначе вызывает `raftNode.handleRequestVote(request)`;
  - уточняет событие: если узел вернул `GRANTED` и `before.votedFor == candidate`, event заменяется на `GRANTED_REPEAT` и вызывается `raftNode.resetElectionTimerIfUnchanged(after)` (таймер сбрасывается под `nodeLock`, только если состояние всё ещё совпадает с outcome);
  - переключает MDC на `after`-снапшот, логирует событие, собирает `RequestVoteResponse(after.currentTerm(), outcome.granted())`;
  - живое состояние после вызова узла **не перечитывается** (кроме защитной проверки в методе узла при `GRANTED_REPEAT`).
- `handleAppendEntries(AppendEntriesRequest)`:
  - **prefilter**: `request.term < pre.currentTerm` → `REJECTED_STALE_TERM`; либо `request.term == pre.currentTerm && pre.leaderId != null && pre.leaderId != request.leaderId` → `REJECTED_FOREIGN_LEADER`;
  - иначе вызывает `raftNode.handleAppendEntries(request)` (авторитетная проверка чужого лидера под `nodeLock`);
  - логирует `AppendEntries received`, ответ из `after.currentTerm()` и `outcome.accepted()`.
- Логи по событию (MDC `nodeId/term/role` из `after`):
  - `GRANTED` / `GRANTED_REPEAT` → `vote granted (...)`;
  - `GRANTED_TERM_ADVANCED` → `stepping down (incoming RequestVote has higher term)` + `vote granted (to candidate) ... (was N)`;
  - `REJECTED_STALE_TERM` → DEBUG `vote rejected (stale term, ...)`;
  - `REJECTED_ALREADY_VOTED` → `vote rejected (already voted / condition failed)`;
  - `ACCEPTED_TERM_ADVANCED` → `stepping down (incoming AppendEntries has higher term)`;
  - `ACCEPTED_LEADER_DISCOVERED` → `discovered new leader` (если роль была не `FOLLOWER`) и/или `leader changed to ...` (если сменился `leaderId`);
  - `ACCEPTED_IDEMPOTENT` → DEBUG `heartbeat accepted (no state change)`;
  - `REJECTED_STALE_TERM` → DEBUG `heartbeat ignored (incoming AppendEntries stale term)`;
  - `REJECTED_FOREIGN_LEADER` → DEBUG `heartbeat ignored (foreign leader at same term) from=... currentLeader=...`.
- **Persist-before-respond** обеспечивается `RaftState` (persist выполняется внутри решения до возврата outcome). При исключении `storage.persist` событие не логируется, ответ не формируется — исключение уходит наверх (HTTP 500).
- Журнал/entries не валидируются и не применяются (heartbeat/election-only).

**Ответственность за атомарность/конкурентность:** сервис состояния не защищает и не сериализует; весь переход (решение + side-effects + снапшоты) атомарен внутри `RaftNode` под `nodeLock`; сброс таймера по `GRANTED_REPEAT` также выполняется узлом под `nodeLock` с re-check'ом против `after`. MDC выставляется/очищается в потоке HTTP-запроса.

### `raft.rpc.RaftRpcClient` (`src/main/java/com/myraft/raft/rpc/RaftRpcClient.java`)
**Назначение:** исходящие HTTP RPC к пирам.

**Контракт:**
- `requestVote(peer, request)` / `appendEntries(peer, request)` выполняют POST (`/raft/requestVote`, `/raft/appendEntries`) через `RestTemplate` в `CompletableFuture.supplyAsync(...)` **без явного executor**.
- При `RestClientException` логирует и возвращает `null` (ошибка сети/пира не пробрасывается как успешный ответ).
- `getPeers()` возвращает список пиров из `ClusterProperties`.

**Ответственность за атомарность/конкурентность:** не синхронизирует состояние узла; асинхронно выполняет сетевые вызовы и порождает конкурирующие completion callbacks, которые должны вызывать безопасную логику обработки ответов.

### `raft.rpc.RaftRpcController` (`src/main/java/com/myraft/raft/rpc/RaftRpcController.java`)
**Назначение:** HTTP-адаптер входящих Raft RPC.

**Контракт:**
- `POST /raft/requestVote` → `rpcService.handleRequestVote(request)`.
- `POST /raft/appendEntries` → `rpcService.handleAppendEntries(request)`.
- Не содержит собственной синхронизации/очередизации; параллелизм запросов определяется web-контейнером.

### `api.StatusController` (`src/main/java/com/myraft/api/StatusController.java`)
**Назначение:** диагностический HTTP endpoint.

**Контракт:**
- `GET /status` формирует `StatusResponse(nodeId, role, currentTerm, leaderId, peers, isLeader, uptime)` из **одного** `RaftState.snapshot()` (read-lock); `nodeLock` не захватывается.
- `MDC` (`nodeId/term/role`) выставляется из снапшота на время обработки запроса.
- Не выполняет мутаций состояния.

**Ответственность за атомарность/конкурентность:** набор полей `RaftState` согласован между собой (один `readLock`); `peers` берутся из статической конфигурации. `nodeLock` не участвует, поэтому `/status` не блокирует Raft-переходы и не может вызвать инверсию блокировок.

### `raft.timer.ElectionTimer` (`src/main/java/com/myraft/raft/timer/ElectionTimer.java`)
**Назначение:** управление таймаутом выборов.

**Контракт:**
- Однопоточный планировщик `raft-election-timer`; таймаут случайный в `[electionTimeoutMinMs, electionTimeoutMaxMs]`.
- `start/reset` планируют новый таймаут, отменяя предыдущий; использует `epoch` (`AtomicLong` + `currentEpoch`) и `synchronized(lock)` для отбраковки устаревших срабатываний.
- `onTimeout` вызывает зарегистрированный callback только если `scheduledEpoch == currentEpoch`.
- `stop()` отменяет задачу и инвалидирует текущую эпоху.

**Ответственность за атомарность/конкурентность:** сериализует управление собственным расписанием и фильтрацию устаревших таймаутов; **не** атомарен совместно с переходами `RaftNode/RaftState`.

### Конфигурация и DTO (ключевые свойства)
- `RaftProperties` (`config/RaftProperties.java`): параметры узла/таймингов/пути хранения (значения по умолчанию в классе могут отличаться от `application.yml`; фактические runtime-значения задаются конфигурацией).
- `ClusterProperties`: список пиров (фильтрация пустых).
- `RestTemplateConfig`: таймауты HTTP-клиента `200ms/200ms`.
- DTO RPC: `RequestVoteRequest(term, candidateId, lastLogIndex, lastLogTerm)`, `RequestVoteResponse(term, voteGranted)`, `AppendEntriesRequest(term, leaderId, entries...)`, `AppendEntriesResponse(term, success)` — структуры данных без логики синхронизации.

## Границы ответственности по атомарности (сводно)
- **Атомарно и с persist-before-visible:** инкремент терма/выдача голоса/монотонное обновление `currentTerm,votedFor` (`RaftState` + `RaftStorage.persist`).
- **Атомарно в рамках процесса (под `nodeLock`):** составные переходы узла `FOLLOWER/CANDIDATE/LEADER` + `leaderId` + election timer + запуск/остановка heartbeats + обработка входящих RPC и колбэков ответов; решение и его side-effects логируются из `before/after`-снапшотов этого перехода.
- **Вне `nodeLock`:** сетевые вызовы (`RaftRpcClient`), построение `/status` (только `RaftState.snapshot()`).
- **Порядок блокировок:** `nodeLock` → лок `RaftState`; обратный порядок запрещён (внутренний лок `RaftState` не экспонируется). `RaftRpcClient` не удерживает ни один из локов во время HTTP-вызова.
- **Источники конкуренции:** HTTP-запросы (`/raft/*`, `/status`), `raft-election-timer`, `raft-heartbeat`, асинхронные RPC-вызовы/колбэки (`CompletableFuture.supplyAsync`).

## Логирование решений (RPC outcome logging)
- Входящие RPC: `RaftNode` возвращает outcome, `RaftRpcService` логирует `vote granted/stepping down/leader changed/...` и формирует ответ из `after.currentTerm()`.
- Исходящие: `starting election`, `RequestVote sent term=... peers=N` (одна строка на раунд выборов в `RaftNode`; per-peer строки `RaftRpcClient` — DEBUG), `vote granted (n/m) from peer ... from=<peer>`, `became leader`, `stepping down (outgoing ... higher term)`, `AppendEntries sent` (раз в раунд на лидере).
- MDC (`nodeId/term/role`) выставляется в точке логирования из снапшота: async-колбэки и heartbeat-поток не наследуют MDC HTTP-потока.
- Решение и его side-effects не логируются повторным чтением живого состояния — только из `StateTransition`.

## Оставшиеся ограничения реализации
- Нет репликации лога/FSM, commit/apply, снапшотов, pre-vote, динамической конфигурации.
- Персистируются только `currentTerm/votedFor`; `role/leaderId` не восстанавливаются.
- `AppendEntries` не проверяет согласованность лога; при валидном терме и известном/том же лидере возвращает `success=true`.
- При равном терме heartbeat принимается только от уже известного лидера (`leaderId == null` или совпадает); от другого лидера — `REJECTED_FOREIGN_LEADER`. Проверка относится только к терму/лидеру, не к содержимому журнала.
- `nodeLock` сериализует переходы только внутри одного процесса; корректность между узлами по-прежнему опирается на Raft-инварианты терма/голоса.
