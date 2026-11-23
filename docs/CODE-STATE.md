# Состояние кода и контракты ключевых классов (my-raft)

Актуальный снимок реализации (election-only Raft + этап 1 модели лога/персистентности/условий
согласованности + этап 2 абстрактной state machine и KV-конкретизации, Spring Boot 3.3.5,
Java 21). Документ фиксирует **контракты ключевых классов в текущем коде**, включая
ответственность за атомарность/конкурентность. План дальнейшей репликации —
`docs/LOG-REPLICATION-PLAN.md`.

## Стек и структура
- Java 21, Spring Boot 3.3.5, Maven (`spring-boot-starter-web`, `spring-boot-configuration-processor`, `spring-boot-starter-test`).
- Основные пакеты: `api`, `config`, `raft.model`, `raft.fsm`, `raft.fsm.kv`, `raft.rpc`, `raft.rpc.dto`, `raft.storage`, `raft.timer`, `raft` (`RaftNode`).
- Конфигурация (`application.yml`, по умолчанию): `raft.election-timeout-min-ms=5000`, `raft.election-timeout-max-ms=8000`, `raft.heartbeat-interval-ms=1000`, `raft.storage-path=data/raft-state.json`, `RestTemplate` connect/read `200ms`.
- Тесты: `mvn test` — 50 тестов, BUILD SUCCESS (19 исходных + 16 этапа 1 + 15 этапа 2): `MyRaftApplicationTests` (`@SpringBootTest` contextLoads), `StatusResponseJsonTest`, `RaftElectionTest`, `RaftNodeElectionTest`, `RaftStateAtomicityTest`, `raft.RaftNodeApplyTest`, `raft.rpc.RaftRpcOutcomeTest`, `raft.rpc.RaftRpcServiceAtomicityTest`, `FileRaftStorageTest`, `raft.model.RaftLogTest`, `raft.model.RaftCommandJsonTest`, `raft.fsm.kv.KvStateMachineTest`, `raft.rpc.RaftVoteFreshnessTest`, `raft.rpc.dto.RaftDtoJsonTest`.

## Контракты ключевых классов

### `raft.fsm` / `raft.model`: лог, конверт команд и state machine (этапы 1–2)
- `raft.fsm.StateMachine<R>` — абстракция детерминированной SM: `ApplyResult<R> apply(long index, Command command)`; `snapshot()/restore(Snapshot)` — future-default (`UnsupportedOperationException`), не реализуются. Требования к concrete SM: детерминизм, идемпотентность по индексу, быстрый apply без сетевых/дисковых побочных эффектов под `nodeLock`.
- `raft.fsm.Command` — **открытый** маркер пользовательских команд с `@JsonTypeInfo(property = "type")`, без `@JsonSubTypes` в ядре. Concrete-типы регистрируются внешним `KvCommandsModule` (Jackson `SimpleModule`, `PUT → PutCommand`); неизвестный `type` → ошибка десериализации. Ядро не знает KV-семантику.
- `raft.fsm.ApplyResult<R>(previousValue, value, index)` — результат apply; ядро хранит его как непрозрачный `ApplyResult<?>`.
- `raft.fsm.kv.KvStateMachine implements StateMachine<JsonNode>` — первая конкретизация: `Map<String, JsonNode>` (ConcurrentHashMap), `get(key)` для чтения. `PutCommand(key, JsonNode value)` (`type=PUT`) отвергает пустой `key`, Java `null` нормализуется в `NullNode`; неизвестная команда → `IllegalArgumentException`. KV-специфичный `key` логируется только в concrete SM.
- `raft.model.RaftCommand` — sealed-конверт лога: `UserCommand(Command payload)` (`type=USER`) и `NoOpCommand` (`type=NOOP`). `NoOpCommand` — Raft-уровневый маркер, **не команда SM**, до `StateMachine.apply` не доходит. Формат этапа 1 (`type=PUT/NOOP` напрямую) не поддерживается.
- `LogEntry(index, term, command)` — неизменяемая запись лога; индексы начинаются с 1, `term >= 0`, `command != null`, тип `command` — `RaftCommand`.
- `RaftLog(entries)` — неизменяемое представление лога с проверкой непрерывности индексов:
  - `lastIndex()` / `lastTerm()` — 0 для пустого лога;
  - `termAt(index)` / `entryAt(index)` — 0 / `null` для отсутствующих индексов;
  - `append(entry)` и `truncateFrom(index)` возвращают **новый** `RaftLog` (Persist-before-visible);
  - `isAtLeastAsUpToDate(candidateLastLogTerm, candidateLastLogIndex, local...)` — правило §5.4.1: сначала `lastLogTerm`, при равенстве `lastLogIndex`.

### `raft.model.RaftState` (`src/main/java/com/myraft/raft/model/RaftState.java`)
**Назначение:** единый источник in-memory состояния узла (`currentTerm`, `votedFor`, `role`, `leaderId`, `startTime`, `raftLog`, volatile `commitIndex`/`lastApplied`) и точка атомарных переходов для unified персистентного состояния `{formatVersion, term, votedFor, log[]}`.

**Контракт:**
- При создании загружает `PersistedState` из `RaftStorage` (`load()`), инициализируя `currentTerm/votedFor/raftLog`. `commitIndex/lastApplied` всегда начинаются с 0 (volatile, не персистируются).
- `startElection(String selfId): int` — **атомарно** (под `writeLock`): `newTerm = currentTerm + 1`, `persist(PersistedState.of(newTerm, selfId, log))`, затем публикация `currentTerm=newTerm`, `votedFor=selfId`, возврат `newTerm`. **Persist-before-visible.**
- `grantVote(int candidateTerm, String candidateId): GrantVoteResult(granted, termIncreased, term)` — **атомарно** (под `writeLock`) решает выдачу голоса:
  - `candidateTerm < currentTerm` → отказ, терм не меняется;
  - `candidateTerm > currentTerm` → `persist(candidateTerm, candidateId, log)`, обновление `currentTerm/votedFor`, `granted=true, termIncreased=true`;
  - `candidateTerm == currentTerm` и `votedFor == null || votedFor.equals(candidateId)` → `persist(currentTerm, candidateId, log)`, установка/подтверждение `votedFor`, `granted=true`;
  - иначе отказ. **Persist-before-visible** при изменении/фиксации голоса; не более одного голоса в терме.
- `updateTermAndVote(int term, String votedFor): void` — **монотонная атомарная операция** (под `writeLock`): если `term < currentTerm` — no-op без изменения `votedFor`; иначе `persist(term, votedFor, log)` затем публикация полей.
- `appendLogEntry(LogEntry)` / `truncateLogFrom(long index)` — под `writeLock`: строят новый `RaftLog`, фиксируют unified состояние в хранилище и только затем публикуют ссылку на лог. Сбой записи логируется как `log persist failed` и пробрасывается наверх.
- `lastLogIndex()/lastLogTerm()/logSnapshot()` — чтения лога под `readLock`; `logSnapshot()` возвращает неизменяемый `RaftLog` и предназначен для чтения под `nodeLock` без вложенных локов.
- `snapshot(): RaftStateSnapshot(currentTerm, votedFor, role, leaderId, startTime, lastLogIndex, lastLogTerm)` — согласованный снимок всех полей под одним `readLock`; используется для логирования решений, `/status` и проверок «до/после». Внутренний лок (`getLock()`) наружу **не** экспонируется, чтобы не создавать обходной порядок блокировок.
- `getCurrentTerm/getVotedFor/getRole/getLeaderId` — чтения под `readLock`; `setRole/setLeaderId` — записи под `writeLock` (но **отдельно** от term/vote-переходов).
- `advanceCommitIndex(long)` / `getCommitIndex()` и `advanceLastApplied(long)` / `getLastApplied()` — монотонные volatile-указатели (под тем же `readLock`/`writeLock`). `commitIndex` не может опережать конец лога; **не персистируются** (volatile по Figure 2). Продвижение по большинству — этап 3.

**Ответственность за атомарность:** отвечает за атомарность и монотонность **unified персистентной части** `currentTerm/votedFor/log[]` и её фиксации относительно видимости (persist-before-visible). **Не** обеспечивает атомарность составных переходов узла (`term/vote/log` + `role` + `leaderId` + таймеры + heartbeats + apply) — их сериализует `RaftNode` через `nodeLock`.

### `raft.storage.RaftStorage` и реализации
**`RaftStorage` (`raft/storage/RaftStorage.java`) контракт:**
- `load()` — возвращает сохранённое состояние или `PersistedState.EMPTY` при отсутствии.
- `persist(PersistedState state)` — атомарно сохраняет unified `{formatVersion, term, votedFor, log[]}`; по контракту запись должна завершиться (данные зафиксированы) до ответа на RPC/отправки RPC, меняющего терм. `PersistedState.of(term, votedFor, log)` подставляет `CURRENT_FORMAT_VERSION = 2`.

**`FileRaftStorage` (`raft/storage/FileRaftStorage.java`, bean только при явном `raft.storage.type=file`, `matchIfMissing=false`):**
- `load()`: при отсутствии файла → `EMPTY`; при ошибке чтения/парсинга → `IllegalStateException` (запрет тихого старта с term 0). Отсутствующая/старая/неизвестная `formatVersion` → fail-fast `unsupported raft state format`; legacy-reader для этапа 1 отсутствует (persisted-логов нет).
- `persist(...)`: `synchronized` на экземпляре, сериализация unified `PersistedState` через внедрённый Spring `ObjectMapper` (в нём зарегистрирован `KvCommandsModule` для полиморфных команд), запись в temp-файл, `force(true)`, atomic move (`ATOMIC_MOVE` с fallback на `REPLACE_EXISTING`). **Ответственен за атомарность/долговечность файловой записи в рамках одного процесса** (насколько позволяет ФС).

**`InMemoryRaftStorage` (`raft/storage/InMemoryRaftStorage.java`, bean при `raft.storage.type=memory` или при отсутствии свойства — режим по умолчанию, `matchIfMissing=true`):**
- Хранит `volatile PersistedState`; `load/persist` — `synchronized`. **Не обеспечивает персистентность между перезапусками**, обеспечивает потокобезопасный доступ в памяти.

### `raft.RaftNode` (`src/main/java/com/myraft/raft/RaftNode.java`)
**Назначение:** единственная точка принятия решений и мутаций состояния узла (старт, таймаут выборов, выборы, лидерство, heartbeats, обработка входящих RPC и ответов на исходящие RPC).

**Контракт:**
- Конструктор для DI: `(RaftState, RaftProperties, RaftRpcClient, ElectionTimer, StateMachine<?>)`; KV — только одна из конкретизаций, ядро работает с абстракцией.
- **Единый `nodeLock`** (`private final Object`) сериализует все переходы узла: `role`/`leaderId`, election timer, запуск/остановку heartbeats, обработку входящих RPC, apply и колбэки ответов.
- `init()` (`@PostConstruct`): под `nodeLock` переводит узел в `FOLLOWER`, сбрасывает `leaderId`, регистрирует callback election timer и запускает таймер.
- `handleRequestVote(RequestVoteRequest): RequestVoteOutcome` — под `nodeLock`: снимает `before`-снапшот; при `term < currentTerm` — `REJECTED_STALE_TERM`; затем проверяет свежесть лога по §5.4.1 (`lastLogTerm`, затем `lastLogIndex`) и при более свежем локальном логе возвращает `REJECTED_LOG_NOT_UP_TO_DATE` (при `term > currentTerm` сначала монотонно обновляет term/vote через `updateTermAndVote(term, null)` и делает step-down, **не выдавая голос**); иначе вызывает `RaftState.grantVote(...)` и применяет side-effects (`FOLLOWER`/`leaderId=null` при росте терма, иначе `reset()` таймера при **первом** голосе). События узла: `GRANTED`, `GRANTED_TERM_ADVANCED`, `REJECTED_STALE_TERM`, `REJECTED_ALREADY_VOTED`, `REJECTED_LOG_NOT_UP_TO_DATE`. Повторный голос тому же кандидату возвращается как `GRANTED` и классифицируется в `GRANTED_REPEAT` сервисом; узел в этом случае таймер не трогает. **Не логирует** и не трогает MDC.
- `resetElectionTimerIfUnchanged(RaftStateSnapshot expected)` — под `nodeLock` сравнивает текущий снапшот с `after` из outcome (term/votedFor/role) и только при совпадении сбрасывает election timer. Используется сервисом для эффекта `GRANTED_REPEAT` без прямого доступа к таймеру и без сброса после параллельного step-down/смены роли.
- `handleAppendEntries(AppendEntriesRequest): AppendEntriesOutcome` — под `nodeLock`: `before`-снапшот; `request.term < currentTerm` → `REJECTED_STALE_TERM` без мутаций; **равный терм и уже известный `leaderId != null`, отличный от `request.leaderId` → `REJECTED_FOREIGN_LEADER` без мутаций**; иначе монотонный `updateTermAndVote(term, null)` при росте терма, `becomeFollowerLocked(leaderId, true)`; событие `ACCEPTED_TERM_ADVANCED` / `ACCEPTED_LEADER_DISCOVERED` / `ACCEPTED_IDEMPOTENT` (первые два приоритетнее). **Не логирует**.
- `applyCommittedEntries()` / `appliedResult(index)` (этап 2): под `nodeLock` применяют записи `(lastApplied, commitIndex]` строго по индексу. Для `UserCommand` вызывается `StateMachine.apply(index, payload)`, результат сохраняется в `Map<Long, ApplyResult<?>>` для будущих клиентов; `NoOpCommand` только продвигает `lastApplied` (SM не вызывается). При отсутствии новых записей — DEBUG `apply skipped`. Повторный вызов ничего не меняет; исключение apply оставляет запись неприменённой. Продвижение `commitIndex` по большинству — этап 3.
- `onElectionTimeout()`: логирует `election timeout`, затем `startElection()`; если узел `LEADER` — только reset таймера.
- `startElection()`:
  - под `nodeLock`: проверка «не лидер», `raftState.startElection(nodeId)`, затем проверка (1) — терм и self-vote всё ещё актуальны; установка `role=CANDIDATE`, `leaderId=null`, логирование `starting election`, reset таймера;
  - если кворум уже достигнут (single-node) — `tryBecomeLeaderLocked` под тем же локом;
  - рассылка `RequestVote(newTerm, nodeId, lastLogIndex, lastLogTerm)` пирам — **вне `nodeLock`**, метаданные лога берутся из снапшота сразу после `startElection`.
- `handleRequestVoteResponse(...)`: под `nodeLock`; при `resp.term > currentTerm` — step-down (`updateTermAndVote`, `FOLLOWER`, `leaderId=null`, stop heartbeats, reset timer); ответ учитывается только если терм совпадает с термом кампании, узел `CANDIDATE` и голосовал за себя (проверка (4)); при кворуме — `tryBecomeLeaderLocked`.
- `tryBecomeLeaderLocked(...)` (проверка (2)): переводит в `LEADER` только если кворум собран **и** узел всё ещё `CANDIDATE` в ожидаемом терме с self-vote; логирует `became leader`, запускает heartbeats.
- `startHeartbeatsLocked()/stopHeartbeatsLocked()`: управляют единичной задачей в `raft-heartbeat` (`scheduleWithFixedDelay`, интервал `heartbeatIntervalMs`); вызываются только под `nodeLock`.
- `sendHeartbeats()`: под `nodeLock` проверяет (5) актуальность лидерства/терма, фиксирует `AppendEntries(currentTerm, nodeId)` и список пиров, иначе останавливает heartbeats; логирует `AppendEntries sent` и рассылает RPC **вне `nodeLock`**.
- `handleAppendEntriesResponse(...)`: под `nodeLock`; ответ учитывается только для актуального лидерства/терма (проверка (5)); при `resp.term > currentTerm` — step-down.
- `shutdown()` (`@PreDestroy`): под `nodeLock` останавливает таймер, heartbeats и `heartbeatScheduler`.

**Ответственность за атомарность/конкурентность:** обеспечивает атомарность **составных** переходов узла в рамках одного процесса: решение и все его side-effects (`role`, `leaderId`, timer, heartbeats, apply) выполняются под `nodeLock`. Порядок блокировок: **`nodeLock` → лок `RaftState`**; обратный порядок запрещён (внутренний лок `RaftState` не экспонируется). Сетевые вызовы выполняются вне `nodeLock`, поэтому медленный/зависший пир не блокирует обработку входящих RPC и таймеры; актуальность контекста после сети проверяется повторно под локом.

### `raft.model` outcome/event-типы
- `StateTransition(before, after)` — пара `RaftStateSnapshot`, описывающая ровно один переход.
- `RequestVoteOutcome(event, state)` / `AppendEntriesOutcome(event, state)` — решение узла + переход; `granted()`/`accepted()` — производные предикаты для формирования ответа.
- `RequestVoteEvent`: `GRANTED`, `GRANTED_REPEAT` (`before.votedFor == candidate` при равном терме; определяется **сервисом**), `GRANTED_TERM_ADVANCED`, `REJECTED_STALE_TERM`, `REJECTED_ALREADY_VOTED`, `REJECTED_LOG_NOT_UP_TO_DATE` (§5.4.1).
- `AppendEntriesEvent`: `ACCEPTED_TERM_ADVANCED` (приоритет), `ACCEPTED_LEADER_DISCOVERED` (смена роли и/или `leaderId`), `ACCEPTED_IDEMPOTENT`, `REJECTED_STALE_TERM`, `REJECTED_FOREIGN_LEADER` (равный терм, другой лидер). `accepted()` истинно только для `ACCEPTED_*`.

### `raft.rpc.RaftRpcService` (`src/main/java/com/myraft/raft/rpc/RaftRpcService.java`)
**Назначение:** тонкий адаптер «входящий RPC → `RaftNode` → лог/ответ». Состояние не мутирует напрямую; единственный побочный эффект — сброс election timer через узел при `GRANTED_REPEAT`.

**Контракт:**
- Конструктор (единственный, для DI): `(RaftNode, RaftState, RaftProperties)`.
- `handleRequestVote(RequestVoteRequest)`:
  - снимает `pre`-снапшот для MDC;
  - **prefilter** (синтетический outcome `StateTransition(pre, pre)`, узел не вызывается): `request.term < pre.currentTerm` → `REJECTED_STALE_TERM`; затем `request.term == pre.currentTerm && pre.votedFor != null && pre.votedFor != request.candidateId` → `REJECTED_ALREADY_VOTED`; затем при равном терме не-свежий лог кандидата → `REJECTED_LOG_NOT_UP_TO_DATE`. При `term > currentTerm` узел вызывается, чтобы шагнуть вниз по §5.1;
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
  - `REJECTED_LOG_NOT_UP_TO_DATE` → `vote rejected (log not up-to-date)` с term/index кандидата и локальными term/index (INFO);
  - `ACCEPTED_TERM_ADVANCED` → `stepping down (incoming AppendEntries has higher term)`;
  - `ACCEPTED_LEADER_DISCOVERED` → `discovered new leader` (если роль была не `FOLLOWER`) и/или `leader changed to ...` (если сменился `leaderId`);
  - `ACCEPTED_IDEMPOTENT` → DEBUG `heartbeat accepted (no state change)`;
  - `REJECTED_STALE_TERM` → DEBUG `heartbeat ignored (incoming AppendEntries stale term)`;
  - `REJECTED_FOREIGN_LEADER` → DEBUG `heartbeat ignored (foreign leader at same term) from=... currentLeader=...`.
- **Persist-before-respond** обеспечивается `RaftState` (persist выполняется внутри решения до возврата outcome). При исключении `storage.persist` событие не логируется, ответ не формируется — исключение уходит наверх (HTTP 500).
- Журнал/entries пока не валидируются и не применяются на стороне получателя `AppendEntries` (heartbeat/election-only); DTO уже несут поля для будущей репликации.

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
- `RestTemplateConfig`: таймауты HTTP-клиента `200ms/200ms`; `MappingJackson2HttpMessageConverter` использует Spring `ObjectMapper` с зарегистрированными subtype-модулями (иначе `Command` в DTO не десериализуется).
- `StateMachineConfig`: бины `KvCommandsModule` (реестр `PUT → PutCommand`) и `StateMachine<JsonNode> = KvStateMachine`.
- DTO RPC: `RequestVoteRequest(term, candidateId, lastLogIndex, lastLogTerm)`, `RequestVoteResponse(term, voteGranted)`, `AppendEntriesRequest(term, leaderId, prevLogIndex, prevLogTerm, entries: List<LogEntry>, leaderCommit)`, `AppendEntriesResponse(term, success, lastIndex, conflictTerm, conflictIndex)` — структуры данных без логики синхронизации. `entries` содержат `RaftCommand`-конверт; получатель пока не проверяет журнал (этап 3), но JSON round-trip покрыт тестами.

## Границы ответственности по атомарности (сводно)
- **Атомарно и с persist-before-visible:** инкремент терма/выдача голоса/монотонное обновление `currentTerm,votedFor` и изменение лога (`RaftState` + `RaftStorage.persist(PersistedState)`).
- **Атомарно в рамках процесса (под `nodeLock`):** составные переходы узла `FOLLOWER/CANDIDATE/LEADER` + `leaderId` + election timer + запуск/остановка heartbeats + обработка входящих RPC и колбэков ответов; решение и его side-effects логируются из `before/after`-снапшотов этого перехода.
- **Вне `nodeLock`:** сетевые вызовы (`RaftRpcClient`), построение `/status` (только `RaftState.snapshot()`).
- **Порядок блокировок:** `nodeLock` → лок `RaftState`; обратный порядок запрещён (внутренний лок `RaftState` не экспонируется). `RaftRpcClient` не удерживает ни один из локов во время HTTP-вызова.
- **Источники конкуренции:** HTTP-запросы (`/raft/*`, `/status`), `raft-election-timer`, `raft-heartbeat`, асинхронные RPC-вызовы/колбэки (`CompletableFuture.supplyAsync`).

## Логирование решений (RPC outcome logging)
- Входящие RPC: `RaftNode` возвращает outcome, `RaftRpcService` логирует `vote granted/stepping down/leader changed/...` и формирует ответ из `after.currentTerm()`.
- Исходящие: `starting election`, `RequestVote sent term=... peers=N` (одна строка на раунд выборов в `RaftNode`; per-peer строки `RaftRpcClient` — DEBUG), `vote granted (n/m) from peer ... from=<peer>`, `became leader`, `stepping down (outgoing ... higher term)`, `AppendEntries sent` (раз в раунд на лидере).
- MDC (`nodeId/term/role`) выставляется в точке логирования из снапшота: async-колбэки и heartbeat-поток не наследуют MDC HTTP-потока.
- Решение и его side-effects не логируются повторным чтением живого состояния — только из `StateTransition`.
- Этап 1 добавил: `vote rejected (log not up-to-date)` (INFO, из `before`-снапшота и полей кандидата) и `log persist failed` (ERROR, из фиксируемого `term/votedFor/logSize` внутри `RaftState`).
- Этап 2 добавил: `entry applied` (INFO, `index`/`lastApplied`, без KV-ключа), `no-op applied` и `apply skipped` (DEBUG), `kv applied index=... key=...` (DEBUG, только в `KvStateMachine`).

## Оставшиеся ограничения реализации
- Нет репликации лога, коммита по большинству, NoOp после избрания, снапшотов, pre-vote, динамической конфигурации. Apply-цикл есть, но запускается вручную/тестами: производственный код пока не продвигает `commitIndex`.
- Персистируются `formatVersion/currentTerm/votedFor/log[]`; `role/leaderId`, `commitIndex`/`lastApplied` не восстанавливаются (volatile по Figure 2).
- Старые файлы этапа 1 (без `formatVersion` или со старым форматом) не читаются — fail-fast `unsupported raft state format`; legacy-reader не предусмотрен.
- `AppendEntries` не проверяет согласованность лога; при валидном терме и известном/том же лидере возвращает `success=true`.
- При равном терме heartbeat принимается только от уже известного лидера (`leaderId == null` или совпадает); от другого лидера — `REJECTED_FOREIGN_LEADER`. Проверка относится только к терму/лидеру, не к содержимому журнала.
- Пользовательских REST API записи/чтения пока нет; `ApplyResult` сохраняется в `RaftNode` для будущего API-адаптера (этап 4).
- `nodeLock` сериализует переходы только внутри одного процесса; корректность между узлами по-прежнему опирается на Raft-инварианты терма/голоса.
