# Состояние кода и контракты ключевых классов (my-raft)

Актуальный снимок реализации (Raft с выборами лидера, моделью лога и персистентностью, абстрактной
state machine с KV-конкретизацией, репликацией/коммитом и внутренним REST API записи/чтения — этапы 1–4,
Spring Boot 3.3.5, Java 21). Документ фиксирует **контракты ключевых классов в текущем коде**,
включая ответственность за атомарность/конкурентность. Целевой внешний контракт
`docs/client_openapi.json` не реализован.

## Стек и структура
- Java 21, Spring Boot 3.3.5, Maven (`spring-boot-starter-web`, `spring-boot-configuration-processor`, `spring-boot-starter-test`).
- Основные пакеты: `api`, `config`, `raft.model`, `raft.fsm`, `raft.fsm.kv`, `raft.rpc`, `raft.rpc.dto`, `raft.storage`, `raft.timer`, `raft` (`RaftNode`).
- Конфигурация (`application.yml`, по умолчанию): `raft.election-timeout-min-ms=500`, `raft.election-timeout-max-ms=700`, `raft.heartbeat-interval-ms=100`, `raft.storage-path=data/raft-state.json`, `raft.client.apply-timeout-ms=5000`, `RestTemplate` connect/read `200ms`.
- Тесты: `mvn test` — 68 тестов, BUILD SUCCESS (17 классов; 19 исходных + 16 этапа 1 + 15 этапа 2 + 5 этапа 3 + 5 этапа 4 + 3 отказоустойчивости + 5 клиентских): `MyRaftApplicationTests` (`@SpringBootTest` contextLoads), `StatusResponseJsonTest`, `api.RaftClientApiTest`, `RaftElectionTest`, `RaftNodeElectionTest`, `RaftStateAtomicityTest`, `raft.RaftNodeApplyTest`, `raft.RaftReplicationTest`, `raft.rpc.RaftRpcOutcomeTest`, `raft.rpc.RaftRpcServiceAtomicityTest`, `FileRaftStorageTest`, `raft.storage.FileRaftStorageRestartTest`, `raft.model.RaftLogTest`, `raft.model.RaftCommandJsonTest`, `raft.fsm.kv.KvStateMachineTest`, `raft.rpc.RaftVoteFreshnessTest`, `raft.rpc.dto.RaftDtoJsonTest`.
- Тесты отказоустойчивости: `RaftReplicationTest` расширен сценариями гибели лидера/фолловера и перезапуска узла поверх того же `RaftStorage`; `FileRaftStorageRestartTest` проверяет восстановление term/vote/лога и сброс volatile-полей.
- Клиентские тесты: `RaftClientApiTest` расширен проверками `503` без лидера, `504` apply-timeout с незакоммиченной записью и догоном после рестарта, `found=false` против JSON `null`, `/raft/log` после смены лидера/рестарта и round-trip JSON-типов. Production-код не затрагивается.

## Контракты ключевых классов

### `raft.fsm` / `raft.model`: лог, конверт команд и state machine (этапы 1–2)
- `raft.fsm.StateMachine<R>` — абстракция детерминированной SM: `ApplyResult<R> apply(long index, Command command)`; `snapshot()/restore(Snapshot)` — future-default (`UnsupportedOperationException`), не реализуются. Требования к concrete SM: детерминизм, идемпотентность по индексу, быстрый apply без сетевых/дисковых побочных эффектов под `nodeLock`.
- `raft.fsm.Command` — **открытый** маркер пользовательских команд с `@JsonTypeInfo(property = "type")`, без `@JsonSubTypes` в ядре. Concrete-типы регистрируются внешним `KvCommandsModule` (Jackson `SimpleModule`, `PUT → PutCommand`); неизвестный `type` → ошибка десериализации. Ядро не знает KV-семантику.
- `raft.fsm.ApplyResult<R>(previousValue, value, index)` — результат apply; ядро хранит его как непрозрачный `ApplyResult<?>`.
- `raft.fsm.kv.KvStateMachine implements StateMachine<JsonNode>` — первая конкретизация: `Map<String, JsonNode>` (ConcurrentHashMap), `get(key)` и `contains(key)` для чтения API (`contains` отличает отсутствующий ключ от JSON null). `PutCommand(key, JsonNode value)` (`type=PUT`) отвергает пустой `key`, Java `null` нормализуется в `NullNode`; неизвестная команда → `IllegalArgumentException`. KV-специфичный `key` логируется только в concrete SM.
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
- `appendLogEntry(LogEntry)` / `appendLogEntries(List<LogEntry>)` / `truncateLogFrom(long index)` — под `writeLock`: строят новый `RaftLog`, фиксируют unified состояние в хранилище и только затем публикуют ссылку на лог. `appendLogEntries` пишет всю пачку **одной** персистентной операцией (persist-before-visible для батча); пустой список — no-op. Сбой записи логируется как `log persist failed` и пробрасывается наверх.
- `lastLogIndex()/lastLogTerm()/logSnapshot()` — чтения лога под `readLock`; `logSnapshot()` возвращает неизменяемый `RaftLog` и предназначен для чтения под `nodeLock` без вложенных локов.
- `snapshot(): RaftStateSnapshot(currentTerm, votedFor, role, leaderId, startTime, lastLogIndex, lastLogTerm)` — согласованный снимок всех полей под одним `readLock`; используется для логирования решений, `/status` и проверок «до/после». Внутренний лок (`getLock()`) наружу **не** экспонируется, чтобы не создавать обходной порядок блокировок.
- `getCurrentTerm/getVotedFor/getRole/getLeaderId` — чтения под `readLock`; `setRole/setLeaderId` — записи под `writeLock` (но **отдельно** от term/vote-переходов).
- `advanceCommitIndex(long)` / `getCommitIndex()` и `advanceLastApplied(long)` / `getLastApplied()` — монотонные volatile-указатели (под тем же `readLock`/`writeLock`). `commitIndex` не может опережать конец лога; **не персистируются** (volatile по Figure 2). Решение о продвижении принимает `RaftNode.maybeCommitLocked()` (лидер) или `advanceFollowerCommitLocked()` (фолловер); сам `RaftState` только хранит указатели.

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
**Назначение:** единственная точка принятия решений и мутаций состояния узла (старт, таймаут выборов, выборы, лидерство, репликация лога, коммит/apply, обработка входящих RPC и ответов на исходящие RPC).

**Контракт:**
- Конструктор для DI: `(RaftState, RaftProperties, RaftRpcClient, ElectionTimer, StateMachine<?>)`; KV — только одна из конкретизаций, ядро работает с абстракцией.
- **Единый `nodeLock`** (`private final Object`) сериализует все переходы узла: `role`/`leaderId`, election timer, запуск/остановку heartbeats, `nextIndex`/`matchIndex`, лог и `commitIndex`/`lastApplied`, обработку входящих RPC, apply и колбэки ответов.
- `init()` (`@PostConstruct`): под `nodeLock` переводит узел в `FOLLOWER`, сбрасывает `leaderId`, регистрирует callback election timer и запускает таймер.
- `handleRequestVote(RequestVoteRequest): RequestVoteOutcome` — под `nodeLock`: снимает `before`-снапшот; при `term < currentTerm` — `REJECTED_STALE_TERM`; затем проверяет свежесть лога по §5.4.1 (`lastLogTerm`, затем `lastLogIndex`) и при более свежем локальном логе возвращает `REJECTED_LOG_NOT_UP_TO_DATE` (при `term > currentTerm` сначала монотонно обновляет term/vote через `updateTermAndVote(term, null)` и делает step-down, **не выдавая голос**); иначе вызывает `RaftState.grantVote(...)` и применяет side-effects (`FOLLOWER`/`leaderId=null` при росте терма, иначе `reset()` таймера при **первом** голосе). События узла: `GRANTED`, `GRANTED_TERM_ADVANCED`, `REJECTED_STALE_TERM`, `REJECTED_ALREADY_VOTED`, `REJECTED_LOG_NOT_UP_TO_DATE`. Повторный голос тому же кандидату возвращается как `GRANTED` и классифицируется в `GRANTED_REPEAT` сервисом; узел в этом случае таймер не трогает. **Не логирует** и не трогает MDC.
- `resetElectionTimerIfUnchanged(RaftStateSnapshot expected)` — под `nodeLock` сравнивает текущий снапшот с `after` из outcome (term/votedFor/role) и только при совпадении сбрасывает election timer. Используется сервисом для эффекта `GRANTED_REPEAT` без прямого доступа к таймеру и без сброса после параллельного step-down/смены роли.
- `handleAppendEntries(AppendEntriesRequest): AppendEntriesOutcome` — под `nodeLock`: `before`-снапшот; `request.term < currentTerm` → `REJECTED_STALE_TERM`; равный терм и уже известный `leaderId != null`, отличный от `request.leaderId` → `REJECTED_FOREIGN_LEADER`; иначе монотонный `updateTermAndVote(term, null)` при росте терма и `becomeFollowerLocked(leaderId, true)`. Далее проверка согласованности лога по §5.3: если `prevLogIndex > lastIndex` или `termAt(prevLogIndex) != prevLogTerm` → `REJECTED_LOG_MISMATCH` с подсказками `conflictTerm`/`conflictIndex` (первый индекс с этим термом, иначе `lastIndex + 1`). Иначе `appendEntriesLocked`: несовпадающий терм по индексу вырезает хвост через `truncateLogFrom`, оставшиеся записи пишутся батчем через `RaftState.appendLogEntries`; уже реплицированные записи с тем же `(index, term)` пропускаются (идемпотентность). Затем `advanceFollowerCommitLocked(leaderCommit, lastNewIndex)`: `min(leaderCommit, lastNewIndex)`, для heartbeat без entries граница — `lastLogIndex`; рост `commitIndex` вызывает apply. Событие `ACCEPTED_TERM_ADVANCED` / `ACCEPTED_LEADER_DISCOVERED` / `ACCEPTED_LOG_TRUNCATED` / `ACCEPTED_LOG_APPENDED` / `ACCEPTED_IDEMPOTENT` (приоритет в этом порядке). **Не логирует**.
- `applyCommittedEntries()` / `appliedResult(index)` (этапы 2–4): под `nodeLock` применяют записи `(lastApplied, commitIndex]` строго по индексу. Для `UserCommand` вызывается `StateMachine.apply(index, payload)`, результат сохраняется в `Map<Long, ApplyResult<?>>` и завершает зарегистрированный `CompletableFuture` ожидающего клиента; `NoOpCommand` только продвигает `lastApplied` (SM не вызывается). При отсутствии новых записей — DEBUG `apply skipped`. Повторный вызов ничего не меняет; исключение apply оставляет запись неприменённой. С этапа 3 вызывается автоматически при каждом росте `commitIndex` через `applyCommittedSafelyLocked()` (исключение apply логируется как ERROR и не гасит RPC/колбэк).
- `awaitApplied(long index, long timeoutMs)` (этап 4): под `nodeLock` возвращает уже готовый `ApplyResult<?>` или регистрирует `CompletableFuture`, затем ждёт **вне лока**. При таймауте вызывает `cancelAwait(index, future)` (публичный `cancelAwait(index)` — для снятия регистрации) и бросает `ApplyTimeoutException`; прерванный поток восстанавливает interrupt. API-адаптер превращает таймаут в `504`.
- `onElectionTimeout()`: логирует `election timeout`, затем `startElection()`; если узел `LEADER` — только reset таймера.
- `startElection()`:
  - под `nodeLock`: проверка «не лидер», `raftState.startElection(nodeId)`, затем проверка (1) — терм и self-vote всё ещё актуальны; установка `role=CANDIDATE`, `leaderId=null`, логирование `starting election`, reset таймера;
  - если кворум уже достигнут (single-node) — `tryBecomeLeaderLocked` под тем же локом;
  - рассылка `RequestVote(newTerm, nodeId, lastLogIndex, lastLogTerm)` пирам — **вне `nodeLock`**, метаданные лога берутся из снапшота сразу после `startElection`.
- `handleRequestVoteResponse(...)`: под `nodeLock`; при `resp.term > currentTerm` — step-down (`updateTermAndVote`, `FOLLOWER`, `leaderId=null`, stop heartbeats, reset timer); ответ учитывается только если терм совпадает с термом кампании, узел `CANDIDATE` и голосовал за себя (проверка (4)); при кворуме — `tryBecomeLeaderLocked`.
- `tryBecomeLeaderLocked(...)` (проверка (2)): переводит в `LEADER` только если кворум собран **и** узел всё ещё `CANDIDATE` в ожидаемом терме с self-vote; логирует `became leader`, затем `initReplicationLocked()`, `appendNoOpLocked(currentTerm)`, `maybeCommitLocked()` и запускает heartbeats.
- `initReplicationLocked()` (этап 3): сбрасывает `nextIndex`/`matchIndex`; для каждого пира `nextIndex = lastLogIndex + 1`, `matchIndex = 0`. Карты очищаются также в `becomeFollowerLocked` при потере лидерства.
- `appendNoOpLocked(term)` (этап 3): сразу после избрания дописывает `LogEntry(lastLogIndex + 1, term, NoOpCommand)`; NoOp — Raft-маркер текущего терма, в SM не попадает.
- `appendUserCommand(Command): long` (этап 3, без REST): только на лидере дописывает `UserCommand` текущего терма, вызывает `maybeCommitLocked()`; возвращает индекс или `-1`, если узел не лидер.
- `maybeCommitLocked()` (этап 3): перебирает `n` от `lastIndex` до `commitIndex + 1`, коммит только при `log[n].term == currentTerm` и `1 + count(matchIndex[peer] >= n) >= majority`; старые термы коммитятся косвенно через запись текущего терма. Рост `commitIndex` логируется и запускает apply.
- `startHeartbeatsLocked()/stopHeartbeatsLocked()`: управляют единичной задачей в `raft-heartbeat` (`scheduleWithFixedDelay`, интервал `heartbeatIntervalMs`); вызываются только под `nodeLock`.
- `sendAppendEntries()` (этап 3; заменил `sendHeartbeats`): под `nodeLock` проверяет (5) лидерство/терм, для каждого пира строит `ReplicationTarget` с персональным `prevLogIndex = nextIndex - 1`, `prevLogTerm`, срезом `entries` от `prevLogIndex` до конца лога и общим `leaderCommit`; одну строку `AppendEntries sent` пишет на раунд. Все HTTP-вызовы — **вне `nodeLock`**.
- `handleAppendEntriesResponse(target, resp)` (этап 3): step-down при `resp.term > currentTerm` имеет приоритет; затем проверка (5) (роль, терм отправки). На успех `matchIndex = min(resp.lastIndex, sentLastIndex)` (монотонно), `nextIndex = max(oldNext, max(match, sentLastIndex) + 1)` и `maybeCommitLocked()`. На отказ — `conflictBasedNextIndex`: по `conflictTerm` (последний индекс этого терма в логе лидера + 1, либо `conflictIndex` пира), иначе `conflictIndex`, иначе `currentNext - 1`; индекс никогда не увеличивается.
- `replicateNow()`: внеочередной раунд репликации (для тестов/принудительной репликации), тот же путь `sendAppendEntries()` без захвата лока на время сети.
- `shutdown()` (`@PreDestroy`): под `nodeLock` останавливает таймер, heartbeats и `heartbeatScheduler`.

**Ответственность за атомарность/конкурентность:** обеспечивает атомарность **составных** переходов узла в рамках одного процесса: решение и все его side-effects (`role`, `leaderId`, timer, heartbeats, `nextIndex`/`matchIndex`, лог, `commitIndex`/`lastApplied`, apply) выполняются под `nodeLock`. Порядок блокировок: **`nodeLock` → лок `RaftState`**; обратный порядок запрещён (внутренний лок `RaftState` не экспонируется). Сетевые вызовы выполняются вне `nodeLock`, поэтому медленный/зависший пир не блокирует обработку входящих RPC и таймеры; актуальность контекста после сети проверяется повторно под локом.

### `raft.model` outcome/event-типы
- `StateTransition(before, after)` — пара `RaftStateSnapshot`, описывающая ровно один переход.
- `RequestVoteOutcome(event, state)` — решение узла + переход; `granted()` — производный предикат. `AppendEntriesOutcome(event, state, details)` — решение + переход + `Details(appended, truncated, lastNewIndex, conflictTerm, conflictIndex)`; `accepted()` истинно только для `ACCEPTED_*`. Детали нужны адаптеру для ответа (`lastIndex`, conflict-подсказки) и логов; для решений без изменения лога используется `Details.EMPTY`.
- `RequestVoteEvent`: `GRANTED`, `GRANTED_REPEAT` (`before.votedFor == candidate` при равном терме; определяется **сервисом**), `GRANTED_TERM_ADVANCED`, `REJECTED_STALE_TERM`, `REJECTED_ALREADY_VOTED`, `REJECTED_LOG_NOT_UP_TO_DATE` (§5.4.1).
- `AppendEntriesEvent`: `ACCEPTED_TERM_ADVANCED` (приоритет), `ACCEPTED_LEADER_DISCOVERED` (смена роли и/или `leaderId`), `ACCEPTED_LOG_TRUNCATED` / `ACCEPTED_LOG_APPENDED` (реальные изменения лога), `ACCEPTED_IDEMPOTENT`, `REJECTED_STALE_TERM`, `REJECTED_FOREIGN_LEADER` (равный терм, другой лидер), `REJECTED_LOG_MISMATCH` (`prevLog*`/терм не совпали). `accepted()` истинно только для `ACCEPTED_*`.

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
  - иначе вызывает `raftNode.handleAppendEntries(request)` (авторитетная проверка терма/лидера/лога под `nodeLock`);
  - логирует `AppendEntries received`, детали из `outcome.details()` и формирует ответ `AppendEntriesResponse(after.currentTerm(), outcome.accepted(), after.lastLogIndex(), conflictTerm, conflictIndex)`; живое состояние после вызова узла не перечитывается.
- Логи по событию (MDC `nodeId/term/role` из `after`):
  - `GRANTED` / `GRANTED_REPEAT` → `vote granted (...)`;
  - `GRANTED_TERM_ADVANCED` → `stepping down (incoming RequestVote has higher term)` + `vote granted (to candidate) ... (was N)`;
  - `REJECTED_STALE_TERM` → DEBUG `vote rejected (stale term, ...)`;
  - `REJECTED_ALREADY_VOTED` → `vote rejected (already voted / condition failed)`;
  - `REJECTED_LOG_NOT_UP_TO_DATE` → `vote rejected (log not up-to-date)` с term/index кандидата и локальными term/index (INFO);
  - `ACCEPTED_TERM_ADVANCED` → `stepping down (incoming AppendEntries has higher term)`;
  - `ACCEPTED_LEADER_DISCOVERED` → `discovered new leader` (если роль была не `FOLLOWER`) и/или `leader changed to ...` (если сменился `leaderId`);
  - `ACCEPTED_LOG_APPENDED` / `ACCEPTED_LOG_TRUNCATED` → INFO `log entry appended count=... lastNewIndex=...`, при truncate дополнительно `log conflict truncated from=... appended=...`;
  - `ACCEPTED_IDEMPOTENT` → DEBUG `heartbeat accepted (no state change)`;
  - `REJECTED_LOG_MISMATCH` → INFO `log mismatch rejected ...` с `prevLogIndex/prevLogTerm` и локальными term/index из `after`-снапшота;
  - `REJECTED_STALE_TERM` → DEBUG `heartbeat ignored (incoming AppendEntries stale term)`;
  - `REJECTED_FOREIGN_LEADER` → DEBUG `heartbeat ignored (foreign leader at same term) from=... currentLeader=...`.
- **Persist-before-respond** обеспечивается `RaftState` (persist выполняется внутри решения до возврата outcome). При исключении `storage.persist` событие не логируется, ответ не формируется — исключение уходит наверх (HTTP 500).
- Журнал/entries на стороне получателя `AppendEntries` проверяются по `prevLog*`, при необходимости вырезается конфликтующий хвост, записи дописываются идемпотентно; `leaderCommit` продвигает `commitIndex` и запускает apply.

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

### `api.RaftClientController` и `api.RaftClientService` (этап 4)
**Назначение:** внутренний клиентский API записи/чтения поверх ядра Raft. Внешний контракт
`docs/client_openapi.json` (`POST /query`, `GET /query`, `GET /log`) остаётся целевым и не реализован.

**Контракт контроллера:**
- `POST /raft/log` — тело `AppendRequest { key, value: JsonNode }`; только лидер. Успех —
  `AppendResponse { previousValue, value, index }` из `ApplyResult`. Ответ фолловера — `503`
  с `ApiErrorResponse { error, leaderId }` и без forward; пустой `key` — `400`; таймаут apply — `504`.
- `GET /raft/log` — список `LogEntryResponse { index, term, key, value }`: только `UserCommand`
  в порядке индекса, `NoOpCommand` отфильтрован.
- `GET /raft/kv?key=...` — `KvReadResponse { key, value, found }`; `value = null` допустим,
  `found=false` означает отсутствие ключа.
- MDC `nodeId/term/role` выставляется из одного `RaftState.snapshot()` на каждый запрос,
  в том числе в обработчиках ошибок (`@ExceptionHandler`: `NotLeaderException`, `ApplyTimeoutException`,
  `IllegalArgumentException`).

**Контракт сервиса (API-адаптер):**
- Конструктор `RaftClientService(RaftNode, RaftState, KvStateMachine, applyTimeoutMs)` (`raft.client.apply-timeout-ms`, по умолчанию 5000 мс).
- `append(key, value)`: валидирует `key`, проверяет лидерство по `RaftState.snapshot()`, вызывает
  `RaftNode.appendUserCommand(new PutCommand(key, value))`, логирует `client append accepted`, ждёт
  `awaitApplied(index, timeout)` и сужает непрозрачный `ApplyResult<?>` до `ApplyResult<JsonNode>`;
  логирует `client append responded`. Если лидерство изменилось между снимком и append — тот же `503 + leaderId`.
- `readLog()`: читает `RaftState.logSnapshot()`, берёт только `UserCommand`/`PutCommand` без захвата
  `nodeLock`; логирует `log read`. `readKv(key)`: `get`/`contains` из `KvStateMachine`, логирует `kv read`.
- Ядро по-прежнему не знает KV-семантику: конкретизация `ApplyResult` и DTO происходят только здесь.

**Ответственность за атомарность/конкурентность:** проверка лидерства и чтение лога используют
согласованные снимки `RaftState`; ожидание apply не удерживает `nodeLock`. MDC в exception-хендлерах
выставляется заново и не зависит от `finally` основного обработчика.

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
- `StateMachineConfig`: бины `KvCommandsModule` (реестр `PUT → PutCommand`) и `KvStateMachine` (тип объявлен конкретным для API-адаптера, для ядра остаётся `StateMachine<?>`).
- DTO RPC: `RequestVoteRequest(term, candidateId, lastLogIndex, lastLogTerm)`, `RequestVoteResponse(term, voteGranted)`, `AppendEntriesRequest(term, leaderId, prevLogIndex, prevLogTerm, entries: List<LogEntry>, leaderCommit)`, `AppendEntriesResponse(term, success, lastIndex, conflictTerm, conflictIndex)` — структуры данных без логики синхронизации. `entries` содержат `RaftCommand`-конверт; получатель проверяет журнал по `prevLog*`; ответ несёт `lastIndex` и conflict-подсказки; JSON round-trip покрыт тестами.
- DTO клиентского API: `AppendRequest(key, value)`, `AppendResponse(previousValue, value, index)`, `LogEntryResponse(index, term, key, value)`, `KvReadResponse(key, value, found)`, `ApiErrorResponse(error, leaderId)` — только данные, синхронизации не содержат.

## Границы ответственности по атомарности (сводно)
- **Атомарно и с persist-before-visible:** инкремент терма/выдача голоса/монотонное обновление `currentTerm,votedFor` и изменение лога (`RaftState` + `RaftStorage.persist(PersistedState)`).
- **Атомарно в рамках процесса (под `nodeLock`):** составные переходы узла `FOLLOWER/CANDIDATE/LEADER` + `leaderId` + election timer + запуск/остановка heartbeats + `nextIndex`/`matchIndex` + изменение лога и `commitIndex`/`lastApplied` + apply + обработка входящих RPC и колбэков ответов; решение и его side-effects логируются из `before/after`-снапшотов этого перехода.
- **Вне `nodeLock`:** сетевые вызовы (`RaftRpcClient`), построение `/status` и клиентских чтений (только `RaftState.snapshot()`/`logSnapshot()`), ожидание apply в `RaftClientService` (`awaitApplied`).
- **Порядок блокировок:** `nodeLock` → лок `RaftState`; обратный порядок запрещён (внутренний лок `RaftState` не экспонируется). `RaftRpcClient` не удерживает ни один из локов во время HTTP-вызова.
- **Источники конкуренции:** HTTP-запросы (`/raft/*`, `/status`), `raft-election-timer`, `raft-heartbeat`, асинхронные RPC-вызовы/колбэки (`CompletableFuture.supplyAsync`).

## Логирование решений (RPC outcome logging)
- Входящие RPC: `RaftNode` возвращает outcome, `RaftRpcService` логирует `vote granted/stepping down/leader changed/...` и формирует ответ из `after.currentTerm()`.
- Исходящие: `starting election`, `RequestVote sent term=... peers=N` (одна строка на раунд выборов в `RaftNode`; per-peer строки `RaftRpcClient` — DEBUG), `vote granted (n/m) from peer ... from=<peer>`, `became leader`, `stepping down (outgoing ... higher term)`, `AppendEntries sent term=... peers=N entries=... leaderCommit=... lastLogIndex=...` (раз в раунд на лидере).
- MDC (`nodeId/term/role`) выставляется в точке логирования из снапшота: async-колбэки и heartbeat-поток не наследуют MDC HTTP-потока.
- Решение и его side-effects не логируются повторным чтением живого состояния — только из `StateTransition`.
- Этап 1 добавил: `vote rejected (log not up-to-date)` (INFO, из `before`-снапшота и полей кандидата) и `log persist failed` (ERROR, из фиксируемого `term/votedFor/logSize` внутри `RaftState`).
- Этап 2 добавил: `entry applied` (INFO, `index`/`lastApplied`, без KV-ключа), `no-op applied` и `apply skipped` (DEBUG), `kv applied index=... key=...` (DEBUG, только в `KvStateMachine`).
- Этап 3 добавил: `no-op entry appended`, `log entry appended` (follower, батч), `log conflict truncated`, `log mismatch rejected`, `log replicated to ...` (DEBUG), `nextIndex decremented ...` (DEBUG), `commitIndex advanced from=... to=...` (лидер с `term`, follower с `leaderCommit`). MDC выставляется в heartbeat-потоке и async-колбэках; детали по пирам — DEBUG, решения — INFO.
- Этап 4 добавил: `client append accepted` (key, index, term, размер value), `client append responded` (index, previousValue/value), `client redirected` (leaderId), `log read` (count, lastIndex), `kv read` (key, found). MDC выставляется в контроллере и повторно в exception-хендлерах.

## Оставшиеся ограничения реализации
- Репликация лога, NoOp после избрания, коммит по большинству и внутренний REST API записи/чтения реализованы (этапы 3–4). Остаются нереализованными: снапшоты, pre-vote, динамическая конфигурация кластера и внешний контракт `docs/client_openapi.json`.
- Персистируются `formatVersion/currentTerm/votedFor/log[]`; `role/leaderId`, `commitIndex`/`lastApplied`, `nextIndex`/`matchIndex` не восстанавливаются (volatile по Figure 2; индексы репликации инициализируются заново при каждом лидерстве).
- Старые файлы этапа 1 (без `formatVersion` или со старым форматом) не читаются — fail-fast `unsupported raft state format`; legacy-reader не предусмотрен.
- `AppendEntries` проверяет согласованность лога по `prevLogIndex`/`prevLogTerm`, вырезает конфликтующий хвост и дописывает батч идемпотентно; `success=true` возвращается только после успешной персистенции.
- При равном терме запрос принимается только от уже известного лидера (`leaderId == null` или совпадает); от другого лидера — `REJECTED_FOREIGN_LEADER`. Проверка терма/лидера предшествует проверке содержимого журнала.
- Коммит записей прошлых термов косвенный: лидер коммитит только запись своего терма по большинству; `NoOpCommand` не попадает в `StateMachine`.
- Внутренний REST API (`POST /raft/log`, `GET /raft/log`, `GET /raft/kv`) реализован: лидер отвечает после коммита/apply, фолловер — `503 + leaderId` без forward, `NoOpCommand` из клиентских ответов исключён. Forward на лидера и внешний контракт `docs/client_openapi.json` (`POST /query`, `GET /query`, `GET /log`) не реализованы.
- `nodeLock` сериализует переходы только внутри одного процесса; корректность между узлами по-прежнему опирается на Raft-инварианты терма/голоса/лога.
