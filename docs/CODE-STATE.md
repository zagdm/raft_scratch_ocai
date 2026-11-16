# Состояние кода и контракты ключевых классов (my-raft)

Актуальный снимок реализации (election-only Raft, Spring Boot 3.3.5, Java 21). Документ фиксирует **контракты ключевых классов в текущем коде**, включая ответственность за атомарность/конкурентность.

## Стек и структура
- Java 21, Spring Boot 3.3.5, Maven (`spring-boot-starter-web`, `spring-boot-configuration-processor`, `spring-boot-starter-test`).
- Основные пакеты: `api`, `config`, `raft.model`, `raft.rpc`, `raft.rpc.dto`, `raft.storage`, `raft.timer`, `raft` (`RaftNode`).
- Конфигурация (`application.yml`, по умолчанию): `raft.election-timeout-min-ms=5000`, `raft.election-timeout-max-ms=8000`, `raft.heartbeat-interval-ms=1000`, `raft.storage-path=data/raft-state.json`, `RestTemplate` connect/read `200ms`.
- Тесты: `mvn test` — 13 тестов, BUILD SUCCESS (`StatusResponseJsonTest`, `RaftElectionTest`, `RaftNodeElectionTest`, `RaftStateAtomicityTest`, `raft.rpc.RaftRpcServiceAtomicityTest`, `FileRaftStorageTest`).

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
- `getCurrentTerm/getVotedFor/getRole/getLeaderId` — чтения под `readLock`; `setRole/setLeaderId` — записи под `writeLock` (но **отдельно** от term/vote-переходов).
- `getLock()` экспонирует внутренний `ReentrantReadWriteLock`.

**Ответственность за атомарность:** отвечает за атомарность и монотонность **только** пары `currentTerm/votedFor` и их персистентной фиксации относительно видимости. **Не** обеспечивает атомарность составных переходов узла (`term/vote` + `role` + `leaderId` + таймеры + heartbeats).

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
**Назначение:** оркестрация жизненного цикла узла (старт, таймаут выборов, выборы, лидерство, heartbeats) и обработка ответов на исходящие RPC.

**Контракт:**
- `init()` (`@PostConstruct`): переводит узел в `FOLLOWER`, сбрасывает `leaderId`, регистрирует callback election timer и запускает таймер.
- `onElectionTimeout()`: если `LEADER` — reset таймера, иначе запускает `startElection()`.
- `startElection()`:
  - вызывает `raftState.startElection(nodeId)` и получает новый терм;
  - отдельно устанавливает `role=CANDIDATE`, `leaderId=null`, сбрасывает election timer;
  - голосует за себя, при необходимости сразу становится лидером (single-node/кворум), рассылает `RequestVote(newTerm, nodeId)` пирам; колбэки ответов обрабатываются асинхронно.
- `handleRequestVoteResponse(...)` (`synchronized` на `RaftNode`): при `resp.term > currentTerm` выполняет step-down (`updateTermAndVote(resp.term,null)`, `FOLLOWER`, `leaderId=null`, stop heartbeats, reset timer); учитывает ответ только если узел всё ещё `CANDIDATE` и терм совпадает; при кворуме вызывает `becomeLeader()`.
- `becomeLeader()` (`synchronized`): разрешён только из `CANDIDATE`; устанавливает `LEADER`, `leaderId=self`, запускает heartbeats.
- `startHeartbeats()/stopHeartbeats()` (`synchronized`): управляют единичной задачей в `raft-heartbeat` (`scheduleWithFixedDelay`, интервал фиксированный `heartbeatIntervalMs`).
- `sendHeartbeats()`: если не `LEADER` — останавливает heartbeats; иначе рассылает пустые `AppendEntries(currentTerm, nodeId)` всем пирам, ответы обрабатываются асинхронно.
- `handleAppendEntriesResponse(...)` (`synchronized`): при не-лидере игнорирует; при `resp.term > currentTerm` выполняет step-down.

**Ответственность за атомарность/конкурентность:** **не обеспечивает атомарность составных переходов узла**. Сериализованы только отдельные `synchronized` методы; `startElection`, `sendHeartbeats`, фоновые потоки (`raft-election-timer`, `raft-heartbeat`) и асинхронные RPC-колбэки могут конкурировать между собой и с HTTP-обработчиками. Корректность campaign/лидерства опирается на проверки роли/терма и атомарность `RaftState` для term/vote, но не на единый атомарный state transition.

### `raft.rpc.RaftRpcService` (`src/main/java/com/myraft/raft/rpc/RaftRpcService.java`)
**Назначение:** обработка входящих Raft RPC (`RequestVote`, `AppendEntries`) и применение побочных эффектов на основе решений состояния.

**Контракт:**
- `handleRequestVote(RequestVoteRequest)`:
  - получает решение через `raftState.grantVote(request.term, request.candidateId)` (атомарное решение по term/vote);
  - при `termIncreased` выполняет side-effects: `setRole(FOLLOWER)`, `setLeaderId(null)`, `electionTimer.reset()`;
  - при `granted` сбрасывает election timer и отвечает `voteGranted=true`; иначе отвечает отказом с текущим термом из результата.
  - **Persist-before-respond** для term/vote обеспечивается через `RaftState.grantVote`.
- `handleAppendEntries(AppendEntriesRequest)`:
  - stale (`request.term < currentTerm`) → `success=false`, `votedFor` не меняется;
  - `request.term > currentTerm` → `raftState.updateTermAndVote(request.term, null)` (монотонно);
  - для валидного терма переводит в `FOLLOWER` при необходимости, обновляет `leaderId`, сбрасывает election timer, отвечает `success=true` текущим термом.
  - Журнал/entries не валидируются и не применяются (heartbeat/election-only).

**Ответственность за атомарность/конкурентность:** не атомарен как единый обработчик перехода узла; атомарность ограничена вызовами `RaftState` (`grantVote`, `updateTermAndVote`). Между решением/обновлением term-vote и побочными эффектами (`role`, `leaderId`, timer, формирование ответа/логов) возможны гонки с другими потоками.

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
- `GET /status` формирует `StatusResponse(nodeId, role, currentTerm, leaderId, peers, isLeader, uptime)` из отдельных чтений `RaftState`/`RaftProperties`/`RaftRpcClient`.
- Не выполняет мутаций состояния; не собирает согласованный снимок под единым локом.

**Ответственность за атомарность/конкурентность:** не гарантирует консистентность набора полей между собой при конкурентных изменениях.

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
- **Не атомарно как единая операция:** переходы узла `FOLLOWER/CANDIDATE/LEADER`, обновление `leaderId`, сброс/запуск таймеров и heartbeats, обработка входящих/исходящих RPC end-to-end, формирование `/status` снимка.
- **Источники конкуренции:** HTTP-запросы (`/raft/*`, `/status`), `raft-election-timer`, `raft-heartbeat`, асинхронные RPC-вызовы/колбэки (`CompletableFuture.supplyAsync`).

## Оставшиеся ограничения реализации
- Нет репликации лога/FSM, commit/apply, снапшотов, pre-vote, динамической конфигурации.
- Персистируются только `currentTerm/votedFor`; `role/leaderId` не восстанавливаются.
- `AppendEntries` не проверяет согласованность лога и в допустимом терме возвращает `success=true`.
- Составные переходы узла не сериализованы единым локом/event-loop; `/status` может быть несогласованным снимком.
