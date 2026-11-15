# Состояние кода my-raft

Актуальный снимок реализации классов, логики Raft, персистентности, RPC, таймеров и оставшихся ограничений.

## Стек
- Java 21, Spring Boot 3.3.5, Maven
- Зависимости: `spring-boot-starter-web`, `spring-boot-configuration-processor`, `spring-boot-starter-test`
- Артефакт: `com.myraft:my-raft:0.0.1-SNAPSHOT`

## Структура пакетов
```
src/main/java/com/myraft/
  MyRaftApplication.java
  api/StatusController.java
  api/dto/StatusResponse.java
  config/RaftProperties.java, ClusterProperties.java, RestTemplateConfig.java
  raft/RaftNode.java
  raft/model/NodeRole.java, RaftState.java
  raft/rpc/RaftRpcController.java, RaftRpcService.java, RaftRpcClient.java
  raft/rpc/dto/RequestVoteRequest.java, RequestVoteResponse.java, AppendEntriesRequest.java, AppendEntriesResponse.java
  raft/storage/RaftStorage.java, FileRaftStorage.java, InMemoryRaftStorage.java, PersistedState.java
  raft/timer/ElectionTimer.java
src/main/resources/
  application.yml, logback-spring.xml
src/test/java/com/myraft/
  api/dto/StatusResponseJsonTest.java
  raft/RaftElectionTest.java
  raft/RaftNodeElectionTest.java
  raft/RaftStateAtomicityTest.java
  raft/rpc/RaftRpcServiceAtomicityTest.java
  raft/storage/FileRaftStorageTest.java
  raft/storage/InMemoryRaftStorage.java
```

## Конфигурация
- `RaftProperties` (`raft`): `nodeId`, `electionTimeoutMinMs`, `electionTimeoutMaxMs`, `heartbeatIntervalMs`, `storagePath`.
- `application.yml` (текущие значения): `electionTimeoutMinMs=5000`, `electionTimeoutMaxMs=8000`, `heartbeatIntervalMs=1000`, `storagePath=data/raft-state.json`.
- `ClusterProperties` (`cluster`): `peers` (пустые/blank значения фильтруются).
- `RestTemplateConfig`: connect/read timeout `200ms`.
- `logback-spring.xml`: консольный вывод с MDC `[node:..][term:..][role:..]`.

## Состояние и хранение
- `NodeRole`: `FOLLOWER`, `CANDIDATE`, `LEADER`.
- `PersistedState(term, votedFor)`, `EMPTY=(0,null)` — персистятся только `currentTerm` и `votedFor`.
- `RaftState` хранит `currentTerm`, `votedFor`, `role`, `leaderId`, `startTime`; загружает персистентное состояние из `RaftStorage` при создании. Защита через `ReentrantReadWriteLock`.
- Атомарные операции term/vote (persist-before-visible, под write-lock):
  - `startElection(String selfId)` — инкрементирует терм, голосует за себя, возвращает новый терм.
  - `grantVote(int candidateTerm, String candidateId)` → `GrantVoteResult(granted, termIncreased, term)` — атомарно решает выдачу голоса (не более одного голоса в терме).
  - `updateTermAndVote(int term, String votedFor)` — монотонный (no-op при `term < currentTerm`, `votedFor` не меняется при откате).
- `RaftStorage` (`load/persist`), реализации:
  - `FileRaftStorage` (`raft.storage.type=file`, `matchIfMissing=true`): запись через temp-файл + `fsync` + atomic move/fallback, при ошибке чтения бросает `IllegalStateException`.
  - `InMemoryRaftStorage` (`raft.storage.type=memory`): хранение в памяти, используется для неперсистентного режима.

## RPC и протокол (election-only)
- DTO: `RequestVoteRequest/Response`, `AppendEntriesRequest/Response` (журнал не передаётся, `entries` пустые для heartbeat).
- `RaftRpcController`: `POST /raft/requestVote`, `POST /raft/appendEntries`.
- `RaftRpcClient`: отправка через `RestTemplate` в `CompletableFuture.supplyAsync()`, при ошибке возвращает `null`.
- `RaftRpcService`:
  - `handleRequestVote`: решение по голосу через `raftState.grantVote(...)`; side-effects (`FOLLOWER`, `leaderId=null`, `electionTimer.reset()`, логи) выполняются по результату.
  - `handleAppendEntries`: stale term отклоняется без изменения `votedFor`; более высокий терм — монотонное обновление; при валидном heartbeat узел становится/остаётся `FOLLOWER`, обновляет лидера и сбрасывает таймер выборов.
  - Логируются получение/отправка `RequestVote`/`AppendEntries` и ответы.

## Ядро узла и таймеры
- `ElectionTimer`: однопоточный `ScheduledExecutorService` с `epoch` + синхронизацией, отбрасывает устаревшие таймауты; случайный таймаут в `[min,max]`.
- `RaftNode`:
  - Старт в `FOLLOWER`, запуск election timer.
  - `startElection()` вызывает `raftState.startElection(nodeId)`, переходит в `CANDIDATE`, работает с возвращённым термом, голосует за себя, рассылает `RequestVote`.
  - При кворуме становится `LEADER`, запускает периодические пустые `AppendEntries` (heartbeat scheduler).
  - Ответы с более высоким термом приводят к шагу вниз (`FOLLOWER`, сброс leaderId/heartbeat/timer).

## API
- `GET /status` (`StatusController`) возвращает `nodeId`, `role`, `currentTerm`, `leaderId`, `peers`, `uptime`, `isLeader` (сериализация поля `isLeader`).

## Тесты (`src/test`)
`mvn test`: **13 тестов, BUILD SUCCESS**.
- `StatusResponseJsonTest` (1)
- `RaftElectionTest` (3)
- `RaftNodeElectionTest` (1)
- `RaftStateAtomicityTest` (3) — монотонность `updateTermAndVote`, `startElection`, конкурентный `grantVote`
- `raft.rpc.RaftRpcServiceAtomicityTest` (3) — конкурентные `RequestVote`, сценарий без отката терма, stale `AppendEntries` не меняет `votedFor`
- `FileRaftStorageTest` (2)

## Оставшиеся ограничения/проблемы
1. **Нет репликации лога и FSM:** отсутствуют `log[]`, `prevLogIndex/prevLogTerm`, `entries` с данными, `commitIndex`/`lastApplied`, применение команд, снапшоты.
2. **Нет pre-vote:** таймаут выборов сразу увеличивает терм.
3. **Персистентность неполная:** сохраняются только `currentTerm`/`votedFor`; роль и `leaderId` не восстанавливаются.
4. **Проверки up-to-date log не реализованы:** нет сравнения `lastLogIndex/lastLogTerm` (неактуально до появления лога, но требуется для полноценного Raft).
5. **`AppendEntries` всегда `success=true` при допустимом терме:** нет проверки согласованности лога.
6. **Синхронизация распределена по компонентам:** term/vote атомарны, но смена роли/лидера и часть побочных эффектов выполняются отдельными шагами; `getLock()` остаётся доступным.
7. **RPC/тайминги эвристические:** фиксированные таймауты, обработка ошибок как `null`, отсутствие полноценной политики retry/backoff и лимитов исполнения.
