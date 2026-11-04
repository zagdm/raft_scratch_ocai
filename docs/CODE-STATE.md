# Состояние кода my-raft

Снимок на **2026-10-28**. Описывает реализацию классов, логику Raft, персистентность, RPC, таймеры и известные проблемы именно в коде.

## Стек
- Java 21, Spring Boot 3.3.5, Maven
- Зависимости: spring-boot-starter-web, spring-boot-configuration-processor, spring-boot-starter-test
- Артефакт: `com.myraft:my-raft:0.0.1-SNAPSHOT` (jar в `target/`)

## Структура пакетов
```
src/main/java/com/myraft/
  MyRaftApplication.java
  api/StatusController.java, api/dto/StatusResponse.java
  config/RaftProperties.java, config/ClusterProperties.java, config/RestTemplateConfig.java
  raft/RaftNode.java
  raft/model/NodeRole.java, raft/model/RaftState.java
  raft/rpc/RaftRpcController.java, raft/rpc/RaftRpcService.java, raft/rpc/RaftRpcClient.java, raft/rpc/dto/*
  raft/storage/RaftStorage.java, raft/storage/FileRaftStorage.java, raft/storage/PersistedState.java
  raft/timer/ElectionTimer.java
src/main/resources/
  application.yml, logback-spring.xml
src/test/java/com/myraft/
  api/dto/StatusResponseJsonTest.java
  raft/RaftElectionTest.java
  raft/RaftNodeElectionTest.java
  raft/storage/FileRaftStorageTest.java
  raft/storage/InMemoryRaftStorage.java
```

## Ключевые классы

### Конфигурация
- `RaftProperties` (prefix `raft`): `nodeId`, `electionTimeoutMinMs`, `electionTimeoutMaxMs`, `heartbeatIntervalMs`, `storagePath`. Текущие значения: 5000/8000/1000.
- `ClusterProperties` (prefix `cluster`): `peers` с фильтрацией пустых/blank значений.
- `RestTemplateConfig`: `connectTimeout/readTimeout = 200ms` (ранее 1000).
- `logback-spring.xml`: консольный вывод с MDC `[node:..][term:..][role:..]`, INFO по умолчанию.

### Состояние
- `NodeRole`: `FOLLOWER`, `CANDIDATE`, `LEADER`.
- `PersistedState` (record): `term`, `votedFor`. `EMPTY=(0,null)`. Сохраняются только `term`/`votedFor` (role/leaderId не персистятся).
- `RaftState`: хранит `currentTerm`, `votedFor`, `role`, `leaderId`, `startTime`. При конструировании грузит из `RaftStorage`. `updateTermAndVote(term,votedFor)` — единственный путь изменения term/vote: сначала `storage.persist()`, потом обновление полей в памяти. Защита `ReentrantReadWriteLock`. Доступ к локу через `getLock()` (опасный паттерн).

### Хранение
- `RaftStorage`: контракт `load()`/`persist(term,votedFor)`.
- `FileRaftStorage`: запись во временный файл в той же директории, `FileChannel.force(true)`, `ATOMIC_MOVE` с fallback на `REPLACE_EXISTING`. Каталоги создаются при необходимости. При отсутствии файла — `EMPTY`. При ошибке чтения — `IllegalStateException` (запрет тихого старта с term=0).
- `InMemoryRaftStorage`: тестовая реализация.

### RPC
- DTO: `RequestVoteRequest/Response`, `AppendEntriesRequest/Response` (журнал не передаётся).
- `RaftRpcController`: `POST /raft/requestVote`, `POST /raft/appendEntries` → `RaftRpcService`.
- `RaftRpcClient`: отправка через `CompletableFuture.supplyAsync()` с `RestTemplate`. При ошибке логирует DEBUG и возвращает `null`. `getPeers()` из `ClusterProperties`.
- `RaftRpcService`: обработка голосований/heartbeat. При `term > currentTerm` — шаг вниз, сброс голоса, `FOLLOWER`. При получении heartbeat не в FOLLOWER — переход в FOLLOWER, обновление leaderId, сброс таймера. `raft.leader.discovered` только при смене лидера, heartbeat в DEBUG. При голосовании за кандидата — персист голоса и сброс таймера выборов.

### Ядро Raft
- `ElectionTimer`: однопоточный `ScheduledExecutorService` `raft-election-timer`. Случайный таймаут в [min,max]. **Обновлён**: синхронизация через `synchronized(lock)`, `epoch` + `currentEpoch`, проверка поколения в `onTimeout()` для отбраковки устаревших лямбд, `timeoutCallback` `volatile`, `stop()` инкрементирует epoch. При колбэке лидера просто делает `reset()`.
- `RaftNode`: оркестратор. Старт — `FOLLOWER`, запуск таймера. По таймауту (если не лидер) — `startElection()`: `term++`, `updateTermAndVote(self,null)`? нет, `updateTermAndVote(newTerm, selfId)`, роль `CANDIDATE`, сброс таймера, голоса = 1, проверка кворума до рассылки, рассылка RequestVote асинхронно. При получении большинства голосов — `becomeLeader()` (только из CANDIDATE) → `LEADER`, `leaderId=self`, запуск heartbeats. Heartbeats через `raft-heartbeat` `ScheduledExecutorService`, фиксированный интервал 100мс, пустые AppendEntries. При ответе с бóльшим term — шаг вниз.

### API
- `StatusController`: `GET /status` — собирает данные из RaftState/RaftProperties/RaftRpcClient, считает uptime, логирует, обновляет/очищает MDC.
- `StatusResponse`: поля nodeId/role/currentTerm/leaderId/peers/isLeader/uptime. `isLeader` сериализуется как `isLeader` через `@JsonProperty` на геттере/сеттере.

## Модульные/юнит-тесты (src/test)
7 тестов, проходят (`mvn test` BUILD SUCCESS):
- `StatusResponseJsonTest.leaderFlagIsSerializedAsIsLeader`
- `RaftElectionTest` (3): followerInitialState, termAndVoteArePersisted, stateIsRestoredFromStorage
- `RaftNodeElectionTest.singleNodeBecomesLeaderWithoutPeers`
- `FileRaftStorageTest` (2): persistsAndReloadsState, overwritesPreviousState
- `InMemoryRaftStorage` — тестовый двойник

## Известные проблемы в коде
1. **ElectionTimer/gонки** (частично исправлено): добавлены синхронизация + epoch, но архитектура всё ещё чувствительна к таймингу; "старые" таймауты отбраковываются по поколению.
2. **Соотношение таймаутов**: снижены RPC-таймауты до 200мс, увеличены election timeout до 500-800мс, heartbeat до 100мс — уменьшает риск "запоздалых" ответов, но не устраняет все гонки.
3. **Отсутствие pre-vote** — узел при старте/сетевых событиях может агрессивно повышать term.
4. **Только term/votedFor персистятся** — роль/лидер теряются при рестарте.
5. **Нет репликации лога**: AppendEntries пустые, нет log/commitIndex/lastApplied.
6. **Синхронизация точечная**: часть синхронизации в RaftNode, `getLock()` у RaftState доступен извне (потенциальный риск инкапсуляции).
7. **Планировщики**: два однопоточных ScheduledExecutorService (election/heartbeat), нет централизованного управления.
8. **@EnableScheduling** включён, но прямых `@Scheduled` нет.
