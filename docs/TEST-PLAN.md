# План тестов: падение и перезапуск узлов in-memory

Статус: **реализовано** (`mvn test` — 63/63, BUILD SUCCESS).

## Цель и границы

Добавить детерминированные тесты отказоустойчивости в существующий in-memory harness
`src/test/java/com/myraft/raft/RaftReplicationTest.java` — без реальных процессов, Windows-скриптов,
JAR, фиксированных портов и ручного ожидания реальных таймаутов выборов.

Покрываемые сценарии:

- гибель лидера, выборы нового лидера, возвращение старого лидера;
- гибель фолловера, запись во время его отсутствия, догон лога после возвращения;
- сохранение персистентного состояния Raft при перезапуске (файловое хранилище).

Вне области этого плана: subprocess/E2E на реальных JVM, клиентские REST API (этап 4),
сетевые partition-тесты на HTTP, снапшоты.

## Текущая точка опоры

- `RaftReplicationTest` уже содержит `Cluster`, `Node`, `ManualTimer`, `RoutingClient`,
  `RecordingStateMachine` и `await(...)`.
- `RaftNode.shutdown()` останавливает `heartbeatScheduler`; переиспользовать тот же
  `RaftNode` после `shutdown()` нельзя — нужен новый экземпляр.
- `RaftState` при создании читает `PersistedState` из `RaftStorage`; `commitIndex`,
  `lastApplied`, `role`, `leaderId` не восстанавливаются (volatile по Figure 2).
- `InMemoryRaftStorage` хранит единственный `PersistedState`; один и тот же экземпляр
  можно передать новому `RaftState` при перезапуске узла.
- `RoutingClient.blocked` сейчас блокирует только исходящие `AppendEntries`
  (это нужно существующему тесту перевыборов: старый лидер должен успеть узнать новый
  терм через `RequestVote`). Полную гибель узла надо моделировать отдельным флагом
  `offline`, который отбрасывает **оба** направления RPC.

## Изменения harness (только тестовый код)

1. `Cluster`:
   - завести `Map<String, RaftStorage> storages`, создавать storage один раз на узел
     и переиспользовать при перезапуске;
   - сделать поля `Node` (State/Node/Service/Timer/Client/SM) не-final, чтобы подменить
     узел в `nodes`;
   - добавить `Node stop(String id)`: `node.shutdown()`, `offline.add(id)`, удалить узел
     из маршрутизации, вернуть «замороженный» `Node` для проверок его старого состояния;
   - добавить `Node restart(String id)`: создать новые `RaftState` (из того же storage),
     `ManualTimer`, `RoutingClient`, `RecordingStateMachine`, `RaftNode`, `RaftRpcService`,
     положить в `nodes`, снять `offline`, вызвать `node.init()` и вернуть новый `Node`;
   - `close()` должен безопасно закрывать и живые, и уже остановленные узлы.

2. `RoutingClient`:
   - добавить `Set<String> offline`; `requestVote` и `appendEntries` возвращают `null`
     для offline-узлов;
   - оставить существующую семантику `blocked` (только `AppendEntries`) как имитацию
     одностороннего разделения сети, не смешивать её с `offline`.

3. Хелперы проверок:
   - `awaitCommit(Node node, long index, String message)`;
   - `assertConverged(Node... nodes)` — одинаковые `RaftLog`, `commitIndex`, `lastApplied`;
   - `assertApplied(Node node, List<Long> indexes)` — пользовательские индексы в SM,
     `NoOpCommand` в SM не попадает, но продвигает `lastApplied`.

4. Новые тесты добавить в тот же `RaftReplicationTest`; production-код не трогать.

## Тест 1. Гибель лидера → новый лидер → возврат старого лидера

Подготовка:

- `Cluster(3)`, `node-1` выигрывает выборы в терме 1, NoOp `index=1`;
- записать пользовательскую команду `PUT key=v` через `node-1.append(...)` → `index=2`,
  реплицировать, дождаться `commitIndex=2` на всех узлах; `sm.indexes == [2]` везде.

Шаги:

1. `Node oldLeader = cluster.stop("node-1")`: лидер выключен, его state заморожен
   (терм 1, роль LEADER).
2. `cluster.node("node-2").fireElection()` — второй узел выигрывает в терме 2
   (голос `node-3`), пишет NoOp `index=3`, коммит `index=3` кворумом `{node-2, node-3}`.
3. Проверить: `node-2` — LEADER term 2; `node-3` — FOLLOWER `leaderId=node-2`,
   `commitIndex=3`, `lastApplied=3`, `sm.indexes == [2]`.
4. `Node revived = cluster.restart("node-1")` с тем же `InMemoryRaftStorage`.
5. `node-2.replicate()` (при необходимости цикл до сходимости), потому что `nextIndex`
   нового лидера для `node-1` равен 3.
6. Проверить:
   - `oldLeader.state.getCurrentTerm() == 1` (старый объект действительно мёртв);
   - `revived.state.getCurrentTerm() == 2`, роль FOLLOWER, `leaderId=node-2`;
   - логи `revived`, `node-2`, `node-3` равны, NoOp `index=3` присутствует;
   - `commitIndex == 3`, `lastApplied == 3`, `revived.sm.indexes == [2]` —
     закоммиченная пользовательская запись не потеряна и применена заново из лога
     (SM после рестарта создаётся пустым);
   - повторная репликация не меняет лог (идемпотентность).

## Тест 2. Гибель фолловера → запись → догон после возвращения

Подготовка: `Cluster(3)`, `node-1` лидер терма 1, NoOp `index=1`, `commitIndex=1`.

Шаги:

1. `cluster.stop("node-2")`.
2. `node-1.append(PUT key=k2 value=v2)` → `index=2`, репликация только на `node-3`;
   кворум `{node-1, node-3}` даёт `commitIndex=2`, `sm.indexes == [2]` на живых узлах.
3. `Node revived = cluster.restart("node-2")`: у него сохранился лог `index=1`,
   но `commitIndex=0`, `lastApplied=0`, пустой SM.
4. `node-1.replicate()` до сходимости — `nextIndex[node-2]` у лидера остался 2,
   отправляется запись `index=2` с `leaderCommit=2`.
5. Проверить: логи всех трёх узлов равны, `commitIndex == 2`, `lastApplied == 2`,
   `revived.sm.indexes == [2]`, роль FOLLOWER, `leaderId=node-1`, терм 1.
6. Дополнительно: `node-1` оставался лидером всё время (перевыборов не было).

## Тест 3. Персистентность term/vote/log при перезапуске (FileRaftStorage)

Не требует `RaftNode` — проверяется уровень `RaftState` + `FileRaftStorage` с `@TempDir`.

1. Создать `RaftState(new FileRaftStorage(tempDir.resolve("node-1.json")))`.
2. `updateTermAndVote(3, "node-2")`; `appendLogEntries([LogEntry(1, 1, NoOpCommand),
   LogEntry(2, 2, UserCommand(PUT k=v))])`; `advanceCommitIndex(2)`,
   `advanceLastApplied(2)`.
3. Создать новый `RaftState(new FileRaftStorage(...тот же файл...))`.
4. Проверить: `currentTerm == 3`, `votedFor == "node-2"`, лог в точности совпадает
   (`formatVersion=2`), а `commitIndex == 0`, `lastApplied == 0`, роль FOLLOWER,
   `leaderId == null`.
5. Негатив (уже покрыто `FileRaftStorageTest`, здесь только не дублировать): файл
   без `formatVersion` → fail-fast.

## Возможные тесты на потом (после базовых трёх)

- Возврат лидера с расходящимся некоммитнутым хвостом: старый лидер успел дописать запись,
  которую никто не закоммитил; новый лидер вырезает её через `conflictTerm/conflictIndex`.
- Двойной failover подряд и сохранность той же закоммиченной записи.
- Перезапуск узла с `--raft.storage.type=file` в subprocess E2E (уровень 2 плана).

## Критерии готовности

- Изменения только в `src/test/java/.../RaftReplicationTest.java` и, при тесте 3,
  новый тестовый класс рядом с `FileRaftStorageTest`.
- `mvn test -Dtest=RaftReplicationTest` зелёный (7/7); затем полный `mvn test`
  ожидаемо `63/63` (60 текущих + 3 новых).
- Тесты не используют реальные `Thread.sleep` для ожидания выборов, не слушают
  фиксированные порты, не запускают `java`/`.cmd`, не читают `C:\temp`.
- Время прогона нового набора — единицы секунд, без Windows-зависимостей.
- Документация: после реализации отметить пункты в этом файле как выполненные
  и при необходимости добавить краткую строку в `docs/CODE-STATE.md`.

## Чеклист реализации

- [x] Расширить `Cluster`/`Node`: `stop(id)`, `restart(id)`, общий `RaftStorage`.
- [x] Добавить `offline` в `RoutingClient`, сохранить семантику `blocked`.
- [x] Тест 1: гибель лидера и возврат с сохранением пользовательской записи.
- [x] Тест 2: гибель фолловера и догон лога.
- [x] Тест 3: `FileRaftStorage` + `@TempDir`, term/vote/log сохраняются, commit/lastApplied — нет.
- [x] Прогнать `mvn test -Dtest=RaftReplicationTest` и `mvn test`.

Фактические результаты: `RaftReplicationTest` — 7/7, `FileRaftStorageRestartTest` — 1/1,
полный прогон — 63/63, BUILD SUCCESS. Production-код не менялся; новые тесты используют
только in-memory маршрутизацию и `@TempDir`, реальные таймауты выборов не ожидаются.
