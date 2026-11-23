# Контекст тестирования my-raft (Maven)

Актуально для текущего состояния: Raft с выборами лидера, репликацией лога, коммитом по большинству
и внутренним REST API записи/чтения; Java 21 / Spring Boot 3.3.5.

Все проверки в этом файле — Maven-тесты: `mvn test` — **68/68, BUILD SUCCESS**.
Тесты не запускают реальные JVM, `.cmd`-скрипты, JAR и не слушают фиксированные порты.

## Как запускать

```bash
mvn test                                  # весь набор
mvn test -Dtest=RaftReplicationTest        # один класс
mvn test -Dtest=RaftClientApiTest#blankKeyIsRejected  # один метод
mvn clean package -DskipTests              # сборка без тестов
```

Maven в PATH отсутствует; на этой машине используется бандл IntelliJ IDEA, запуск из WSL — через
`powershell.exe` (команды и ошибки WSL ↔ Windows описаны в `docs/ENVIRONMENT.md`, §8.2).
Подробные отчёты по каждому классу — в `target/surefire-reports/`.

## Как устроены тесты

- JUnit 5; только один тест (`MyRaftApplicationTests`) поднимает полный Spring-контекст,
  остальные — обычные unit/integration-тесты в памяти.
- Контроллеры проверяются через `MockMvcBuilders.standaloneSetup(...)` — без HTTP-сервера и портов.
- Сеть заменяется подклассом `RaftRpcClient`, который вызывает `RaftRpcService` нужного узла
  напрямую; таймер выборов — ручной (`ElectionTimer` с «выстрелом» из теста). Это убирает
  ожидание реальных election/heartbeat-таймаутов из тестов.
- Хранилище по умолчанию — `InMemoryRaftStorage`; один и тот же экземпляр передаётся новому
  `RaftState` при имитации перезапуска узла. Файловое хранилище проверяется отдельно с `@TempDir`.
- Конкурентность проверяется реальными потоками в тестах атомарности term/vote.

## Что покрывают тесты

| Класс | Тестов | Что проверяет |
|---|---:|---|
| `MyRaftApplicationTests` | 1 | загрузка Spring-контекста приложения |
| `api.StatusResponseJsonTest` | 1 | JSON `GET /status`, свойство `isLeader` |
| `api.RaftClientApiTest` | 10 | `POST /raft/log` → чтение значения, JSON `null`, `503` с `leaderId` с фолловера и `leaderId=null` без лидера, `400` на пустой ключ, `504` apply-timeout с догоном после рестарта; `found=false` против JSON `null`; `GET /raft/log` без NoOp после смены лидера/рестарта; round-trip JSON-типов и `previousValue` |
| `raft.RaftElectionTest` | 3 | начальное состояние FOLLOWER, персистентность term/vote, восстановление состояния из storage |
| `raft.RaftNodeElectionTest` | 1 | одиночный узел становится лидером без пиров |
| `raft.RaftStateAtomicityTest` | 4 | монотонность `updateTermAndVote`, выборы и self-vote, запись лога вместе с term/vote, ровно один голос в терме при конкуренции |
| `raft.RaftNodeApplyTest` | 4 | apply только закоммиченных записей, пропуск NoOp, идемпотентность, ошибка apply не двигает `lastApplied`, монотонность/ограничение `commitIndex` |
| `raft.RaftReplicationTest` | 7 | NoOp и коммит кворумом, отказ по `prevLog*` и truncate хвоста, запись старого терма не коммитится, перевыборы сходятся, `conflictTerm/conflictIndex`, гибель лидера/фолловера и догон лога после рестарта |
| `raft.model.RaftLogTest` | 5 | пустой лог, неизменяемость append, truncate, сравнение «свежести» по §5.4.1, отказ от несплошных индексов |
| `raft.model.RaftCommandJsonTest` | 5 | JSON round-trip конверта `USER`/`NOOP`, отказ неизвестных типов и старого формата этапа 1 |
| `raft.fsm.kv.KvStateMachineTest` | 5 | PUT/GET и `previousValue`, все JSON-типы, `null` → `NullNode`, детерминизм, отказ неизвестной команды |
| `raft.rpc.RaftRpcOutcomeTest` | 5 | классификация всех событий `RequestVote`/`AppendEntries`, ответ из снапшота, повторный голос и сброс таймера через узел |
| `raft.rpc.RaftRpcServiceAtomicityTest` | 3 | параллельные голоса: один голос в терме, отсутствие отката терма, устаревший `AppendEntries` не меняет голос |
| `raft.rpc.RaftVoteFreshnessTest` | 5 | правило свежести лога при голосовании, step-down без выдачи голоса, префильтр сервиса |
| `raft.rpc.dto.RaftDtoJsonTest` | 3 | JSON RPC-DTO: метаданные лога в `RequestVote`, записи и `leaderCommit` в `AppendEntries`, `lastIndex`/конфликт-поля в ответе |
| `raft.storage.FileRaftStorageTest` | 5 | запись/чтение файла, перезапись, лог вместе с term/vote, fail-fast на отсутствии/старом/неизвестном `formatVersion` |
| `raft.storage.FileRaftStorageRestartTest` | 1 | рестарт: term/vote/лог и `formatVersion=2` восстанавливаются, `commitIndex`/`lastApplied`/роль/`leaderId` сбрасываются |

Итого 68 тестов в 17 классах. Отказоустойчивость и клиентский REST API покрываются тестами
`RaftReplicationTest`, `FileRaftStorageRestartTest` и `RaftClientApiTest`.

## Что не покрывается Maven-тестами

- subprocess/E2E на реальных JVM и Windows `.cmd`-скриптах;
- реальный HTTP, фиксированные порты, сетевые partition/задержки;
- снапшоты, pre-vote, динамическая конфигурация кластера;
- внешний контракт `docs/client_openapi.json` (`/query`, `/log` с форматом ошибок FastAPI) и forward на лидера — не реализованы.

## Ручная проверка кластера

Не является частью `mvn test` и в этом документе не описывается. Команды запуска/остановки узлов
на тестовых портах 18080–18082 — в `docs/ENVIRONMENT.md` (§10), порядок закрытия окна узла — там же
(§7, п. 6).
