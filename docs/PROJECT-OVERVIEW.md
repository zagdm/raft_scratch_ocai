# Обзор проекта my-raft

Актуальное описание проекта: назначение, структура, запуск, конфигурация и текущее состояние реализации.

## Назначение и границы
- Минимальная реализация Raft (выбор лидера + heartbeats + репликация лога и коммит + внутренний REST API записи/чтения) на **Spring Boot 3.3.5 / Java 21 / HTTP JSON**.
- Реализовано: выбор лидера, `AppendEntries` с репликацией лога и `leaderCommit`, NoOp после избрания, коммит по большинству и apply, `GET /status`, персистентность `currentTerm`/`votedFor`/лога; `POST /raft/log`, `GET /raft/log`, `GET /raft/kv` с ожиданием apply на лидере и redirect (`503 + leaderId`) на фолловере.
- Не реализовано: внешний контракт `docs/client_openapi.json` (`POST /query`, `GET /query`, `GET /log`), forward на лидера, снапшоты, pre-vote, динамическая конфигурация кластера.
- Без Docker и gRPC. Логи — в консоль. Запуск рассчитан на Windows (каждый узел в отдельном окне консоли).

## Структура репозитория
- `src/main/java/com/myraft/` — приложение, API, конфигурация, Raft core (`raft/model`, `raft/fsm`, `raft/fsm/kv`, `raft/rpc`, `raft/rpc/dto`, `raft/storage`, `raft/timer`).
- `src/main/resources/` — `application.yml`, `logback-spring.xml`.
- `src/test/java/com/myraft/` — модульные/интеграционные тесты (JUnit 5).
- `scripts/runtime/` — Windows `.cmd` скрипты запуска/остановки/сборки (`start-node.cmd`, `start-cluster.cmd`, `stop-cluster.cmd`, `build-jar.cmd`).
- `scripts/tests/` — заготовка под тестовые скрипты.
- `docs/` — документация (`CODE-STATE.md`, `ENVIRONMENT.md`, `TEST-CONTEXT.md`, `PROJECT-OVERVIEW.md`).

## Сборка, запуск, тесты
- Сборка: `mvn clean package -DskipTests` (или `scripts\runtime\build-jar.cmd`).
- Тесты: `mvn test` — **63/63, BUILD SUCCESS** (19 исходных + 16 этапа 1 + 15 этапа 2 + 5 этапа 3 + 5 этапа 4 + 3 плана отказоустойчивости). Новые плана отказоустойчивости (`docs/TEST-PLAN.md`): гибель лидера и возврат старого, гибель фолловера и догон лога, перезапуск с `FileRaftStorage` (term/vote/лог сохраняются, commit/lastApplied нет). Сборка: `mvn clean package -DskipTests` — BUILD SUCCESS.
- Запуск кластера: `scripts\runtime\start-cluster.cmd [total-nodes] [base-port] [--jvm "..."]` (цикл вызовов `start-node.cmd`).
- Запуск узла: `scripts\runtime\start-node.cmd <node> [total-nodes] [base-port] [--jvm "..."]`.
- Остановка: `scripts\runtime\stop-cluster.cmd` (закрытие окна узла также останавливает узел).

## Конфигурация и поведение
- Таймауты/интервалы (по умолчанию): election `5000-8000ms`, heartbeat `1000ms`, RPC `RestTemplate` `200ms`.
- Хранилище: in-memory по умолчанию (состояние не переживает перезапуск); файловый режим включается `-Draft.storage.type=file` (`raft.storage-path`, напр. `data/node-N.json`).
- Состояние Raft: персистятся `currentTerm`, `votedFor` и реплицируемый лог (`formatVersion=2`); `commitIndex`/`lastApplied`/роль/`leaderId`/`nextIndex`/`matchIndex` **не** восстанавливаются (volatile, по Figure 2).
- Атомарность term/vote (F1/F2): `RaftState.startElection`/`grantVote` и монотонный `updateTermAndVote` выполняются под write-lock с persist-before-visible; `RaftRpcService`/`RaftNode` используют атомарные решения/возвращаемый терм, предотвращая двойное голосование в терме и откат терма.
- Атомарность переходов узла: единый `nodeLock` в `RaftNode` сериализует `role`/`leaderId`/таймер/heartbeats/`nextIndex`/`matchIndex`/лог/`commitIndex`/apply и обработку входящих RPC и ответов; сеть — вне лока. Порядок блокировок: `nodeLock` → лок `RaftState`. `RaftRpcService` — тонкий адаптер (snapshot-prefilter'ы stale term / already voted / foreign leader, уточнение `GRANTED_REPEAT`, логирование outcome, ответ из `after`-снапшота); сброс таймера при повторном голосе выполняется узлом под `nodeLock`.
- Репликация: лидер после избрания пишет `NoOpCommand` своего терма, рассылает `AppendEntries` с персональным `nextIndex`, проверяет `prevLogIndex/prevLogTerm` у фолловера, при конфликте вырезает хвост; по ответам обновляет `matchIndex` (с подсказками `conflictTerm`/`conflictIndex` при отказе). Коммит только записей текущего терма по большинству; фолловер продвигает `commitIndex` по `leaderCommit`. `UserCommand` применяется к `StateMachine`, `NoOpCommand` — только продвигает `lastApplied`.
- `AppendEntries` при равном терме от другого лидера (если текущий `leaderId` известен) не принимается — `REJECTED_FOREIGN_LEADER`; несовпадение `prevLog*` даёт `REJECTED_LOG_MISMATCH`; heartbeat от известного лидера и более высокий терм принимаются как раньше.
- Клиентский API (внутренний): `POST /raft/log {"key":..., "value":<any JSON>}` — только лидер, ждёт коммита и apply (`raft.client.apply-timeout-ms=5000`), отвечает `{"previousValue":..., "value":..., "index":...}`; фолловер — `503 {"error":..., "leaderId":...}` без forward; `GET /raft/log` — только пользовательские записи без NoOp; `GET /raft/kv?key=...` — `{"key":..., "value":..., "found":...}`. Пустой `key` — `400`, таймаут apply — `504`.
- Логирование: консоль, MDC `[node/term/role]`; события: `starting election`, `RequestVote sent term=... peers=N` (один раз за раунд; детали по пирам — DEBUG в `RaftRpcClient`), `vote granted`, `became leader`, `stepping down`, `vote rejected`, `leader changed`/`discovered new leader`, `AppendEntries sent term=... peers=N entries=...`, `AppendEntries received`, `no-op entry appended`, `log entry appended`, `log conflict truncated`, `log mismatch rejected`, `commitIndex advanced`, `client append accepted/responded`, `client redirected`, `log read`, `kv read`, ответы/игнорирование heartbeat.

## Окружение и эксплуатационные заметки
- Основное окружение разработки/проверок — Windows 10, JDK 21, Maven из IntelliJ (часто запускается из WSL через `powershell.exe`), пути с кириллицей/пробелами требуют аккуратных команд (см. `docs/ENVIRONMENT.md`).
- Процедура инфраструктурной проверки запуска/восстановления узлов описана в `docs/TEST-CONTEXT.md`. Следует учитывать задержки, связанные с увеличенными election/heartbeat таймаутами.
