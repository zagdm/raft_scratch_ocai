# Обзор проекта my-raft

Актуальное описание проекта: назначение, структура, запуск, конфигурация и текущее состояние реализации.

## Назначение и границы
- Минимальная реализация Raft (election-only: выбор лидера + heartbeats) на **Spring Boot 3.3.5 / Java 21 / HTTP JSON**.
- Реализовано: выбор лидера, периодические пустые `AppendEntries` (heartbeats), `GET /status`, персистентность `currentTerm`/`votedFor`.
- Не реализовано: репликация лога, commit/apply, клиентские команды, снапшоты, pre-vote, динамическая конфигурация кластера.
- Без Docker и gRPC. Логи — в консоль. Запуск рассчитан на Windows (каждый узел в отдельном окне консоли).

## Структура репозитория
- `src/main/java/com/myraft/` — приложение, API, конфигурация, Raft core (`raft/model`, `raft/rpc`, `raft/storage`, `raft/timer`).
- `src/main/resources/` — `application.yml`, `logback-spring.xml`.
- `src/test/java/com/myraft/` — модульные/интеграционные тесты (JUnit 5).
- `scripts/runtime/` — Windows `.cmd` скрипты запуска/остановки/сборки (`start-node.cmd`, `start-cluster.cmd`, `stop-cluster.cmd`, `build-jar.cmd`).
- `scripts/tests/` — заготовка под тестовые скрипты.
- `docs/` — документация (`CODE-STATE.md`, `ENVIRONMENT.md`, `TEST-CONTEXT.md`, `PROJECT-OVERVIEW.md`).

## Сборка, запуск, тесты
- Сборка: `mvn clean package -DskipTests` (или `scripts\runtime\build-jar.cmd`).
- Тесты: `mvn test` — **19/19, BUILD SUCCESS** (текущий набор: `MyRaftApplicationTests` (`@SpringBootTest`), `StatusResponseJsonTest`, `RaftElectionTest`, `RaftNodeElectionTest`, `RaftStateAtomicityTest`, `raft.rpc.RaftRpcOutcomeTest`, `raft.rpc.RaftRpcServiceAtomicityTest`, `FileRaftStorageTest`).
- Запуск кластера: `scripts\runtime\start-cluster.cmd [total-nodes] [base-port] [--jvm "..."]` (цикл вызовов `start-node.cmd`).
- Запуск узла: `scripts\runtime\start-node.cmd <node> [total-nodes] [base-port] [--jvm "..."]`.
- Остановка: `scripts\runtime\stop-cluster.cmd` (закрытие окна узла также останавливает узел).

## Конфигурация и поведение
- Таймауты/интервалы (по умолчанию): election `5000-8000ms`, heartbeat `1000ms`, RPC `RestTemplate` `200ms`.
- Хранилище: in-memory по умолчанию (состояние не переживает перезапуск); файловый режим включается `-Draft.storage.type=file` (`raft.storage-path`, напр. `data/node-N.json`).
- Состояние Raft: персистятся только `currentTerm` и `votedFor`.
- Атомарность term/vote (F1/F2): `RaftState.startElection`/`grantVote` и монотонный `updateTermAndVote` выполняются под write-lock с persist-before-visible; `RaftRpcService`/`RaftNode` используют атомарные решения/возвращаемый терм, предотвращая двойное голосование в терме и откат терма.
- Атомарность переходов узла: единый `nodeLock` в `RaftNode` сериализует `role`/`leaderId`/таймер/heartbeats и обработку входящих RPC и ответов; сеть — вне лока. Порядок блокировок: `nodeLock` → лок `RaftState`. `RaftRpcService` — тонкий адаптер (snapshot-prefilter'ы stale term / already voted / foreign leader, уточнение `GRANTED_REPEAT`, логирование outcome, ответ из `after`-снапшота); сброс таймера при повторном голосе выполняется узлом под `nodeLock`.
- `AppendEntries` при равном терме от другого лидера (если текущий `leaderId` известен) не принимается — `REJECTED_FOREIGN_LEADER`; heartbeat от известного лидера и более высокий терм принимаются как раньше.
- Логирование: консоль, MDC `[node/term/role]`; события: `starting election`, `RequestVote sent term=... peers=N` (один раз за раунд; детали по пирам — DEBUG в `RaftRpcClient`), `vote granted`, `became leader`, `stepping down`, `vote rejected`, `leader changed`/`discovered new leader`, `AppendEntries sent`, `AppendEntries received`, ответы/игнорирование heartbeat.

## Окружение и эксплуатационные заметки
- Основное окружение разработки/проверок — Windows 10, JDK 21, Maven из IntelliJ (часто запускается из WSL через `powershell.exe`), пути с кириллицей/пробелами требуют аккуратных команд (см. `docs/ENVIRONMENT.md`).
- Процедура инфраструктурной проверки запуска/восстановления узлов описана в `docs/TEST-CONTEXT.md`. Следует учитывать задержки, связанные с увеличенными election/heartbeat таймаутами.
