# План: репликация лога, KV state machine и REST API записи/чтения

**Статус: этап 1 реализован, этапы 2–4 — план.** Документ фиксирует схему, сверенную с
оригинальной статьёй D. Ongaro, J. Ousterhout «In Search of an Understandable Consensus
Algorithm (Raft)» (`https://raft.github.io/raft.pdf`, локальная копия `/tmp/opencode/raft.pdf`,
текст `/tmp/opencode/raft.txt`). Выполненные пункты этапа 1 отмечены `[x]` в §13;
остальные этапы остаются нереализованными.

## 1. Цель и границы

- Добавить к текущей election-only реализации репликацию лога и применение закоммиченных
  команд к внутренней key-value state machine.
- Добавить новые REST API **только для добавления и чтения лога**. `POST /query`,
  `GET /query?key`, `GET /log` из `docs/client_openapi.json` — целевой контракт, но на этом
  шаге **не реализовываются**.
- Клиентский запрос на вставку возвращает не просто «успешно», а **результат state machine**
  (например, значение, которое получилось в KV после применения команды).
- Сохранить текущую архитектуру: единый `nodeLock` в `RaftNode` сериализует все переходы,
  сетевые вызовы выполняются **вне лока**, сохранить текущий формат логирования
  (MDC `node/term/role`, outcome-based логирование в `RaftRpcService`).

## 1.1. Сверка с оригинальной статьёй (§5.2–5.4, Figure 2, §8)

| Пункт плана | Что говорит статья | Статус / правка |
|---|---|---|
| Лог с индекса с 1, запись содержит команду и term | Figure 2, «log[] … first index is 1»; §5.3 | ✅ совпадает |
| Лидер реплицирует, добавляет в свой лог и отвечает после apply | Figure 2, «If command received from client: append entry … respond after entry applied to state machine» | ✅ совпадает, §4 шаг 7 явно возвращает результат KV |
| `nextIndex`/`matchIndex`, инициализация `nextIndex = lastLogIndex+1`, reinit после выборов | Figure 2, §5.3 | ✅ совпадает; re-init в `tryBecomeLeaderLocked` добавлен в §3 |
| Проверка `prevLogIndex`/`prevLogTerm`, удаление конфликтующих и последующих записей | AppendEntries receiver rules 2–4, §5.3 | ✅ совпадает, §6 |
| Фолловер: `commitIndex = min(leaderCommit, индекс последней новой записи)` | AppendEntries rule 5 | ⚠️ уточнение: для пустого heartbeat «последней новой записи» нет — использовать `lastLogIndex` фолловера как безопасную верхнюю границу |
| Коммит только записей текущего терма по большинству | §5.4.2, Figure 2 rule: `log[N].term == currentTerm` | ✅ совпадает, §4 шаг 5; старые записи коммитятся только косвенно |
| Свежесть лога в `RequestVote`: сначала `lastLogTerm`, потом `lastLogIndex` | §5.4.1, RequestVote receiver rule 2 | ⚠️ добавлено в §5: сейчас поля есть в DTO, но всегда нули |
| NOOP-запись в начале терма | §8 (линейные reads) | ✅ совпадает, §7; нужна и для фиксации `commitIndex` своего терма |
| Применение `log[lastApplied]` к state machine при `commitIndex > lastApplied`, по порядку | Figure 2, All Servers; §5.3 | ✅ совпадает, §6 шаг 6 |
| Redirect клиента с фолловера на лидера | §5.1 | ✅ совпадает, §8; выбрано `503 + leaderId`, forward пометлен на будущее |
| `conflictTerm`/`conflictIndex` в ответе — оптимизация, не обязательная для корректности | §5.3, абзац про оптимизацию nextIndex | ⚠️ помечено опциональным, §6 |
| Персистентны `currentTerm`, `votedFor`, `log[]`; `commitIndex`/`lastApplied` volatile | Figure 2 «Persistent state on all servers» | ✅ в §9 выбран unified `PersistedState` с логом; `commitIndex`/`lastApplied` остаются volatile |
| Идемпотентность AppendEntries и GRANTED_REPEAT | §5.3 + текущая реализация | ✅ сохраняем текущую семантику Repeat |

**Итог сверки:** схема соответствует Raft; исправления внесены в §4–§9 (свежесть лога при
голосовании, коммит только текущего терма, NOOP, уточнение `min(leaderCommit, …)` для
heartbeat, опциональность conflict-оптимизации, volatile `commitIndex`).

## 2. Текущее состояние (точка старта)

- Персистируются только `currentTerm`/`votedFor` (`PersistedState`), атомарно и
  persist-before-visible внутри `RaftState`.
- `AppendEntriesRequest` уже содержит `prevLogIndex`, `prevLogTerm`, `entries`,
  `leaderCommit`, но они не используются; heartbeat — пустой `AppendEntries`.
- `RequestVoteRequest` уже содержит `lastLogIndex`/`lastLogTerm`, но они всегда нули.
- `AppendEntriesResponse` содержит только `term`/`success`; нет `lastIndex`/`conflict*`.
- `commitIndex`, `lastApplied`, `nextIndex[]`, `matchIndex[]` отсутствуют; лога как такового нет.
- Внутренний API: `POST /raft/requestVote`, `POST /raft/appendEntries`, `GET /status`.

## 3. Модель данных

- `LogEntry { long index, int term, Command command }`, индексы с 1.
- `Command` — полиморфная команда state machine:
  - `Put(key: String, value: JsonNode)` — пользовательская запись; `value` — любые JSON-данные
    (string/number/array/object/null), как в `HashtablePostInput`;
  - `NoOp` — служебная запись лидера, исключается из отдаваемого клиенту списка лога.
- State machine (внутренняя, не REST): `Map<String, JsonNode> kv`. Применение команды `Put`
  кладёт `value` по ключу и возвращает новое значение; `NoOp` ничего не меняет.
- Volatile: `commitIndex`, `lastApplied`; на лидере `nextIndex[peer]`, `matchIndex[peer]`.
  При переходе в `LEADER` (в `tryBecomeLeaderLocked`) `nextIndex[peer] = lastLogIndex + 1`,
  `matchIndex[peer] = 0`.

## 4. Ход записи (лидер)

1. Принять команду от клиента на лидере; если узел не лидер — redirect (см. §8).
2. Под `nodeLock`: проверить актуальность лидерства/терма, добавить entry в локальный лог
   с term = `currentTerm`, обновить `nextIndex` для себя.
3. Вне `nodeLock`: разослать `AppendEntries` пирам с `prevLogIndex`/`prevLogTerm`,
   `entries`, `leaderCommit`.
4. Под `nodeLock`: обработать ответы; при `success` обновить `nextIndex`/`matchIndex`.
5. Как только для некоторого N > `commitIndex` большинство `matchIndex[i] >= N` **и**
   `log[N].term == currentTerm` — установить `commitIndex = N`.
6. Применить записи от `lastApplied + 1` до `commitIndex` к KV в порядке индекса,
   обновить `lastApplied`.
7. Ответить клиенту результатом применённой state machine (значением) и индексом записи.

## 5. RequestVote: проверка свежести лога

- Заполнять `lastLogIndex`/`lastLogTerm` при рассылке `RequestVote`.
- Получатель отказывает в голосе, если его лог «свежее» лога кандидата: сначала сравнить
  `lastLogTerm`, при равенстве — `lastLogIndex`.
- Интеграция: проверка свежести добавляется в решение узла (`RaftNode`) рядом с
  `RaftState.grantVote`; в `RaftRpcService` добавляется событие
  `REJECTED_LOG_NOT_UP_TO_DATE` и префильтр, чтобы не логировать решение как обычное
  `REJECTED_ALREADY_VOTED`.
- При повторном голосе тому же кандидату сохраняется текущая классификация `GRANTED_REPEAT`
  и сброс таймера через `resetElectionTimerIfUnchanged(after)`.

## 6. AppendEntries: согласованность, коммит, apply

- Лидер: `prevLogIndex = nextIndex[peer] - 1`, `prevLogTerm = term(prevLogIndex)`,
  `entries = log[nextIndex .. lastLogIndex]`, `leaderCommit = commitIndex`.
- Фолловер под `nodeLock`:
  1. `term < currentTerm` → `success=false` (текущее `REJECTED_STALE_TERM`).
  2. Нет записи с `prevLogIndex`/`prevLogTerm` → `success=false`.
  3. Конфликт по индексу (другой term) → удалить конфликтующую запись и все последующие.
  4. Дописать новые записи; идемпотентно для уже имеющихся.
  5. `leaderCommit > commitIndex` → `commitIndex = min(leaderCommit, lastNewIndex)`; для пустого heartbeat (нет новых записей) верхняя граница — `lastLogIndex` фолловера.
  6. Применить `log[lastApplied+1 .. commitIndex]` к KV, обновить `lastApplied`.
- Расширить `AppendEntriesResponse`: `term`, `success`, `lastIndex`, опционально
  `conflictTerm`/`conflictIndex` для быстрого отката `nextIndex`.
- При отказе из-за рассинхрона лидер уменьшает `nextIndex` и повторяет (с оптимизацией
  по `conflictTerm/conflictIndex` — опционально, можно начать с простого decrement).

## 7. NOOP после избрания

- Сразу после перехода в `LEADER` лидер добавляет пустую `NoOp`-запись своего терма и
  реплицирует её. Это нужно, чтобы зафиксировать `commitIndex` своего терма и,
  как следствие, все записи предыдущих термов (§5.4.2).
- `NoOp` не попадает в `GET /log` и не меняет KV.

## 8. Работа с клиентом

- Запись принимает только лидер; фолловер возвращает отказ с указанием лидера
  (например, `503` + `leaderId`). **Forward на лидера пока не делаем**; возможность
  добавить его позже сохраняем здесь как отметку (не реализовано).
- Ответ вставки: **оба значения из state machine** — `previousValue` (до применения)
  и `value` (после применения), плюс `index` записи в логе. `NoOp` в ответ не попадает.
- `GET /log`: список пользовательских команд в порядке индекса, без `NoOp`.
- `GET /query?key` и `GET /log` из клиентского OpenAPI пока не реализовываются;
  оставляем внутренние endpoint-ы: `POST /raft/log`, `GET /raft/log`, опционально
  `GET /raft/kv`.

## 9. Хранение

- Расширить `PersistedState` до unified состояния: `term`, `votedFor`, `log[]`.
  **Лог сохраняется так же, как терм и голос** — в том же персистентном состоянии.
- `commitIndex`/`lastApplied` остаются volatile, по Figure 2 статьи их хранить не обязательно.
- Сохранить атомарную запись `FileRaftStorage` (tmp + `force(true)` + atomic move).
- Порядок фиксации: лог/терм/голос пишутся до видимого изменения состояния и до ответа
  на RPC (persist-before-visible).

## 10. Конкурентность и логирование

- Все мутации (лог, `commitIndex`, `lastApplied`, KV, `nextIndex`/`matchIndex`) — под
  тем же `nodeLock`; порядок локов `nodeLock` → `RaftState` → (log/kv).
- Сеть и ожидание ответов клиента (до коммита) — вне `nodeLock`; после сети контекст
  перепроверяется под локом.
- Сохранить текущие события и формат (MDC `nodeId/term/role`, решение и side-effects
  из `before/after`-снапшотов/outcome, без повторного чтения живого состояния).
- **Логирование новых операций** (уровень INFO для ключевых решений, DEBUG для деталей по пирам):
  - запись принята лидером: `client append accepted` — key, index, term, размер value;
  - запись добавлена в лог: `log entry appended` — index, term, тип команды (`PUT`/`NOOP`);
  - NOOP после избрания: `no-op entry appended` — index, term;
  - отправка репликации: `AppendEntries sent` (уже есть) дополнить `prevLogIndex`,
    `prevLogTerm`, `entries`, `leaderCommit`; детали по каждому пиру — DEBUG;
  - ответ фолловера: `log replicated to` — peer, matchIndex, success/failure;
  - рассинхрон: `log conflict truncated` — peer/индекс/терм, `nextIndex decremented`;
  - коммит: `commitIndex advanced` — old → new, основание (большинство `matchIndex` + term);
  - применение: `entry applied` — index, key (для `PUT`), `lastApplied`;
  - чтение/запись клиента: `client append responded` (index, previousValue/value),
    `client redirected` (leaderId), `log read` / `kv read` — с индексом/ключом;
  - отказ голоса по свежести: `vote rejected (log not up-to-date)` — candidate,
    `lastLogTerm/lastLogIndex` кандидата и локальные значения;
  - ошибки персистентности лога: `log persist failed` — с контекстом узла/терма/индекса.
- Все новые события логируются из уже зафиксированного контекста (`StateTransition`,
  outcome, снимок лога под `nodeLock`), чтобы не смешивать состояния разных переходов.

## 11. Тесты (план и результаты)

- `mvn test`: 19 текущих тестов не должны сломаться — проверено на этапе 1 (всего 35 зелёных).
- Новые unit: проверка `prevLogTerm` mismatch; truncation; идемпотентное appending;
  up-to-date vote rule; commit только записей текущего терма; NOOP; apply по порядку.
- Интеграция: 3 узла, записать значение, убедиться что все KV и `/log` совпадают;
  выключить лидера, проверить перевыборы и сохранность записей; записать на фолловере —
  redirect.

## 12. Принятые решения (бывшие открытые вопросы)

1. **API:** пока оставляем внутренние `POST /raft/log` + `GET /raft/log`
   (опционально `GET /raft/kv`). Целевые `POST /query`, `GET /query`, `GET /log`
   из `docs/client_openapi.json` не реализовываем.
2. **Ответ вставки:** оба значения state machine — `previousValue` и `value`,
   плюс `index` записи.
3. **Хранение:** лог входит в персистентное состояние наравне с `term`/`votedFor`
   (unified `PersistedState`, атомарная запись через `FileRaftStorage`).
4. **Фолловер:** отвечает отказом с `leaderId`, без форварда. Возможность добавить
   forward на лидера помечена в §8 и может быть реализована позже.

## 13. Этапы реализации и чек-листы

Разделы 1–12 остаются без изменений; ниже задаётся только **порядок внедрения**.
Отметка `[x]` означает, что пункт реализован и проверен. Чек-листы вместе покрывают
весь план (соответствие разделов и этапов — в конце этого раздела).

### Этап 1. Модель лога, персистентность и условия согласованности

- [x] `LogEntry { index, term, command }`, индексы с 1; `Command` = `Put(key, JsonNode)` или `NoOp`.
- [x] Внутреннее представление лога с операциями `lastIndex()`, `termAt(index)`,
      `append(...)`, `truncateFrom(index)`; все обращения идут под `nodeLock`.
- [x] `PersistedState` расширен до unified `{ term, votedFor, log[] }`; пустое состояние —
      term 0, `votedFor = null`, пустой лог.
- [x] `RaftStorage.load()/persist(...)` работают с unified состоянием;
      `FileRaftStorage` сохраняет атомарно (tmp + `force(true)` + atomic move),
      in-memory реализация обновлена соответственно.
- [x] Соблюдён persist-before-visible: лог/терм/голос фиксируются до публикации изменения
      и до ответа на RPC.
- [x] `RequestVoteRequest` заполняется `lastLogIndex`/`lastLogTerm` при рассылке
      (до этапа 1 всегда нули).
- [x] Правило «свежести» лога реализовано в решении узла: сначала `lastLogTerm`,
      при равенстве `lastLogIndex`; голос отказан, если лог голосующего свежее.
- [x] `RaftRpcService`: префильтр и событие `REJECTED_LOG_NOT_UP_TO_DATE`;
      семантика `GRANTED_REPEAT` и сброс таймера через `resetElectionTimerIfUnchanged` сохранены.
- [x] DTO готовы к репликации: `AppendEntriesRequest` использует `prevLogIndex`,
      `prevLogTerm`, `entries`, `leaderCommit`; `AppendEntriesResponse` расширен до
      `term`, `success`, `lastIndex`, опционально `conflictTerm`/`conflictIndex`.
- [x] `commitIndex`/`lastApplied` не персистируются (volatile, по Figure 2).
- [x] Юнит-тесты: сохранение/загрузка unified состояния, сравнение свежести лога,
      round-trip DTO; исходные 19 тестов зелёные, `/status` и выборы работают как раньше.
- [x] Логирование этапа: `vote rejected (log not up-to-date)`, `log persist failed`;
      MDC из зафиксированного снапшота, INFO для решений и DEBUG для деталей.
- [x] Проверка этапа: `mvn test` зелёный + smoke-старт 3 узлов (выборы и heartbeat работают).
- [x] Доки: обновить `docs/CODE-STATE.md` в части unified `PersistedState`, `RaftStorage`,
      DTO и префильтра `RequestVote`.

### Этап 2. Внутренняя KV state machine

- [ ] `Map<String, JsonNode> kv` и применение `Put` возвращают `previousValue` и `value`.
- [ ] `NoOp` ничего не меняет в KV и не попадает в список лога.
- [ ] `lastApplied` отслеживается; применяются только записи `(lastApplied, commitIndex]`
      строго в порядке индекса.
- [ ] Повторный apply уже применённой записи ничего не меняет (идемпотентность).
- [ ] Результат apply подготовлен для ответа клиенту: `previousValue`, `value`, `index`.
- [ ] Тесты: последовательность apply, `NoOp`, повторный apply, все типы JSON-значений
      (string/number/array/object/null).
- [ ] Логирование этапа: `entry applied` (index, key, `lastApplied`); проигнорированный
      повторный apply — DEBUG.
- [ ] Проверка этапа: `mvn test` зелёный + smoke-старт 3 узлов.
- [ ] Доки: добавить описание KV state machine и `lastApplied` в `docs/CODE-STATE.md`.

### Этап 3. Репликация и коммит

- [ ] При переходе в `LEADER`: `nextIndex[peer] = lastLogIndex + 1`,
      `matchIndex[peer] = 0` (в `tryBecomeLeaderLocked`).
- [ ] Сразу после избрания лидер добавляет `NoOp` своего терма и реплицирует его.
- [ ] `AppendEntries` несёт `prevLogIndex`, `prevLogTerm`, `entries`, `leaderCommit`;
      одна строка лога на раунд, детали по пирам — DEBUG.
- [ ] Получатель: отказ при `term < currentTerm` и при несовпадении `prevLog*/term`.
- [ ] Получатель: при конфликте удаляет запись и все последующие, затем дописывает
      новые записи идемпотентно.
- [ ] Получатель: `leaderCommit > commitIndex` → `min(leaderCommit, lastNewIndex)`,
      для пустого heartbeat верхняя граница — `lastLogIndex`.
- [ ] Лидер: на успех обновляет `nextIndex`/`matchIndex`; на отказ уменьшает `nextIndex`
      и повторяет (опционально с `conflictTerm`/`conflictIndex`).
- [ ] Коммит только если `N > commitIndex`, большинство `matchIndex[i] >= N`
      и `log[N].term == currentTerm`; старые записи коммитятся только косвенно.
- [ ] Изменение `commitIndex` запускает apply на всех узлах, `lastApplied` обновляется.
- [ ] Все мутации под `nodeLock`, сеть вне лока; после сети контекст перепроверяется
      (роль/терм), step-down при `term > currentTerm` сохранён.
- [ ] Тесты 3 узлов: расхождение логов, truncate, NOOP, коммит текущего терма,
      перевыборы и сохранность записей.
- [ ] Логирование этапа: `log entry appended`, `no-op entry appended`, дополненный
      `AppendEntries sent`, `log replicated to`, `log conflict truncated`,
      `commitIndex advanced`; MDC в async-колбэках и heartbeat-потоке, логи только из
      outcome/снапшотов, INFO для решений и DEBUG для деталей по пирам.
- [ ] Проверка этапа: `mvn test` зелёный + E2E на 3 узлах (старт, запись NoOp,
      `/status`, перевыборы, сохранность); при желании замер времени старта.
- [ ] Доки: `docs/CODE-STATE.md` — `nextIndex`/`matchIndex`, обновлённые `RaftNode` и
      `RaftRpcService`, границы `nodeLock`; `docs/PROJECT-OVERVIEW.md` — «лог реплицируется,
      API ещё нет».

### Этап 4. REST API и клиентский путь

- [ ] `POST /raft/log` (внутренний) принимает `{ key, value }` и работает только на лидере.
- [ ] Ответ вставки содержит `previousValue`, `value` и `index`; `NoOp` в ответе не участвует.
- [ ] Фолловер возвращает отказ с `leaderId` (`503`), без forward; пометка о возможности
      forward на лидера сохранена.
- [ ] `GET /raft/log` отдаёт пользовательские `Put` в порядке индекса, без `NoOp`.
- [ ] `GET /raft/kv` (опционально) читает значение по ключу.
- [ ] `POST /query`, `GET /query`, `GET /log` из `docs/client_openapi.json` **не реализованы**.
- [ ] Клиентский вызов записи ожидает коммита и apply, затем отвечает результатом state machine.
- [ ] Валидация входа (`key` непустой, `value` — любые JSON, включая `null`) и обработка ошибок.
- [ ] Тесты API: запись → чтение, запись на фолловере → redirect, `/log` без `NoOp`.
- [ ] Логирование этапа: `client append accepted`, `client append responded`,
      `client redirected`, `log read`/`kv read`; MDC выставляется в контроллерах.
- [ ] Финальная проверка: `mvn test` (исходные 19 + новые) и E2E на 3 узлах — старт,
      запись, `/status`, остановка лидера, перевыборы, сохранность данных, запись на
      фолловере → redirect; при желании контроль времени старта (baseline ~20.2 c
      с `-XX:TieredStopAtLevel=1`).
- [ ] Доки: `docs/CODE-STATE.md` — новые контроллеры и формат ответа;
      `docs/PROJECT-OVERVIEW.md` — актуальный статус; `docs/client_openapi.json`
      остаётся целевым контрактом; сверка из §1.1 и границы §1 остаются актуальными.
- [ ] Git-операции (add/commit/push) — только по явному запросу.

### Соответствие разделов плана этапам

| Раздел | Этап(ы) |
|---|---|
| §1 Цель и границы | 1–4 (границы соблюдены на каждом этапе) |
| §1.1 Сверка со статьёй | 1–4 (сверка остаётся актуальной) |
| §2 Текущее состояние | 1 (точка старта и проверка «не сломалось») |
| §3 Модель данных | 1 (лог/команды/персистентность), 2 (KV) |
| §4 Ход записи | 3 (репликация/коммит), 4 (ожидание apply и ответ) |
| §5 RequestVote | 1 |
| §6 AppendEntries | 1 (DTO), 3 (правила и apply) |
| §7 NOOP | 3 |
| §8 Работа с клиентом | 4 |
| §9 Хранение | 1 |
| §10 Конкурентность и логирование | сквозное через 1–4 |
| §11 Тесты | 1–4 (проверка после каждого этапа, финальная в 4) |
| §12 Принятые решения | 1 (персистентность лога), 2 (`previousValue`/`value`), 4 (API, redirect) |
