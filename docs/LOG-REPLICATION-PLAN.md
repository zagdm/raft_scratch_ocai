# План: репликация лога, state machine (первая конкретизация — KV) и REST API записи/чтения

**Статус: этапы 1–2 реализованы, этапы 3–4 — план (уточнён для абстрактной state machine).** Документ фиксирует схему, сверенную с
оригинальной статьёй D. Ongaro, J. Ousterhout «In Search of an Understandable Consensus
Algorithm (Raft)» (`https://raft.github.io/raft.pdf`, локальная копия `/tmp/opencode/raft.pdf`,
текст `/tmp/opencode/raft.txt`). Выполненные пункты этапов 1–2 отмечены `[x]` в §13;
этапы 3–4 остаются нереализованными.

## 1. Цель и границы

- Добавить к текущей election-only реализации репликацию лога и применение закоммиченных
  команд к внутренней state machine через абстрактный интерфейс. Первая конкретизация —
  key-value хранилище; ядро Raft не должно зависеть от KV-семантики.
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
| Лидер реплицирует, добавляет в свой лог и отвечает после apply | Figure 2, «If command received from client: append entry … respond after entry applied to state machine» | ✅ совпадает, §4 шаг 7 явно возвращает результат apply |
| `nextIndex`/`matchIndex`, инициализация `nextIndex = lastLogIndex+1`, reinit после выборов | Figure 2, §5.3 | ✅ совпадает; re-init в `tryBecomeLeaderLocked` добавлен в §3 |
| Проверка `prevLogIndex`/`prevLogTerm`, удаление конфликтующих и последующих записей | AppendEntries receiver rules 2–4, §5.3 | ✅ совпадает, §6 |
| Фолловер: `commitIndex = min(leaderCommit, индекс последней новой записи)` | AppendEntries rule 5 | ⚠️ уточнение: для пустого heartbeat «последней новой записи» нет — использовать `lastLogIndex` фолловера как безопасную верхнюю границу |
| Коммит только записей текущего терма по большинству | §5.4.2, Figure 2 rule: `log[N].term == currentTerm` | ✅ совпадает, §4 шаг 5; старые записи коммитятся только косвенно |
| Свежесть лога в `RequestVote`: сначала `lastLogTerm`, потом `lastLogIndex` | §5.4.1, RequestVote receiver rule 2 | ✅ реализовано на этапе 1; до этапа 1 поля в DTO были всегда нули |
| NOOP-запись в начале терма | §8 (линейные reads) | ✅ совпадает, §7; нужна и для фиксации `commitIndex` своего терма; в плане это Raft-уровневый маркер, не команда state machine |
| Применение `log[lastApplied]` к state machine при `commitIndex > lastApplied`, по порядку | Figure 2, All Servers; §5.3 | ✅ совпадает, §6 шаг 6 |
| Redirect клиента с фолловера на лидера | §5.1 | ✅ совпадает, §8; выбрано `503 + leaderId`, forward пометлен на будущее |
| `conflictTerm`/`conflictIndex` в ответе — оптимизация, не обязательная для корректности | §5.3, абзац про оптимизацию nextIndex | ⚠️ помечено опциональным, §6 |
| Персистентны `currentTerm`, `votedFor`, `log[]`; `commitIndex`/`lastApplied` volatile | Figure 2 «Persistent state on all servers» | ✅ в §9 выбран unified `PersistedState` с логом; `commitIndex`/`lastApplied` остаются volatile |
| Идемпотентность AppendEntries и GRANTED_REPEAT | §5.3 + текущая реализация | ✅ сохраняем текущую семантику Repeat |

**Итог сверки:** схема соответствует Raft; исправления внесены в §4–§9 (свежесть лога при
голосовании, коммит только текущего терма, NOOP, уточнение `min(leaderCommit, …)` для
heartbeat, опциональность conflict-оптимизации, volatile `commitIndex`).

Абстракция state machine (§3) — инженерное решение, а не требование статьи: статья говорит
о произвольной детерминированной replicated state machine, KV — лишь первая конкретизация.

## 2. Текущее состояние (точка старта до этапа 1)

После этапа 1 добавлены модель лога, unified `PersistedState` и свежесть лога в `RequestVote`;
актуальные контракты — в `docs/CODE-STATE.md`. Ниже — историческая точка старта.

- Персистируются только `currentTerm`/`votedFor` (`PersistedState`), атомарно и
  persist-before-visible внутри `RaftState`.
- `AppendEntriesRequest` уже содержит `prevLogIndex`, `prevLogTerm`, `entries`,
  `leaderCommit`, но они не используются; heartbeat — пустой `AppendEntries`.
- `RequestVoteRequest` уже содержит `lastLogIndex`/`lastLogTerm`, но они всегда нули.
- `AppendEntriesResponse` содержит только `term`/`success`; нет `lastIndex`/`conflict*`.
- `commitIndex`, `lastApplied`, `nextIndex[]`, `matchIndex[]` отсутствуют; лога как такового нет.
- Внутренний API: `POST /raft/requestVote`, `POST /raft/appendEntries`, `GET /status`.

## 3. Модель данных

### 3.1. Raft-уровень: лог и служебные записи

- `LogEntry { long index, int term, RaftCommand command }`, индексы с 1.
- `RaftCommand` — sealed-конверт уровня Raft:
  - `UserCommand(Command payload)` — пользовательская команда для state machine;
  - `NoOpCommand` — служебная запись лидера (§7). **Не является командой state machine**
    и не доходит до неё; используется только для фиксации `commitIndex` текущего терма.
- Ядро Raft работает с `Command` как с непрозрачным payload: не знает ключей, значений,
  KV-карты и правил валидации конкретных команд.

### 3.2. Абстракция state machine

- `Command` — открытый для расширения marker-интерфейс пользовательских команд
  (не `sealed`). Конкретные команды регистрируются во внешнем реестре типов /
  Jackson subtype registry в конфигурации, а не аннотациями на ядре.
- Контракт state machine:

  ```java
  interface StateMachine<R> {
      ApplyResult<R> apply(long index, Command command);
      default Snapshot snapshot() { throw new UnsupportedOperationException(); } // future
      default void restore(Snapshot s) { throw new UnsupportedOperationException(); } // future
  }

  record ApplyResult<R>(R previousValue, R value, long index) {}
  ```

- Ядро хранит результат apply как непрозрачный `ApplyResult<?>` для ожидающего клиента
  и не интерпретирует `R`. Конкретный API-адаптер знает тип результата своей state machine
  и превращает его в JSON-ответ.
- Требования к любой concrete state machine:
  - **детерминизм**: одинаковая последовательность команд даёт одинаковый результат на всех узлах;
  - **идемпотентность по индексу**: повторный apply уже применённой записи ничего не меняет;
  - apply должен быть быстрым и без сетевых/дисковых побочных эффектов под `nodeLock`
    (если concrete SM медленная, вынос apply за лок с сохранением порядка — отдельное решение).
- `snapshot()/restore(...)` закладываются в интерфейс на будущее, но в этапах 2–4 не реализуются.

### 3.3. Первая конкретизация: KV

- `KvStateMachine implements StateMachine<JsonNode>` с внутренним `Map<String, JsonNode> kv`.
- `PutCommand(key: String, value: JsonNode)` — пользовательская запись; `value` — любые
  JSON-данные (string/number/array/object/null), как в `HashtablePostInput`.
- Применение `PutCommand` кладёт `value` по ключу и возвращает `previousValue`/`value`.
- Другие команды (например, `Delete`, `CAS`) могут добавляться позже через реестр типов
  и обработчик в concrete SM, без изменения ядра Raft.

### 3.4. Volatile-состояние

- Volatile: `commitIndex`, `lastApplied`; на лидере `nextIndex[peer]`, `matchIndex[peer]`.
  При переходе в `LEADER` (в `tryBecomeLeaderLocked`) `nextIndex[peer] = lastLogIndex + 1`,
  `matchIndex[peer] = 0`.

## 4. Ход записи (лидер)

1. Принять команду от клиента на лидере; если узел не лидер — redirect (см. §8).
2. Под `nodeLock`: проверить актуальность лидерства/терма, добавить entry в локальный лог
   с term = `currentTerm` и `UserCommand(payload)`, обновить `nextIndex` для себя.
3. Вне `nodeLock`: разослать `AppendEntries` пирам с `prevLogIndex`/`prevLogTerm`,
   `entries`, `leaderCommit`.
4. Под `nodeLock`: обработать ответы; при `success` обновить `nextIndex`/`matchIndex`.
5. Как только для некоторого N > `commitIndex` большинство `matchIndex[i] >= N` **и**
   `log[N].term == currentTerm` — установить `commitIndex = N`.
6. Применить записи от `lastApplied + 1` до `commitIndex` в порядке индекса:
   для `UserCommand` вызвать `stateMachine.apply(index, payload)`, для `NoOpCommand`
   продвинуть `lastApplied` без вызова state machine. Обновить `lastApplied`.
7. Ответить клиенту результатом apply (`ApplyResult`) через API-адаптер и индексом записи.

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
  6. Применить `log[lastApplied+1 .. commitIndex]`: `UserCommand` → `stateMachine.apply(...)`,
     `NoOpCommand` → продвинуть `lastApplied` без вызова state machine.
- Расширить `AppendEntriesResponse`: `term`, `success`, `lastIndex`, опционально
  `conflictTerm`/`conflictIndex` для быстрого отката `nextIndex`.
- При отказе из-за рассинхрона лидер уменьшает `nextIndex` и повторяет (с оптимизацией
  по `conflictTerm/conflictIndex` — опционально, можно начать с простого decrement).

## 7. NOOP после избрания

- Сразу после перехода в `LEADER` лидер добавляет `NoOpCommand` своего терма и реплицирует
  его. Это нужно, чтобы зафиксировать `commitIndex` своего терма и, как следствие, все
  записи предыдущих термов (§5.4.2), а также безопасно обслуживать linearizable reads (§8 статьи).
- `NoOpCommand` — **Raft-уровневый маркер**, а не команда state machine: он не доходит до
  `StateMachine.apply`, не меняет состояние и не попадает в клиентский список лога/ответ.
- При apply `lastApplied` продвигается и через `NoOpCommand`, но без вызова state machine.

## 8. Работа с клиентом

- Запись принимает только лидер; фолловер возвращает отказ с указанием лидера
  (например, `503` + `leaderId`). **Forward на лидера пока не делаем**; возможность
  добавить его позже сохраняем здесь как отметку (не реализовано).
- Ответ вставки: **оба значения из state machine** — `previousValue` (до применения)
  и `value` (после применения), плюс `index` записи в логе. Это контракт API-адаптера
  поверх `ApplyResult`; ядро Raft не интерпретирует эти поля. `NoOpCommand` в ответ не попадает.
- `GET /log`: список пользовательских команд (`UserCommand`) в порядке индекса, без `NoOpCommand`.
- `GET /query?key` и `GET /log` из клиентского OpenAPI пока не реализовываются;
  оставляем внутренние endpoint-ы: `POST /raft/log`, `GET /raft/log`, опционально
  `GET /raft/kv`.

## 9. Хранение

- `PersistedState` — unified состояние: `term`, `votedFor`, `log[]`.
  **Лог сохраняется так же, как терм и голос** — в том же персистентном состоянии.
- Записи лога содержат `RaftCommand`-конверт; сериализация пользовательских команд идёт
  через реестр типов / Jackson subtype registry. Новый envelope-формат вводится **без
  обратной совместимости** со старым JSON этапа 1 (persisted-логов этапа 1 нет).
- Вместо legacy-reader — явный `formatVersion` в `PersistedState` и **fail-fast** при
  загрузке: отсутствующая/старая/неизвестная версия → понятная ошибка
  `unsupported raft state format`, а не тихая попытка прочитать старый формат.
- `commitIndex`/`lastApplied` остаются volatile, по Figure 2 статьи их хранить не обязательно.
- Сохранить атомарную запись `FileRaftStorage` (tmp + `force(true)` + atomic move).
- Порядок фиксации: лог/терм/голос пишутся до видимого изменения состояния и до ответа
  на RPC (persist-before-visible).

## 10. Конкурентность и логирование

- Все мутации (лог, `commitIndex`, `lastApplied`, state machine, `nextIndex`/`matchIndex`) —
  под тем же `nodeLock`; порядок локов `nodeLock` → `RaftState` → (log/state machine).
  Concrete state machine должна быть быстрой и детерминированной; медленный apply под
  `nodeLock` будет тормозить Raft-переходы.
- Сеть и ожидание ответов клиента (до коммита) — вне `nodeLock`; после сети контекст
  перепроверяется под локом.
- Сохранить текущие события и формат (MDC `nodeId/term/role`, решение и side-effects
  из `before/after`-снапшотов/outcome, без повторного чтения живого состояния).
- **Логирование новых операций** (уровень INFO для ключевых решений, DEBUG для деталей по пирам):
  - запись принята лидером: `client append accepted` — key, index, term, размер value;
  - запись добавлена в лог: `log entry appended` — index, term, тип `RaftCommand`
    (`USER`/`NOOP`, для `USER` — тип payload);
  - NOOP после избрания: `no-op entry appended` — index, term;
  - отправка репликации: `AppendEntries sent` (уже есть) дополнить `prevLogIndex`,
    `prevLogTerm`, `entries`, `leaderCommit`; детали по каждому пиру — DEBUG;
  - ответ фолловера: `log replicated to` — peer, matchIndex, success/failure;
  - рассинхрон: `log conflict truncated` — peer/индекс/терм, `nextIndex decremented`;
  - коммит: `commitIndex advanced` — old → new, основание (большинство `matchIndex` + term);
  - применение: `entry applied` — index, key (для `PUT`), `lastApplied`;
    `no-op applied` — DEBUG, index, `lastApplied` (без вызова state machine);
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
- Новые unit для абстракции: fake state machine для проверяемости замены; envelope
  `RaftCommand` и реестр типов (round-trip); `formatVersion` fail-fast (отсутствует/старая/
  неизвестная версия); `NoOpCommand` не доходит до state machine; `ApplyResult` непрозрачен
  для ядра.
- Интеграция: 3 узла, записать значение, убедиться что все KV и `/log` совпадают;
  выключить лидера, проверить перевыборы и сохранность записей; записать на фолловере —
  redirect.

## 12. Принятые решения (бывшие открытые вопросы)

1. **API:** пока оставляем внутренние `POST /raft/log` + `GET /raft/log`
   (опционально `GET /raft/kv`). Целевые `POST /query`, `GET /query`, `GET /log`
   из `docs/client_openapi.json` не реализовываем.
2. **Ответ вставки:** оба значения state machine — `previousValue` и `value`,
   плюс `index` записи. Это контракт API-адаптера поверх `ApplyResult`, а не ядра Raft.
3. **Хранение:** лог входит в персистентное состояние наравне с `term`/`votedFor`
   (unified `PersistedState`, атомарная запись через `FileRaftStorage`).
4. **Фолловер:** отвечает отказом с `leaderId`, без форварда. Возможность добавить
   forward на лидера помечена в §8 и может быть реализована позже.
5. **Абстракция state machine:** ядро работает с `RaftCommand`/`Command` и
   `StateMachine.apply(index, command) -> ApplyResult`, не используя KV-семантику.
   KV — первая конкретизация (`KvStateMachine`), заменяемая без изменения ядра.
   `NoOpCommand` — Raft-уровневый маркер, не команда state machine. Пользовательские
   команды открыты для расширения через реестр типов; результат apply непрозрачен для ядра.

## 13. Этапы реализации и чек-листы

Разделы 1–12 задают целевую схему; ниже — **порядок внедрения**.
Отметка `[x]` означает, что пункт реализован и проверен. Чек-листы вместе покрывают
весь план (соответствие разделов и этапов — в конце этого раздела).

### Этап 1. Модель лога, персистентность и условия согласованности

Примечание: уточнение абстракции state machine вводится на этапе 2 и включает небольшой
рефакторинг модели этапа 1 (`RaftCommand`-конверт, открытый `Command`, реестр типов).
Пункты этапа 1 остаются выполненными в текущем виде.

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

### Этап 2. Абстракция state machine и KV-конкретизация

- [x] Ввести `StateMachine<R>` с `apply(long index, Command command): ApplyResult<R>`;
      заложить `snapshot()/restore(...)` как future-default, не реализовывать.
- [x] Ввести Raft-уровневый `RaftCommand = UserCommand(Command payload) | NoOpCommand`;
      `LogEntry` хранит `RaftCommand`; state machine получает только `Command` из `UserCommand`.
- [x] `Command` сделать открытым для расширения (не `sealed`) + реестр типов / Jackson
      subtype registry в конфигурации; конкретные команды не аннотируют ядро. Неизвестный
      тип команды при десериализации → ошибка, а не тихое игнорирование.
- [x] Перевести `LogEntry`/`PersistedState`/DTO `AppendEntriesRequest.entries` и их тесты
      на `RaftCommand`-конверт (миграция wire/persisted формата вместе с моделью).
- [x] Переименовать/перенести старый `NoOpCommand implements Command`, чтобы имя
      `NoOpCommand` означало только Raft-уровневый маркер (без коллизии смыслов).
- [x] Ввести `formatVersion` в `PersistedState` и fail-fast при загрузке отсутствующей/
      старой/неизвестной версии (`unsupported raft state format`). Legacy-reader не нужен:
      persisted-логов этапа 1 нет, новый формат вводится без обратной совместимости.
- [x] KV-конкретизация: `KvStateMachine implements StateMachine<JsonNode>` с
      `Map<String, JsonNode> kv`; `PutCommand(key, JsonNode value)` возвращает
      `previousValue`/`value`.
- [x] `NoOpCommand` не доходит до state machine; `lastApplied` продвигается и для `NoOpCommand`,
      и для `UserCommand` строго по порядку.
- [x] Ввести volatile `commitIndex`/`lastApplied` и apply-цикл `(lastApplied, commitIndex]`;
      логика продвижения `commitIndex` по большинству — этап 3.
- [x] Повторный apply уже применённой записи ничего не меняет (идемпотентность).
- [x] Результат apply хранится как непрозрачный `ApplyResult<?>` для ожидающего клиента;
      API-адаптер конкретизирует его в JSON (`previousValue`, `value`, `index`).
- [x] Требования к concrete SM зафиксированы: детерминизм, идемпотентность по индексу,
      быстрый apply без сетевых/дисковых побочных эффектов под `nodeLock`.
- [x] Тесты: fake SM для проверяемости замены; KV concrete; `NoOpCommand` skip;
      последовательность apply; повторный apply; все типы JSON-значений
      (string/number/array/object/null); envelope/registry round-trip; `formatVersion`
      fail-fast; DTO `AppendEntries` с конвертом.
- [x] Логирование этапа: `entry applied` (index, `lastApplied`); `no-op applied` — DEBUG;
      проигнорированный повторный apply — DEBUG. KV-специфичный key логируется только в
      concrete SM/API, не в ядре.
- [x] Проверка этапа: `mvn test` зелёный + smoke-старт 3 узлов.
- [x] Доки: `docs/CODE-STATE.md` — интерфейс `StateMachine`, `RaftCommand`-конверт,
      KV-конкретизация, `lastApplied`, `formatVersion`.

### Этап 3. Репликация и коммит

- [ ] При переходе в `LEADER`: `nextIndex[peer] = lastLogIndex + 1`,
      `matchIndex[peer] = 0` (в `tryBecomeLeaderLocked`).
- [ ] Сразу после избрания лидер добавляет `NoOpCommand` (Raft-уровневый маркер, не команда
      state machine) своего терма и реплицирует его.
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
- [ ] Изменение `commitIndex` запускает apply на всех узлах: `UserCommand` →
      `StateMachine.apply`, `NoOpCommand` → продвижение `lastApplied` без вызова SM.
- [ ] Все мутации под `nodeLock`, сеть вне лока; после сети контекст перепроверяется
      (роль/терм), step-down при `term > currentTerm` сохранён.
- [ ] Тесты 3 узлов: расхождение логов, truncate, `NoOpCommand`-конверт, коммит текущего
      терма, перевыборы и сохранность записей.
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
- [ ] Ответ вставки содержит `previousValue`, `value` и `index` (API-адаптер поверх
      `ApplyResult`); `NoOpCommand` в ответе не участвует.
- [ ] Фолловер возвращает отказ с `leaderId` (`503`), без forward; пометка о возможности
      forward на лидера сохранена.
- [ ] `GET /raft/log` отдаёт пользовательские команды (`UserCommand`) в порядке индекса,
      без `NoOpCommand`.
- [ ] `GET /raft/kv` (опционально) читает значение по ключу.
- [ ] `POST /query`, `GET /query`, `GET /log` из `docs/client_openapi.json` **не реализованы**.
- [ ] Клиентский вызов записи ожидает коммита и apply, затем отвечает результатом
      `StateMachine.apply` через API-адаптер.
- [ ] Валидация входа (`key` непустой, `value` — любые JSON, включая `null`) и обработка ошибок.
- [ ] Тесты API: запись → чтение, запись на фолловере → redirect, `/log` без `NoOpCommand`.
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
| §3 Модель данных | 1 (лог/персистентность), 2 (абстракция SM + RaftCommand + KV-конкретизация) |
| §4 Ход записи | 2 (apply-цикл), 3 (репликация/коммит), 4 (ожидание apply и ответ) |
| §5 RequestVote | 1 |
| §6 AppendEntries | 1 (DTO), 2 (apply-цикл), 3 (правила и коммит) |
| §7 NOOP | 3 (Raft-уровневый маркер) |
| §8 Работа с клиентом | 4 |
| §9 Хранение | 1 |
| §10 Конкурентность и логирование | сквозное через 1–4 |
| §11 Тесты | 1–4 (проверка после каждого этапа, финальная в 4) |
| §12 Принятые решения | 1 (персистентность лога), 2 (`previousValue`/`value` как контракт API), 4 (API, redirect), 2 (абстракция SM) |
