# my-raft

Минимальная реализация протокола Raft на **Java 21** + **Spring Boot 3.3.5**: выбор лидера, heartbeats, репликация лога с коммитом по большинству, применение команд к KV и внутренний REST API записи/чтения.

## Возможности

- **Leader Election** — автоматический выбор лидера, NoOp нового терма после избрания
- **Репликация лога** — `AppendEntries` с проверкой `prevLog*`, откатом `nextIndex` при конфликте и коммитом по большинству
- **State machine** — применение закоммиченных команд к KV (`PUT`), результат доступен клиенту
- **REST API** — `GET /status`, `POST /raft/log`, `GET /raft/log`, `GET /raft/kv`
- **Персистентность** — `currentTerm`, `votedFor` и лог в файле (`formatVersion=2`) или in-memory
- **Windows-скрипты** — удобный запуск кластера из консоли Windows

## Требования

- Java 21
- Maven 3.9+

## Сборка

```bash
mvn clean package -DskipTests
```

## Запуск

### Кластер из 3 узлов (порты 8080, 8081, 8082)
```cmd
scripts\runtime\start-cluster.cmd 3 8080
```

### Запуск отдельного узла
```cmd
scripts\runtime\start-node.cmd 2 3 8080
```

### Остановка кластера
```cmd
scripts\runtime\stop-cluster.cmd
```

### Сборка артефакта
```cmd
scripts\runtime\build-jar.cmd
```

## Хранилище

По умолчанию используется **in-memory** режим: `currentTerm`, `votedFor` и лог живут только в памяти процесса, между перезапусками состояние не сохраняется.

Файловый режим (персистентность между перезапусками):

```cmd
scripts\runtime\start-cluster.cmd 3 18080 --jvm "-Draft.storage.type=file"
```

Путь задаётся через `-Draft.storage-path=...` (по умолчанию `data/raft-state.json`; скрипты используют `data/node-N.json`).

## Статус узла

```http
GET http://localhost:8080/status
```

Пример ответа:

```json
{
  "nodeId": "node-1",
  "role": "LEADER",
  "currentTerm": 1,
  "leaderId": "node-1",
  "peers": ["http://localhost:8081", "http://localhost:8082"],
  "uptime": "123s",
  "isLeader": true
}
```

## Клиентский API

Запись принимает только лидер и отвечает после коммита и применения команды:

```http
POST /raft/log
Content-Type: application/json

{"key": "k", "value": {"any": "json"}}
```

Успешный ответ: `{"previousValue": ..., "value": ..., "index": 2}`. Фолловер отвечает
`503 {"error": "...", "leaderId": "node-1"}` (без автоматического forward), пустой `key` — `400`,
таймаут ожидания apply — `504`. Чтение: `GET /raft/log` (только пользовательские записи, без NoOp)
и `GET /raft/kv?key=k` → `{"key": "k", "value": ..., "found": true}`. `found=false` означает
отсутствие ключа; ключ со значением JSON `null` существует (`found=true`).

## Тесты

```bash
mvn test    # 68 тестов, BUILD SUCCESS
```

Подробности о покрытии — в `docs/TEST-CONTEXT.md`.

## Особенности

- Каждый узел запускается в отдельном окне консоли Windows
- Закрытие окна консоли = остановка узла
- Логи только в консоль
- `start-cluster.cmd` вызывает `start-node.cmd` в цикле
- Через флаг `--jvm` можно передавать любые JVM-параметры в запуск
- Таймауты (`application.yml`): heartbeat — 100 мс, election — 500–700 мс, ожидание apply клиентом — 5000 мс
