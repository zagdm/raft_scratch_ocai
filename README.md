# my-raft

Минимальная реализация протокола Raft на **Java 21** + **Spring Boot 3.3.5**. Реализованы выбор лидера, heartbeats и REST API для получения статуса узла.

## Возможности

- **Leader Election** — автоматический выбор лидера в кластере
- **Heartbeats (AppendEntries)** — поддержание лидерства
- **REST API** — `GET /status` для просмотра состояния узла
- **Персистентность** — хранение `currentTerm` и `votedFor` в файл или in-memory режиме
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

## In-memory режим (без файлов)

```cmd
scripts\runtime\start-cluster.cmd 3 18080 --jvm "-Draft.storage.type=memory"
```

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

## Тесты

```bash
mvn test
```

## Особенности

- Каждый узел запускается в отдельном окне консоли Windows
- Закрытие окна консоли = остановка узла
- Логи только в консоль
- `start-cluster.cmd` вызывает `start-node.cmd` в цикле
- Через флаг `--jvm` можно передавать любые JVM-параметры в запуск
- Таймауты: heartbeat — 1с, election — 5-8с
