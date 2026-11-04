# Состояние проекта my-raft (обзор)

Снимок на **2026-10-28**. Описывает проект в целом: назначение, требования, скрипты запуска, скриптовые тесты, инфраструктуру, результаты проверок.

## Назначение и требования
- Минимальная реализация Raft (leader election + status API), Spring Boot 3.3.5, Java 21, HTTP/JSON
- Без Docker и gRPC
- Логи только в консоль
- Запуск на Windows: отдельное окно консоли на каждый узел, закрытие окна = остановка узла
- Скрипты только `.cmd`, параметры аргументами
- Только `start-node` из дополнительных команд; `start-cluster` — цикл по `start-node`
- README не требуется

## Организация репозитория
```
my-raft/
├── pom.xml
├── src/                # код (см. CODE-STATE.md)
├── scripts/
│   ├── runtime/        # скрипты запуска
│   │   ├── start-node.cmd
│   │   ├── start-cluster.cmd
│   │   ├── stop-cluster.cmd
│   │   └── build-jar.cmd
│   └── tests/          # папка под тестовые скрипты (пустая)
├── target/             # сборка
├── CODE-STATE.md
├── PROJECT-OVERVIEW.md
├── ENVIRONMENT.md
├── TEST-CONTEXT.md
└── test_opencode.txt   (на рабочем столе также создан по запросу)
```
Bash-версии скриптов удалены (в WSL нет Java/Maven).

## Скрипты запуска (scripts/runtime)
- `start-node.cmd <node> [total-nodes] [base-port]` (по умолчанию 3 8080): разрешение java (JAVA_HOME/java.home), поиск jar + сборка при отсутствии, проверка порта только по LISTENING, сборка peers, заметка про отсутствующий state-файл, `Running peers: X/Y`, запуск `start "raft-node-N" ... -D ... -jar ...` (-D до -jar). Принимает `2` и `node-2`. Если JAR уже есть в `target/` — не пересобирает.
- `start-cluster.cmd [total-nodes] [base-port]`: валидация, цикл `call ...\start-node.cmd`, счётчик сбоев, итоговая сводка, код 1 при ошибках.
- `stop-cluster.cmd`: PowerShell — убивает `java.exe` с `my-raft*.jar` в командной строке.
- `build-jar.cmd`: пересборка артефакта из корня проекта (`mvn -DskipTests package`), вызывается при проведении интеграционного теста.

## Окружение
См. `ENVIRONMENT.md`. Ключевые моменты: Windows 10, WSL2 Ubuntu, JDK 21 в `C:\Program Files\Java\jdk-21`, Maven из IntelliJ (не в PATH), проект по пути с кириллицей (используется короткий путь/копия в ASCII), запуск команд из WSL через `powershell.exe` с `Set-Location` и оператором `&`.

## Результаты проверок

### Модульные тесты
`mvn test` (из копии `C:\temp\myraft-build` по проверенному алгоритму): **7/7 пройдены, BUILD SUCCESS** (см. CODE-STATE.md).

### Интеграционный тест инфраструктуры запуска (2026-10-30)
Проведён по процедуре из `TEST-CONTEXT.md` на портах 18080-18082 из `C:\temp`:
1. Остановлен кластер, очищен `data/`, выполнена пересборка `scripts\runtime\build-jar.cmd`.
2. Кластер запущен через `scripts\runtime\start-cluster.cmd 3 18080` (цикл по `start-node`). Лидер выбран.
3. Узел-2 отключён закрытием окна (`CloseMainWindow()`). Кластер продолжил работу, произошёл перевыбор.
4. Узел-2 восстановлен из `C:\temp` через `scripts\runtime\start-node.cmd 2 3 18080`. Кластер собрался полностью.

**Вывод:** `start-cluster.cmd` работает как цикл по `start-node.cmd`. Отключение закрытием окна и восстановление через `start-node.cmd` из чужого cwd работают корректно. Рост `currentTerm` при перевыборах — особенность текущей реализации.

## Текущее состояние
- Java-процессов нет, тестовые порты свободны, `data/` очищен
- Кодовые правки внесены: `ElectionTimer` (epoch + синхронизация), `RestTemplate` timeout 200мс, `application.yml` (election 5000-8000, heartbeat 1000), обновлён формат логов, добавлены логи `AppendEntries`/`RequestVote` отправки/приёма
- Скрипты запуска в `scripts/runtime/` (`start-node`, `start-cluster`, `stop-cluster`, `build-jar`), `scripts/tests/` пустая
- Описание интеграционного теста с пересборкой и отключением/очисткой — в `TEST-CONTEXT.md`
