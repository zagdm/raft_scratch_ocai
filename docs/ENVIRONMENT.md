# Окружение и конфигурация машины (my-raft)

Снимок на **2026-10-27**. Файл фиксирует, на чём собирался и проверялся проект: версии
инструментов, пути, кодировки и особенности взаимодействия WSL ↔ Windows. При смене машины,
JDK или Maven документ нужно обновить.

## 1. Хост

| Параметр | Значение |
|---|---|
| Имя машины | `DESKTOP-57A7VVA` |
| ОС | Windows 10 Pro, версия 10.0.19045 (build 19045.6456) |
| Процессор | AMD Ryzen 3 3250U with Radeon Graphics, 2 ядра / 4 потока |
| ОЗУ | ~9.9 ГБ |
| Оболочка для работы с проектом | WSL2: Ubuntu 22.04.5 LTS, ядро `6.18.54.1-microsoft-standard-WSL2`, x86_64 |

## 2. Java

| Параметр | Значение |
|---|---|
| JDK | `C:\Program Files\Java\jdk-21` |
| Версия | 21.0.5+9-LTS-239 (Oracle JDK, HotSpot 64-Bit Server VM) |
| `java.home` | `C:\Program Files\Java\jdk-21` |
| `JAVA_HOME` | **не задан** |
| `where java` | `C:\Program Files\Common Files\Oracle\Java\javapath\java.exe` |

Важно: `java` в PATH — это launcher-shim Oracle `javapath`. При запуске через
`start "title" java ...` он порождает **второй** `java.exe`, из-за чего в окне остаётся лишний
процесс. Поэтому `start-node.cmd` разрешает реальный JDK: `JAVA_HOME` → `java.home`
(через `java -XshowSettings:properties`) → `java`.

Java в WSL **не установлена** — все прогоны идут через Windows-JDK.

## 3. Maven

| Параметр | Значение |
|---|---|
| Maven в PATH | отсутствует (`where mvn` ничего не находит) |
| Используемый Maven | встроенный в IntelliJ IDEA: `C:\Program Files\JetBrains\IntelliJ IDEA Community Edition 2025.2.4\plugins\maven\lib\maven3\bin\mvn.cmd` |
| Версия Maven | 3.9.9 (`maven-core-3.9.9.jar`, `maven-embedder-3.9.9.jar`) |
| Локальный репозиторий | `C:\Users\<пользователь>\.m2\repository` (~239 МБ, зависимости уже скачаны) |
| IntelliJ IDEA | Community Edition 2025.2.4 |

Сборка/тесты выполняются с Windows-стороны, например:

```
cmd.exe /c "cd /d C:\temp\myraft-build && \"C:\Program Files\JetBrains\IntelliJ IDEA Community Edition 2025.2.4\plugins\maven\lib\maven3\bin\mvn.cmd\" test"
```

Проект при этом копируется в ASCII-путь `C:\temp\myraft-build` (см. п. 4), потому что Maven
из WSL по UNC-пути и по пути с кириллицей работает ненадёжно.

## 4. Путь проекта и его особенности

| | |
|---|---|
| Рабочий каталог | `/mnt/c/Users/Загнеев Дмитрий/IdeaProjects/my-raft` |
| Windows-путь | `C:\Users\Загнеев Дмитрий\IdeaProjects\my-raft` |
| Короткий путь (8.3) | `C:\Users\BCC9~1\IdeaProjects\my-raft` |

Путь содержит **кириллицу и пробел** — при передаче из WSL в `cmd.exe` такой аргумент
искажается, поэтому в командах используется короткий путь `C:\Users\BCC9~1\...`.
Сам каталог проекта (`my-raft`) — ASCII, короткое имя не требуется.

Git-репозитория нет: `git rev-parse` сообщает «not a git repository».

## 5. Каталоги, создаваемые при работе

| Каталог | Назначение |
|---|---|
| `target/` | сборка; `my-raft-0.0.1-SNAPSHOT.jar` (~20 МБ) + `.original` |
| `data/` | состояние узлов `node-1.json … node-N.json` (создаётся скриптами; в репозитории отсутствует) |

## 6. Кодировки и файлы скриптов

- Кодовая страница `cmd.exe` на машине — **866** (проверено `chcp`). Вывод Windows-команд в
  WSL поэтому выглядит «кракозябрами»; для чтения использовать
  `iconv -f cp866 -t utf-8`.
- Файлы `scripts/*.cmd` обязаны быть **ASCII + CRLF** (проверка: `file scripts/*.cmd`).
  LF-переводы и не-ASCII символы ломают разбор batch-файла.
- Кириллица допустима в `.md`, `.java` (UTF-8), но не в `.cmd`.

## 7. Особенности WSL ↔ Windows (грабли и обходы)

1. **UNC-каталог**: если `cmd.exe` запускается из `/tmp` или другого каталога WSL, он стартует
   в `\\wsl.localhost\...` и предупреждает, что UNC-пути не поддерживаются. Решение: запускать
   `cmd.exe /c "cd /d C:\temp && ..."`, то есть с Windows-каталога.
2. **Кириллица в аргументах** искажается (WSL передаёт UTF-8, cmd ожидает ANSI/866). Решение:
   короткий путь `C:\Users\BCC9~1\...` или ASCII-копия `C:\temp\myraft-build`.
3. **`cmd.exe` не возвращает управление**, пока живы дочерние окна `start` (наследуется
   консольный handle). Поэтому запуск кластера из WSL идёт в фоне с перенаправлением вывода
   в файл; признак завершения — появление сводки в логе, а не выход процесса.
4. **Переменные окружения не пробрасываются** из bash в cmd надёжно (`DATA_DIR=... cmd.exe ...`
   не сработал). `DATA_DIR` задаётся внутри `.cmd` (по умолчанию `data`).
5. **Проверка HTTP** — со стороны Windows, чтобы не зависеть от проброса localhost из WSL:

   ```
   powershell.exe -NoProfile -Command "Invoke-RestMethod http://localhost:18080/status"
   ```

6. **Поиск/закрытие окна ноды** — через WMI и `CloseMainWindow`:

   ```
   powershell.exe -NoProfile -Command "$p = Get-CimInstance Win32_Process -Filter \"Name='java.exe'\" | Where-Object { $_.CommandLine -like '*server.port=18081*' }; Get-Process -Id $p.ProcessId | ForEach-Object { $_.CloseMainWindow() }"
   ```

7. `tasklist /FI "IMAGENAME eq java.exe"` при вызове из WSL требует аккуратного
   экранирования кавычек; проще `Get-Process java` или `tasklist` без фильтра + `grep`.

## 8. Установленные инструменты

| Инструмент | Где | Примечание |
|---|---|---|
| `java` | Windows PATH (shim) / `C:\Program Files\Java\jdk-21` | 21.0.5 |
| `mvn` | только внутри IntelliJ (3.9.9) | в PATH нет |
| `powershell` | Windows | используется для HTTP-проверок и закрытия окон |
| `curl` | WSL `/usr/bin/curl` | есть, но проверки идут через PowerShell |
| `git` | WSL `/usr/bin/git` | проект не под git |
| `iconv` | WSL | чтение вывода cp866 |
| JDK/Maven в WSL | отсутствуют | сборка только на Windows |

## 8.1. Почему нет bash-скриптов

В WSL нет ни Java, ни Maven, и пробросить их туда в текущей конфигурации нельзя. Поэтому
`start-cluster.sh` / `stop-cluster.sh` не могли бы запустить проект и были **удалены**
(вместе с каталогами `scripts/.pids` и `scripts/logs`, которые они использовали).
WSL применяется только как оболочка для работы с файлами и запуска Windows-команд
(`cmd.exe`, `powershell.exe`, `iconv`), а запуск и остановка кластера — исключительно через
`.cmd`-скрипты на стороне Windows.

## 8.2. Правила запуска команд Maven/тестов из WSL

Из-за связки WSL ↔ Windows при запуске `mvn` возникали ошибки:

| Ошибка | Причина | Решение |
|---|---|---|
| `UNC paths are not supported` | `cmd.exe` стартует из `\\wsl.localhost\...` (при запуске из `/tmp` или WSL-UNC) | Работать из Windows-каталога через `cd /d` или `Set-Location` в PowerShell. |
| Искажение кириллицы/пробелов | Путь с кириллицей `C:\Users\Загнеев Дмитрий\...` ломается при передаче аргументов из WSL (UTF-8 -> cp866) | Копировать проект в ASCII-путь: `C:\temp\myraft-build` (`robocopy` с `/XD target .git .idea`). Использовать короткий путь `C:\Users\BCC9~1\...` либо полностью ASCII-каталог. |
| `... не является внутренней или внешней командой` / `POM file ... does not exist` | Неверные вложенные кавычки при `cmd.exe /c "..."` из bash/WSL | **Не** использовать многоуровневый `cmd.exe /c`. Запускать через `powershell.exe -NoProfile -Command ...` с оператором `&`. |
| `mvn` не найден | `mvn` отсутствует в PATH Windows, используется бандл IntelliJ | Использовать полный путь: `"C:\Program Files\JetBrains\IntelliJ IDEA Community Edition 2025.2.4\plugins\maven\lib\maven3\bin\mvn.cmd"`. |
| Ошибки PowerShell-синтаксиса (`=` не распознан) | Неправильное присваивание/экранирование при передаче через строку | Использовать `& 'путь'`, `Set-Location 'путь'`, одинарные кавычки. Не передавать сложные `cmd`-строки через несколько слоёв экранирования. |

**Стандартный приём для тестов/сборки (проверенный):**

```bash
powershell.exe -NoProfile -Command "Set-Location 'C:\temp\myraft-build'; & 'C:\Program Files\JetBrains\IntelliJ IDEA Community Edition 2025.2.4\plugins\maven\lib\maven3\bin\mvn.cmd' test"
```

Суть: минимальное количество слоёв (`WSL -> powershell -> mvn.cmd`), ASCII-путь, корректные кавычки. Вывод `cmd/powershell` может требоваться в `iconv -f cp866 -t utf-8`, но сам запуск должен идти именно так.

## 9. Порты

| Назначение | Порты |
|---|---|
| По умолчанию (`start-cluster.cmd` без аргументов) | 8080, 8081, 8082 |
| Тестовый прогон | 18080, 18081, 18082 |

Перед запуском порты должны быть свободны; `start-node.cmd` проверяет только состояние
`LISTENING` (TIME_WAIT не считается занятостью). На момент снимка все перечисленные порты
свободны, java-процессов нет.

## 10. Воспроизведение сборки и проверки

```
# 1. Копия в ASCII-путь (если сборка запускается с Windows-стороны)
robocopy "C:\Users\BCC9~1\IdeaProjects\my-raft" C:\temp\myraft-build /MIR /XD target

# 2. Тесты и сборка
"C:\Program Files\JetBrains\IntelliJ IDEA Community Edition 2025.2.4\plugins\maven\lib\maven3\bin\mvn.cmd" test
... mvn.cmd clean package -DskipTests

# 3. Запуск кластера (из любого каталога Windows)
C:\Users\BCC9~1\IdeaProjects\my-raft\scripts\start-cluster.cmd 3 18080

# 4. Возврат одной ноды после закрытия её окна
C:\Users\BCC9~1\IdeaProjects\my-raft\scripts\start-node.cmd 2 3 18080

# 5. Остановка всего
C:\Users\BCC9~1\IdeaProjects\my-raft\scripts\stop-cluster.cmd
```
