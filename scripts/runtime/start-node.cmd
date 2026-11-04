@echo off
setlocal EnableExtensions EnableDelayedExpansion

rem Starts exactly one Raft node in its own console window.
rem This is the single primitive: start-cluster.cmd just calls it in a loop.

set "SCRIPT_DIR=%~dp0"
set "PROJECT_ROOT=%SCRIPT_DIR%..\\.."
for %%i in ("%PROJECT_ROOT%") do set "PROJECT_ROOT=%%~fi"
pushd "%PROJECT_ROOT%" >nul

rem ---- arguments ----
set "NODE=%~1"
if not defined NODE goto :usage
if /I "!NODE:~0,5!"=="node-" set "NODE=!NODE:~5!"

set "NODES=%~2"
if not defined NODES set "NODES=3"
set "BASE_PORT=%~3"
if not defined BASE_PORT set "BASE_PORT=8080"
if not defined DATA_DIR set "DATA_DIR=data"

call :require_number "%NODE%" "node number"
if errorlevel 1 goto :usage
call :require_number "%NODES%" "total nodes"
if errorlevel 1 goto :usage
call :require_number "%BASE_PORT%" "base port"
if errorlevel 1 goto :usage

set /a NODE=NODE
set /a NODES=NODES
set /a BASE_PORT=BASE_PORT

set "ERR="
if %NODES% LSS 1 set "ERR=total nodes must be at least 1"
if not defined ERR if %NODE% LSS 1 set "ERR=node must be between 1 and %NODES%"
if not defined ERR if %NODE% GTR %NODES% set "ERR=node must be between 1 and %NODES%"
if defined ERR (
  echo ERROR: !ERR!.
  goto :usage
)

rem ---- parse extra jvm args (--jvm "...") ----
set "JVM_EXTRA="
set "ARG4=%~4"
set "ARG5=%~5"
set "ARG6=%~6"
set "ARG7=%~7"
set "ARG8=%~8"
set "ARG9=%~9"
shift & shift & shift & shift & shift & shift & shift & shift & shift
:parse_args
if "%~1"=="" goto :args_done
if /I "%~1"=="--jvm" (
  shift
  if not "%~1"=="" set "JVM_EXTRA=%JVM_EXTRA% %~1"
  shift
  goto :parse_args
)
if /I "%~1"=="--jvm-args" (
  shift
  if not "%~1"=="" set "JVM_EXTRA=%JVM_EXTRA% %~1"
  shift
  goto :parse_args
)
shift
goto :parse_args
:args_done

rem ---- java ----
set "JAVA_BIN="
if defined JAVA_HOME if exist "%JAVA_HOME%\bin\java.exe" set "JAVA_BIN=%JAVA_HOME%\bin\java.exe"
if not defined JAVA_BIN (
  where java >nul 2>nul
  if errorlevel 1 (
    echo ERROR: java.exe not found. Set JAVA_HOME or add a JDK 21 to PATH.
    popd >nul
    exit /b 1
  )
  for /f "tokens=2 delims==" %%h in ('java -XshowSettings:properties -version 2^>^&1 ^| findstr /C:"java.home ="') do set "JDK_HOME=%%h"
  if defined JDK_HOME if "!JDK_HOME:~0,1!"==" " set "JDK_HOME=!JDK_HOME:~1!"
  if defined JDK_HOME if exist "!JDK_HOME!\bin\java.exe" set "JAVA_BIN=!JDK_HOME!\bin\java.exe"
)
if not defined JAVA_BIN set "JAVA_BIN=java"

rem ---- jar ----
set "JAR="
if exist "target\my-raft.jar" set "JAR=%PROJECT_ROOT%\target\my-raft.jar"
if not defined JAR (
  for /f "delims=" %%f in ('dir /b /a-d /o-d "target\my-raft*.jar" 2^>nul') do (
    if not defined JAR set "JAR=%PROJECT_ROOT%\target\%%f"
  )
)
if not defined JAR (
  echo Jar not found in "%PROJECT_ROOT%\target". Trying to build it...
  if exist "mvnw.cmd" (
    call mvnw.cmd -q -DskipTests package
  ) else (
    where mvn >nul 2>nul
    if errorlevel 1 (
      echo WARNING: Maven is not on PATH, cannot build automatically.
    ) else (
      call mvn -q -DskipTests package
    )
  )
  if exist "target\my-raft.jar" set "JAR=%PROJECT_ROOT%\target\my-raft.jar"
  if not defined JAR (
    for /f "delims=" %%f in ('dir /b /a-d /o-d "target\my-raft*.jar" 2^>nul') do (
      if not defined JAR set "JAR=%PROJECT_ROOT%\target\%%f"
    )
  )
)
if not defined JAR (
  echo ERROR: jar not found in "%PROJECT_ROOT%\target".
  echo Build it first: mvn clean package -DskipTests
  popd >nul
  exit /b 1
)

echo Using jar: %JAR%

set /a "PORT=%BASE_PORT%+%NODE%-1"
netstat -ano | findstr /R /C:":%PORT% " | findstr /C:"LISTENING" >nul
if not errorlevel 1 (
  echo ERROR: port %PORT% is already in use - node %NODE% is probably still running.
  echo Close its console window first, then run this command again.
  popd >nul
  exit /b 1
)

set "PEERS="
for /L %%j in (1,1,%NODES%) do (
  if not "%%j"=="%NODE%" (
    set /a "PEER_PORT=%BASE_PORT%+%%j-1"
    if defined PEERS ( set "PEERS=!PEERS!,http://localhost:!PEER_PORT!" ) else ( set "PEERS=http://localhost:!PEER_PORT!" )
  )
)

set /a RUNNING=0
for /L %%j in (1,1,%NODES%) do (
  if not "%%j"=="%NODE%" (
    set /a "P=%BASE_PORT%+%%j-1"
    netstat -ano | findstr /R /C:":!P! " | findstr /C:"LISTENING" >nul
    if not errorlevel 1 set /a RUNNING+=1
  )
)
set /a PEER_TOTAL=%NODES%-1

set "STATE=%DATA_DIR%\node-%NODE%.json"
if not exist "%STATE%" echo note: %STATE% not found, node-%NODE% will start with term 0

if defined PEERS (
  echo Starting raft-node-%NODE% on port %PORT% peers: %PEERS%
) else (
  echo Starting raft-node-%NODE% on port %PORT% without peers
)
echo Running peers: %RUNNING%/%PEER_TOTAL%

if not exist "%DATA_DIR%" mkdir "%DATA_DIR%"
if defined PEERS (
  start "raft-node-%NODE%" "%JAVA_BIN%" -Dserver.port=%PORT% -Draft.node-id=node-%NODE% -Draft.storage-path=%STATE% -Dcluster.peers=%PEERS% %JVM_EXTRA% -jar "%JAR%"
) else (
  start "raft-node-%NODE%" "%JAVA_BIN%" -Dserver.port=%PORT% -Draft.node-id=node-%NODE% -Draft.storage-path=%STATE% %JVM_EXTRA% -jar "%JAR%"
)

echo Status: curl http://localhost:%PORT%/status
echo Close the "raft-node-%NODE%" window to stop this node.
popd >nul
exit /b 0

:require_number
setlocal EnableDelayedExpansion
set "BAD="
for /f "delims=0123456789" %%c in ("%~1") do set "BAD=1"
if defined BAD (
  endlocal & echo ERROR: %~2 must be a number, got "%~1".
  exit /b 1
)
endlocal
exit /b 0

:usage
echo.
echo Usage: start-node.cmd ^<node^> [total-nodes] [base-port] [--jvm "..."] [--jvm-args "..."]
echo.
echo   node         node number, e.g. 2 or node-2 (required)
echo   total-nodes  cluster size used to build the peers list (default 3)
echo   base-port    port of node 1 (default 8080)
echo   --jvm        extra JVM args passed directly to java before -jar
echo.
echo Examples:
echo   start-node.cmd 2
echo   start-node.cmd 2 3 18080
echo   start-node.cmd 2 3 18080 --jvm "-Draft.storage.type=memory"
echo.
popd >nul
if "%~1"=="" pause
exit /b 1
