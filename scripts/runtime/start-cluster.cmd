@echo off
setlocal EnableExtensions EnableDelayedExpansion

rem Starts the whole cluster by calling start-node.cmd once per node,
rem so a manual "start-node.cmd N" is exactly what this script does per node.
rem Extra JVM args can be passed via --jvm "..." and are forwarded to each start-node.

set "SCRIPT_DIR=%~dp0"
set "PROJECT_ROOT=%SCRIPT_DIR%..\\.."
for %%i in ("%PROJECT_ROOT%") do set "PROJECT_ROOT=%%~fi"
pushd "%PROJECT_ROOT%" >nul

rem ---- arguments ----
set "NODES=%~1"
if not defined NODES set "NODES=3"
set "BASE_PORT=%~2"
if not defined BASE_PORT set "BASE_PORT=8080"
if not defined DATA_DIR set "DATA_DIR=data"

call :require_number "%NODES%" "total nodes"
if errorlevel 1 goto :usage
call :require_number "%BASE_PORT%" "base port"
if errorlevel 1 goto :usage

set /a NODES=NODES
set /a BASE_PORT=BASE_PORT

if %NODES% LSS 1 (
  echo ERROR: total nodes must be at least 1.
  goto :usage
)

rem ---- forward extra jvm args after base-port ----
set "EXTRA_ARGS="
set "idx=0"
:collect_args
set /a idx+=1
if %idx% EQU 1 shift & goto :collect_args
if %idx% EQU 2 shift & goto :collect_args
if %idx% EQU 3 goto :collect_done
shift
goto :collect_args
:collect_done
:forward_loop
if "%~1"=="" goto :forward_done
if "%EXTRA_ARGS%"=="" (
  set "EXTRA_ARGS=%~1"
) else (
  set "EXTRA_ARGS=%EXTRA_ARGS% %~1"
)
shift
goto :forward_loop
:forward_done

rem ---- one start-node.cmd call per node ----
set /a FAILED=0
for /L %%i in (1,1,%NODES%) do (
  echo.
  echo === raft-node-%%i of %NODES% ===
  if "%EXTRA_ARGS%"=="" (
    call "%SCRIPT_DIR%start-node.cmd" %%i %NODES% %BASE_PORT%
  ) else (
    call "%SCRIPT_DIR%start-node.cmd" %%i %NODES% %BASE_PORT% %EXTRA_ARGS%
  )
  if errorlevel 1 (
    echo raft-node-%%i failed to start.
    set /a FAILED+=1
  )
)

echo.
if %FAILED%==0 (
  echo Started %NODES% nodes - one console window per node.
) else (
  echo WARNING: !FAILED! of %NODES% nodes failed to start.
)
echo Close a node window to stop that node.
echo Bring one node back: scripts\runtime\start-node.cmd ^<node^> %NODES% %BASE_PORT% [--jvm "..."]
echo Stop all nodes: scripts\runtime\stop-cluster.cmd
popd >nul
if %FAILED% NEQ 0 exit /b 1
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
echo Usage: start-cluster.cmd [total-nodes] [base-port] [--jvm "..."] [--jvm-args "..."]
echo.
echo   total-nodes  number of nodes to start (default 3)
echo   base-port    port of node 1, node N uses base-port + N - 1 (default 8080)
echo   --jvm        extra JVM args passed directly to java before -jar (forwarded to each node)
echo.
echo Examples:
echo   start-cluster.cmd
echo   start-cluster.cmd 5 9000
echo   start-cluster.cmd 3 18080 --jvm "-Draft.storage.type=memory"
echo.
popd >nul
exit /b 1
