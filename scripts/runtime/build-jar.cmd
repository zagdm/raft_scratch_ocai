@echo off
setlocal EnableExtensions EnableDelayedExpansion

rem Собирает артефакт my-raft из корня проекта
set "SCRIPT_DIR=%~dp0"
set "PROJECT_ROOT=%SCRIPT_DIR%..\\.."
for %%i in ("%PROJECT_ROOT%") do set "PROJECT_ROOT=%%~fi"
pushd "%PROJECT_ROOT%" >nul

echo [build-jar] Building in "%PROJECT_ROOT%"...
if exist "mvnw.cmd" (
  call mvnw.cmd -DskipTests package
) else (
  where mvn >nul 2>nul
  if errorlevel 1 (
    echo [build-jar] ERROR: mvn not found in PATH and mvnw.cmd missing.
    popd >nul
    exit /b 1
  )
  call mvn -DskipTests package
)
if errorlevel 1 (
  echo [build-jar] BUILD FAILED
  popd >nul
  exit /b 1
)
echo [build-jar] DONE
popd >nul
exit /b 0
