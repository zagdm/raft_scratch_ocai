@echo off
setlocal EnableExtensions EnableDelayedExpansion

rem Собирает артефакт my-raft из корня проекта
set "SCRIPT_DIR=%~dp0"
set "PROJECT_ROOT=%SCRIPT_DIR%..\\.."
for %%i in ("%PROJECT_ROOT%") do set "PROJECT_ROOT=%%~fi"
pushd "%PROJECT_ROOT%" >nul

echo [build-jar] Building in "%PROJECT_ROOT%"...

set "MVN_CMD="
if exist "mvnw.cmd" set "MVN_CMD=mvnw.cmd"
if not defined MVN_CMD (
  where mvn >nul 2>nul
  if not errorlevel 1 set "MVN_CMD=mvn"
)
if not defined MVN_CMD if defined MAVEN_HOME if exist "%MAVEN_HOME%\bin\mvn.cmd" set "MVN_CMD=%MAVEN_HOME%\bin\mvn.cmd"
if not defined MVN_CMD if exist "C:\Program Files\JetBrains\IntelliJ IDEA Community Edition 2025.2.4\plugins\maven\lib\maven3\bin\mvn.cmd" set "MVN_CMD=C:\Program Files\JetBrains\IntelliJ IDEA Community Edition 2025.2.4\plugins\maven\lib\maven3\bin\mvn.cmd"
if not defined MVN_CMD if exist "C:\Program Files\JetBrains\IntelliJ IDEA\plugins\maven\lib\maven3\bin\mvn.cmd" set "MVN_CMD=C:\Program Files\JetBrains\IntelliJ IDEA\plugins\maven\lib\maven3\bin\mvn.cmd"

if not defined MVN_CMD (
  echo [build-jar] ERROR: Maven not found. Set MAVEN_HOME, add mvn to PATH, or install IntelliJ Maven bundle.
  popd >nul
  exit /b 1
)

echo [build-jar] Using Maven: "%MVN_CMD%"
call "%MVN_CMD%" -DskipTests package
if errorlevel 1 (
  echo [build-jar] BUILD FAILED
  popd >nul
  exit /b 1
)
echo [build-jar] DONE
popd >nul
exit /b 0
