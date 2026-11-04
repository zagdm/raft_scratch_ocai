@echo off
setlocal EnableExtensions

rem Always run from the project root, no matter where this script is started from.
set "SCRIPT_DIR=%~dp0"
set "PROJECT_ROOT=%SCRIPT_DIR%..\.."
for %%i in ("%PROJECT_ROOT%") do set "PROJECT_ROOT=%%~fi"
pushd "%PROJECT_ROOT%" >nul

echo Stopping my-raft nodes...

powershell -NoProfile -ExecutionPolicy Bypass -Command "$procs = @(Get-CimInstance Win32_Process | Where-Object { $_.Name -eq 'java.exe' -and $_.CommandLine -like '*my-raft*.jar*' }); if ($procs.Count -eq 0) { Write-Host 'No running my-raft nodes found.' } else { foreach ($p in $procs) { Write-Host ('Stopping PID ' + $p.ProcessId); Stop-Process -Id $p.ProcessId -Force -ErrorAction SilentlyContinue } }"

echo Done.
popd >nul
exit /b 0
