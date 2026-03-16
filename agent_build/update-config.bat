@echo off
setlocal

set APP_DIR=%~dp0
set AGENT_EXE=%APP_DIR%torqmind-agent.exe
set CONFIG_FILE=%APP_DIR%config.enc

if not exist "%AGENT_EXE%" (
  echo torqmind-agent.exe nao encontrado em "%APP_DIR%".
  exit /b 1
)

"%AGENT_EXE%" config edit --interactive --config "%CONFIG_FILE%"
if errorlevel 1 exit /b %errorlevel%

sc stop TorqMindAgent >nul 2>&1
sc start TorqMindAgent >nul 2>&1

echo Configuracao atualizada com sucesso.
exit /b 0
