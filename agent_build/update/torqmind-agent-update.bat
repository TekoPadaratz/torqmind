@echo off
REM TorqMind Agent 2.0 — safe WinSW swap (stop → backup → promote → start → rollback)
REM Generated/used by agent.update.apply; kept in repo for packaging.
setlocal EnableExtensions
if "%BASE%"=="" set BASE=%~dp0..
if "%SVC%"=="" set SVC=TorqMindAgent
set EXE=%BASE%\torqmind-agent.exe
set NEW=%BASE%\updates\torqmind-agent.exe.new
set BAK=%BASE%\backup\torqmind-agent.exe
set LOG=%BASE%\updates\last_result.json

echo {"phase":"start"} > "%LOG%"
sc stop %SVC% >nul 2>&1
timeout /t 5 /nobreak >nul

if not exist "%NEW%" (
  echo {"phase":"abort","error":"missing_new_exe"} > "%LOG%"
  sc start %SVC% >nul 2>&1
  exit /b 2
)

if not exist "%BASE%\backup" mkdir "%BASE%\backup"
if exist "%EXE%" (
  if exist "%BAK%" del /f /q "%BAK%"
  move /y "%EXE%" "%BAK%" >nul
)

move /y "%NEW%" "%EXE%" >nul
if errorlevel 1 (
  echo {"phase":"swap_failed"} > "%LOG%"
  if exist "%BAK%" move /y "%BAK%" "%EXE%" >nul
  sc start %SVC% >nul 2>&1
  exit /b 3
)

sc start %SVC% >nul 2>&1
timeout /t 8 /nobreak >nul
sc query %SVC% | find "RUNNING" >nul
if errorlevel 1 (
  echo {"phase":"rollback","error":"service_not_running"} > "%LOG%"
  if exist "%BAK%" (
    del /f /q "%EXE%" >nul 2>&1
    move /y "%BAK%" "%EXE%" >nul
  )
  sc start %SVC% >nul 2>&1
  exit /b 4
)

if exist "%BASE%\updates\pending.json" del /f /q "%BASE%\updates\pending.json"
echo {"phase":"applied","result":"ok"} > "%LOG%"
exit /b 0
