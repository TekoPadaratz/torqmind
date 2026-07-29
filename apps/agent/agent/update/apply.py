"""Prepare Windows WinSW self-update apply script (stop → swap → start → rollback)."""

from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path
from typing import Optional

from agent.update.manifest import write_json

SERVICE_NAME = "TorqMindAgent"
EXE_NAME = "torqmind-agent.exe"


def agent_base_dir(explicit: Optional[Path] = None) -> Path:
    if explicit is not None:
        return explicit
    if getattr(sys, "frozen", False):
        return Path(sys.executable).resolve().parent
    return Path.cwd()


def write_apply_script(
    base_dir: Path,
    *,
    service_name: str = SERVICE_NAME,
    staged_new: Optional[Path] = None,
) -> Path:
    """Write ``torqmind-agent-update.bat`` that swaps the exe safely."""
    base_dir = base_dir.resolve()
    updates = base_dir / "updates"
    backup = base_dir / "backup"
    updates.mkdir(parents=True, exist_ok=True)
    backup.mkdir(parents=True, exist_ok=True)
    new_exe = staged_new or (updates / f"{EXE_NAME}.new")
    bat = updates / "torqmind-agent-update.bat"
    # cmd.exe script: stop service, backup current, promote .new, start, rollback on failure
    content = f"""@echo off
setlocal EnableExtensions
set BASE={base_dir}
set SVC={service_name}
set EXE=%BASE%\\{EXE_NAME}
set NEW={new_exe}
set BAK=%BASE%\\backup\\{EXE_NAME}
set LOG=%BASE%\\updates\\last_result.json
set RESULT=ok

echo {{"phase":"start"}} > "%LOG%"

sc stop %SVC% >nul 2>&1
timeout /t 5 /nobreak >nul

if not exist "%NEW%" (
  echo {{"phase":"abort","error":"missing_new_exe"}} > "%LOG%"
  sc start %SVC% >nul 2>&1
  exit /b 2
)

if exist "%EXE%" (
  if exist "%BAK%" del /f /q "%BAK%"
  move /y "%EXE%" "%BAK%" >nul
)

move /y "%NEW%" "%EXE%" >nul
if errorlevel 1 (
  echo {{"phase":"swap_failed"}} > "%LOG%"
  if exist "%BAK%" move /y "%BAK%" "%EXE%" >nul
  sc start %SVC% >nul 2>&1
  exit /b 3
)

sc start %SVC% >nul 2>&1
timeout /t 8 /nobreak >nul
sc query %SVC% | find "RUNNING" >nul
if errorlevel 1 (
  echo {{"phase":"rollback","error":"service_not_running"}} > "%LOG%"
  if exist "%BAK%" (
    del /f /q "%EXE%" >nul 2>&1
    move /y "%BAK%" "%EXE%" >nul
  )
  sc start %SVC% >nul 2>&1
  exit /b 4
)

if exist "%BASE%\\updates\\pending.json" del /f /q "%BASE%\\updates\\pending.json"
echo {{"phase":"applied","result":"ok"}} > "%LOG%"
exit /b 0
"""
    bat.write_text(content, encoding="utf-8")
    return bat


def spawn_apply_detached(bat_path: Path) -> None:
    """Launch updater outside the running agent process (Windows)."""
    if os.name != "nt":
        # Dev/Linux: only write the script; do not execute sc.exe.
        write_json(
            bat_path.parent / "last_result.json",
            {"phase": "scheduled_noop_non_windows", "bat": str(bat_path)},
        )
        return
    flags = 0
    if hasattr(subprocess, "DETACHED_PROCESS"):
        flags |= subprocess.DETACHED_PROCESS  # type: ignore[attr-defined]
    if hasattr(subprocess, "CREATE_NEW_PROCESS_GROUP"):
        flags |= subprocess.CREATE_NEW_PROCESS_GROUP  # type: ignore[attr-defined]
    subprocess.Popen(
        ["cmd.exe", "/c", str(bat_path)],
        cwd=str(bat_path.parent),
        close_fds=True,
        creationflags=flags,
    )


def prepare_and_schedule_update(
    base_dir: Path,
    *,
    staged_new: Path,
    service_name: str = SERVICE_NAME,
) -> Path:
    bat = write_apply_script(base_dir, service_name=service_name, staged_new=staged_new)
    spawn_apply_detached(bat)
    return bat
