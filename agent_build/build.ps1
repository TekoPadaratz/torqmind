$ErrorActionPreference = "Stop"
$ScriptRoot = Split-Path -Parent $MyInvocation.MyCommand.Path
$RepoRoot = Resolve-Path (Join-Path $ScriptRoot "..")

Set-Location $RepoRoot

Write-Host "Instalando dependencias..."

pip install -r apps/agent/requirements.txt
pip install pyinstaller

Write-Host "Compilando agent..."

pyinstaller `
  --onefile `
  --clean `
  --name torqmind-agent `
  --paths apps/agent `
  apps/agent/main.py

Write-Host "Criando estrutura de release..."

Remove-Item release -Recurse -Force -ErrorAction SilentlyContinue

New-Item -ItemType Directory release | Out-Null

Copy-Item dist/torqmind-agent.exe release/
Copy-Item agent_build/service/torqmind-agent-service.exe release/
Copy-Item agent_build/service/torqmind-agent-service.xml.template release/
Copy-Item agent_build/update-config.bat release/

Write-Host "Build finalizado."
