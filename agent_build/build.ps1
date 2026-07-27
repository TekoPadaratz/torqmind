$ErrorActionPreference = "Stop"
$ScriptRoot = Split-Path -Parent $MyInvocation.MyCommand.Path
$RepoRoot = Resolve-Path (Join-Path $ScriptRoot "..")

Set-Location $RepoRoot

Write-Host "=== TorqMind Agent Build ===" -ForegroundColor Cyan
Write-Host "Commit: $(git log -1 --format='%h %s')"
$AgentVersion = python -c "import sys; sys.path.insert(0,'apps/agent'); from agent import __version__; print(__version__)"
Write-Host "Agent version: $AgentVersion"
Write-Host ""

# --- Step 1: Dependencies ---
Write-Host "Instalando dependencias..." -ForegroundColor Yellow

pip install -r apps/agent/requirements.txt
pip install pyinstaller

# --- Step 2: Compile check ---
Write-Host "Validando compilacao..." -ForegroundColor Yellow
python -m compileall apps/agent/agent -q
if ($LASTEXITCODE -ne 0) { Write-Host "FALHA: compileall" -ForegroundColor Red; exit 1 }

# --- Step 2b: Datasets embutidos neste build (referencia) ---
Write-Host "Datasets embutidos neste build (coletam sozinhos, sem copiar mapeamento):" -ForegroundColor Yellow
python -c "import sys; sys.path.insert(0,'apps/agent'); from agent import config as c; on=[n for n,s in c.DEFAULT_DATASETS.items() if s.get('enabled')]; print('  ENABLED (' + str(len(on)) + '): ' + ', '.join(on))"

# --- Step 3: PyInstaller build ---
Write-Host "Compilando agent com PyInstaller..." -ForegroundColor Yellow

pyinstaller `
  --onefile `
  --clean `
  --noconfirm `
  --name torqmind-agent `
  --paths apps/agent `
  --hidden-import agent.cli `
  --hidden-import agent.config `
  --hidden-import agent.runner `
  --hidden-import agent.secrets `
  --hidden-import agent.state `
  --hidden-import agent.state.watermark `
  --hidden-import agent.extractors `
  --hidden-import agent.extractors.base `
  --hidden-import agent.extractors.xpert `
  --hidden-import agent.sink `
  --hidden-import agent.sink.torqmind_api `
  --hidden-import agent.spool `
  --hidden-import agent.spool.queue `
  --hidden-import agent.utils `
  --hidden-import agent.utils.log `
  --hidden-import agent.utils.ndjson `
  --hidden-import agent.utils.retry `
  --hidden-import agent.utils.timezone `
  apps/agent/main.py

if ($LASTEXITCODE -ne 0) { Write-Host "FALHA: PyInstaller" -ForegroundColor Red; exit 1 }

# --- Step 4: Create release structure ---
Write-Host "Criando estrutura de release..." -ForegroundColor Yellow

Remove-Item release -Recurse -Force -ErrorAction SilentlyContinue
New-Item -ItemType Directory release | Out-Null

Copy-Item dist/torqmind-agent.exe release/
Copy-Item apps/agent/config.example.yaml release/
Copy-Item agent_build/update-config.bat release/
Copy-Item agent_build/service/torqmind-agent-service.xml.template release/

# Copy service wrapper if available
if (Test-Path agent_build/service/torqmind-agent-service.exe) {
    Copy-Item agent_build/service/torqmind-agent-service.exe release/
}

# --- Step 5: Verify exe ---
Write-Host "Verificando exe..." -ForegroundColor Yellow
$exePath = "release/torqmind-agent.exe"

if (-not (Test-Path $exePath)) {
    Write-Host "FALHA: $exePath nao encontrado" -ForegroundColor Red
    exit 1
}

& $exePath --help
if ($LASTEXITCODE -ne 0) { Write-Host "AVISO: --help retornou erro (pode ser normal sem config)" -ForegroundColor Yellow }

# --- Step 6: SHA256 hash ---
Write-Host ""
$hash = (Get-FileHash $exePath -Algorithm SHA256).Hash
$size = (Get-Item $exePath).Length
Write-Host "=== Build finalizado com sucesso ===" -ForegroundColor Green
Write-Host "EXE:    $exePath"
Write-Host "Size:   $([math]::Round($size / 1MB, 2)) MB"
Write-Host "SHA256: $hash"
Write-Host "Commit: $(git log -1 --format='%H')"
Write-Host ""
Write-Host "Proximo passo: copiar SOMENTE o torqmind-agent.exe para o servidor do cliente" -ForegroundColor Cyan
Write-Host "  (o mapeamento das tabelas ja vem embutido no .exe)." -ForegroundColor Cyan
Write-Host "  sc stop TorqMindAgent; copy /Y torqmind-agent.exe <destino>; sc start TorqMindAgent" -ForegroundColor Cyan
Write-Host "  O config.enc do cliente NAO muda. Config seguro: torqmind-agent.exe config init --interactive --config config.enc" -ForegroundColor Cyan
