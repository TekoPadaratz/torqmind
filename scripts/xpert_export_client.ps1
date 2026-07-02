<#
  TorqMind Xpert Source Explorer — Export no Cliente

  Rodar na maquina que enxerga o SQL Server (CENTRALVR).
  NAO requer acesso ao PostgreSQL STG.
  Somente leitura — nenhuma escrita no banco.

  Uso:
    .\scripts\xpert_export_client.ps1 -IdFilial 14458 -DateFrom 2026-05-01 -DateTo 2026-05-14
#>
param(
    [Parameter(Mandatory=$true)]
    [string]$IdFilial,

    [Parameter(Mandatory=$true)]
    [string]$DateFrom,

    [Parameter(Mandatory=$true)]
    [string]$DateTo,

    [string]$EnvFile = "config\source-explorer.env",

    [string]$OutBase = "logs\source_explorer"
)

$ErrorActionPreference = "Stop"

$outName = "source-comprovantes-$IdFilial-$($DateFrom -replace '-','')-$($DateTo -replace '-','')"
$outDir  = Join-Path $OutBase $outName
$zipFile = "$outName.zip"

Write-Host "============================================" -ForegroundColor Cyan
Write-Host " TorqMind Xpert Source Explorer — Export"    -ForegroundColor Cyan
Write-Host "============================================" -ForegroundColor Cyan
Write-Host ""
Write-Host "  Filial:    $IdFilial"
Write-Host "  Periodo:   $DateFrom a $DateTo"
Write-Host "  Env:       $EnvFile"
Write-Host "  Output:    $outDir"
Write-Host ""

# Verificar Python
if (-not (Get-Command python -ErrorAction SilentlyContinue)) {
    Write-Host "ERRO: Python nao encontrado no PATH." -ForegroundColor Red
    exit 1
}

# Verificar env file
if (-not (Test-Path $EnvFile)) {
    Write-Host "ERRO: Env file nao encontrado: $EnvFile" -ForegroundColor Red
    Write-Host "Copie config\source-explorer.env.example para $EnvFile e preencha." -ForegroundColor Yellow
    exit 1
}

# Testar conexao
Write-Host ">>> Testando conexao SQL Server..." -ForegroundColor Yellow
python tools\xpert_source_explorer.py test-connection --env $EnvFile
if ($LASTEXITCODE -ne 0) {
    Write-Host "ERRO: Conexao SQL Server falhou." -ForegroundColor Red
    exit 1
}
Write-Host ""

# Exportar
Write-Host ">>> Exportando comprovantes..." -ForegroundColor Yellow
python tools\xpert_source_explorer.py export-source-comprovantes-range `
    --env $EnvFile `
    --id-filial $IdFilial `
    --date-from $DateFrom `
    --date-to $DateTo `
    --out $outDir

if ($LASTEXITCODE -ne 0) {
    Write-Host "ERRO: Export falhou." -ForegroundColor Red
    exit 1
}
Write-Host ""

# Verificar manifest
$manifestPath = Join-Path $outDir "source_manifest.json"
if (Test-Path $manifestPath) {
    $manifest = Get-Content $manifestPath | ConvertFrom-Json
    Write-Host ">>> Manifest:" -ForegroundColor Green
    Write-Host "  Row count:  $($manifest.row_count)"
    Write-Host "  SHA256:     $($manifest.sha256.Substring(0, 16))..."
    Write-Host "  Warnings:   $($manifest.warnings -join '; ')"
} else {
    Write-Host "AVISO: Manifest nao encontrado." -ForegroundColor Yellow
}
Write-Host ""

# Compactar
Write-Host ">>> Compactando resultado..." -ForegroundColor Yellow
if (Test-Path $zipFile) { Remove-Item $zipFile -Force }
Compress-Archive -Path $outDir -DestinationPath $zipFile -Force
Write-Host "  Arquivo: $zipFile" -ForegroundColor Green
Write-Host ""

Write-Host "============================================" -ForegroundColor Cyan
Write-Host " Export concluido!"                          -ForegroundColor Green
Write-Host "============================================" -ForegroundColor Cyan
Write-Host ""
Write-Host "Proximo passo:" -ForegroundColor Yellow
Write-Host "  1. Copiar $zipFile para o Linux TorqMind"
Write-Host "  2. Colocar em: imports/source_explorer/"
Write-Host "  3. Rodar compare-source-ledger-to-stg"
Write-Host ""
Write-Host "Exemplo SCP:" -ForegroundColor Gray
Write-Host "  scp -P 14022 $zipFile tm@redevr.ddns.me:/home/tm/torqmind/imports/source_explorer/"
