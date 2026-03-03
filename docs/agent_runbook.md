# TorqMind Agent Runbook (Windows / SQL Server Xpert)

Data: 2026-03-03

## 1) Pré-requisitos

- Windows Server/Windows 10+ com Python 3.10+
- ODBC Driver SQL Server:
  - recomendado: **ODBC Driver 18 for SQL Server**
  - alternativa: ODBC Driver 17
- Acesso de rede:
  - SQL Server Xpert (`SERVER:PORT`)
  - API TorqMind (`https://.../health`)

## 2) Instalação

```powershell
cd apps\agent
python -m venv .venv
.venv\Scripts\activate
pip install -r requirements.txt
```

## 3) Configuração (`config.yaml`)

Campos críticos:

- `sqlserver.server`, `sqlserver.database`, `sqlserver.user`, `sqlserver.password`
- `api.base_url`
- produção: `api.ingest_key`
- dev/homolog: `api.empresa_id`
- `state_dir` (watermarks)
- `spool_dir` (fila offline)

Datasets mínimos habilitados:

- `comprovantes`, `movprodutos`, `itensmovprodutos`
- `produtos`, `grupoprodutos`, `entidades/clientes`, `funcionarios`
- `filiais`, `localvendas`, `turnos`
- `contaspagar`, `contasreceber` (ou equivalentes mapeados por schema-scan)

## 4) Comandos operacionais

Check de conectividade:

```powershell
python -m agent check --config config.yaml
```

Rodar uma vez:

```powershell
python -m agent run --once --config config.yaml
```

Loop contínuo:

```powershell
python -m agent run --loop --interval 60 --config config.yaml
```

Backfill:

```powershell
python -m agent backfill --dataset comprovantes --from 2026-01-01 --to 2026-02-01 --config config.yaml
```

Reset watermark:

```powershell
python -m agent reset-watermark --dataset comprovantes --config config.yaml
```

Schema scan AR/AP:

```powershell
python -m agent schema-scan --keywords "PAGAR,RECEBER,TITULO,DUPLICATA,FINANC" --config config.yaml
```

Saída: `docs/xpert_schema_report.json`

## 5) Execução como serviço (Task Scheduler)

Opção simples (sem NSSM):

1. Criar tarefa no Task Scheduler.
2. Trigger: `At startup`.
3. Action:
   - Program/script: `cmd.exe`
   - Args:
     ```text
     /c cd /d C:\TorqMind\apps\agent && .venv\Scripts\python.exe -m agent run --loop --interval 60 --config config.yaml
     ```
4. Marcar:
   - "Run whether user is logged on or not"
   - "Restart task if it fails"

## 6) Troubleshooting rápido

- Erro de driver ODBC:
  - validar `driver` no `config.yaml` (`ODBC Driver 18 for SQL Server`)
- Erro TLS/Certificado:
  - ajustar `encrypt` e `trust_server_certificate`
- API fora:
  - verificar `/health`; lotes devem ir para `spool_dir`
- Sem dados subindo:
  - revisar `watermark_column`/`watermark_style`
  - executar `reset-watermark` e `run --once`
- Encontrar tabelas financeiras:
  - executar `schema-scan` e revisar ranking/colunas amostradas
