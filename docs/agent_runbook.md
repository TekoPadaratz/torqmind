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

## 3) Configuração (`config.enc`)

Campos críticos:

- `sqlserver.server`, `sqlserver.database`, `sqlserver.user`, `sqlserver.password`
- `api.base_url`
- produção: `api.ingest_key`
- dev/homolog: `api.empresa_id`
- `state_dir` (watermarks)
- `spool_dir` (fila offline)
- defaults recomendados para SQL Server:
  - `driver = ODBC Driver 18 for SQL Server`
  - `encrypt = true`
  - `trust_server_certificate = false`

Datasets mínimos habilitados:

- `comprovantes`, `movprodutos`, `itensmovprodutos`
- `produtos`, `grupoprodutos`, `entidades/clientes`, `funcionarios`
- `filiais`, `localvendas`, `turnos`
- `contaspagar`, `contasreceber` (ou equivalentes mapeados por schema-scan)

## 4) Comandos operacionais

Check de conectividade:

```powershell
python -m agent check --config config.enc
```

Rodar uma vez:

```powershell
python -m agent run --once --config config.enc
```

Loop contínuo:

```powershell
python -m agent run --loop --interval 60 --config config.enc
```

Backfill:

```powershell
python -m agent backfill --dataset comprovantes --from 2026-01-01 --to 2026-02-01 --config config.enc
```

Reset watermark:

```powershell
python -m agent reset-watermark --dataset comprovantes --config config.enc
```

Schema scan AR/AP:

```powershell
python -m agent schema-scan --keywords "PAGAR,RECEBER,TITULO,DUPLICATA,FINANC" --config config.enc
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
     /c cd /d C:\TorqMind\apps\agent && .venv\Scripts\python.exe -m agent run --loop --interval 60 --config config.enc
     ```
4. Marcar:
   - "Run whether user is logged on or not"
   - "Restart task if it fails"

## 6) Troubleshooting rápido

- Erro de driver ODBC:
  - validar `driver` no `config.enc` (`ODBC Driver 18 for SQL Server`)
- Erro TLS/Certificado:
  - ajustar `encrypt` e `trust_server_certificate`
- API fora:
  - verificar `/health`; lotes devem ir para `spool_dir`
- Sem dados subindo:
  - revisar `watermark_column`/`watermark_style`
  - executar `reset-watermark` e `run --once`
- Encontrar tabelas financeiras:
  - executar `schema-scan` e revisar ranking/colunas amostradas

## 7) Build do executável e atualização de versão (seguro)

### 7.1 Gerar o .exe (uma vez por versão)

No repositório (Windows, PowerShell):

```powershell
.\agent_build\build.ps1
```

O script valida a compilação, roda o PyInstaller e monta a pasta `release/` com:
`torqmind-agent.exe`, `config.example.yaml`, `update-config.bat`, wrapper de
serviço e o SHA256 + commit do build. **O mapeamento das tabelas já está
embutido no `.exe`** (via `apps/agent/agent/config.py` → `DEFAULT_DATASETS`).

### 7.2 Config do cliente = criptografado, só segredos (nunca .yaml em texto)

O agente usa `config.enc` (cifrado com Windows DPAPI, amarrado à máquina —
não abre no Notepad, não expõe senha/ingest_key). O `config.enc` do cliente só
precisa de conexão SQL + `api.ingest_key` + `id_empresa`; **não precisa do
mapeamento de tabelas**.

Criar/editar no servidor do cliente:

```powershell
torqmind-agent.exe config init --interactive --config config.enc
# ou, para editar depois:
torqmind-agent.exe config edit --interactive --config config.enc   # (update-config.bat)
```

Migrar um cliente antigo que ainda usa `config.yaml` em texto:

```powershell
torqmind-agent.exe config migrate-from-yaml --source config.yaml --config config.enc
del config.yaml   # apaga o arquivo em texto do servidor do cliente
```

### 7.3 Atualizar um cliente para uma versão nova (tabelas novas)

Como o mapeamento vem embutido no `.exe`, **não é preciso copiar nada de
mapeamento**. Basta trocar o binário:

```powershell
sc stop TorqMindAgent
copy /Y torqmind-agent.exe C:\TorqMind\torqmind-agent.exe   # substitui o exe
sc start TorqMindAgent
```

O `config.enc` permanece intacto (conexão preservada) e as tabelas novas
(cheques, situacoes, movcreditoentidades, credito, consolearquivo, tanques,
movtanques, estoque) começam a coletar automaticamente. Validar:

```powershell
torqmind-agent.exe check --config config.enc
torqmind-agent.exe run --once --config config.enc
```

### 7.4 Ligar/desligar datasets por cliente (opcional)

Por padrão todos os datasets habilitados no binário coletam. Para restringir em
um cliente específico, use a env `TORQMIND_ENABLED_DATASETS` (lista separada por
vírgula) ou um override mínimo no `config.enc` (`datasets: { X: { enabled: false } }`).

