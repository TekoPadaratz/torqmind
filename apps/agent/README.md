# TorqMind Extractor Agent (apps/agent)

Agent de produção para rodar no servidor do cliente, extrair incrementalmente do SQL Server (Xpert) e enviar para a API TorqMind em **NDJSON**.

## Modelo de configuração corporativo

O padrão premium usa um único arquivo criptografado:

- `config.enc`: configuração completa criptografada com **DPAPI LocalMachine**
- formato em disco: payload binário DPAPI; descriptografia apenas em memória

Em produção Windows, o diretório final do agent não deve conter `config.yaml` com dados reais.
Toda a configuração é descriptografada apenas em memória pelo agent.

## O que este agent garante

- Extração incremental por dataset com watermark por dataset/scope.
- Envio `POST /ingest/{dataset}` no formato NDJSON (1 JSON por linha).
- Suporte opcional a `Content-Encoding: gzip`.
- Header preferencial `X-Ingest-Key` (produção), fallback `X-Empresa-Id` (dev).
- Resiliência: timeout + retry com exponential backoff.
- Spool offline em disco: se API cair, os lotes são preservados e reenviados no próximo ciclo.
- Observabilidade: logs por dataset/batch/tempo/erro.
- Execução contínua (`run --loop`) e pontual (`run --once`) + `backfill` + `check`.

## Pré-requisitos no servidor do cliente

Para instalação premium no Windows:

1. Não exige Python instalado no cliente.
2. Driver ODBC SQL Server instalado (`ODBC Driver 17` ou `18`).
3. Conectividade de rede:
- Servidor SQL Server (porta padrão 1433 ou custom).
- API TorqMind (ex: `https://torqmind.com/api`).

Para rodar por código-fonte durante desenvolvimento:

1. Python 3.10+

## Instalação por código-fonte

```bash
cd apps/agent
python -m venv .venv
source .venv/bin/activate  # no Windows: .venv\Scripts\activate
pip install -r requirements.txt
```

## Configuração

Arquivos:
- `config.example.yaml`: exemplo de desenvolvimento/local (versionado)
- `config.local.yaml`: configuração local de desenvolvimento
- `config.enc`: configuração completa criptografada usada no pacote Windows

Crie o local a partir do exemplo:

```bash
cp config.example.yaml config.local.yaml
```

Para desenvolvimento, a CLI pode continuar usando `config.local.yaml`.
Para produção Windows, use `config.enc`.

Para a semântica correta de Caixa e Antifraude, mantenha sempre habilitados:

- `datasets.usuarios.enabled = true`
- `datasets.turnos.enabled = true`

Campos principais:
- `sqlserver.dsn` **ou** `sqlserver.server/port/database/user/driver`
- `api.base_url`
- `api.empresa_id` (somente dev)
- `batch_size`, `fetch_size`, `max_retries`, `timeout_seconds`, `gzip_enabled`
- `spool_dir`, `spool_flush_max_files`
- `datasets.<dataset>.enabled/table/watermark_column/query/watermark_style`

### Env overrides

- `TORQMIND_API_BASE_URL`
- `TORQMIND_INGEST_KEY`
- `TORQMIND_EMPRESA_ID`
- `TORQMIND_SQLSERVER_DSN`
- `TORQMIND_SQLSERVER_SERVER`
- `TORQMIND_SQLSERVER_PORT`
- `TORQMIND_SQLSERVER_DATABASE`
- `TORQMIND_SQLSERVER_USER`
- `TORQMIND_SQLSERVER_PASSWORD`
- `TORQMIND_SQLSERVER_DRIVER`
- `TORQMIND_SQLSERVER_ENCRYPT` (`true/false`)
- `TORQMIND_SQLSERVER_TRUST_SERVER_CERTIFICATE` (`true/false`)
- `TORQMIND_SQLSERVER_LOGIN_TIMEOUT_SECONDS`
- `TORQMIND_BATCH_SIZE`
- `TORQMIND_FETCH_SIZE`
- `TORQMIND_MAX_RETRIES`
- `TORQMIND_TIMEOUT_SECONDS`
- `TORQMIND_GZIP_ENABLED`
- `TORQMIND_ENABLED_DATASETS` (csv, ex: `comprovantes,movprodutos,itensmovprodutos`)
- `TORQMIND_ID_EMPRESA`, `TORQMIND_ID_DB`
- `TORQMIND_STATE_DIR`
- `TORQMIND_SPOOL_DIR`
- `TORQMIND_SPOOL_FLUSH_MAX_FILES`

## Comandos

### Configuração criptografada

Criar `config.enc`:

```bash
python -m agent config init --config config.enc --interactive
```

Alterar um ou mais campos:

```bash
python -m agent config set --config config.enc --api-base-url https://api.torqmind.com --interval-seconds 60
```

Editar de forma interativa:

```bash
python -m agent config edit --config config.enc --interactive
```

Visualizar resumo mascarado:

```bash
python -m agent config show-safe --config config.enc
```

Migrar YAML legado para `config.enc`:

```bash
python -m agent config migrate-from-yaml --source config.local.yaml --config config.enc
```

### Check completo

```bash
python -m agent check --config config.enc
```

Valida:
- conexão SQL Server (`SELECT 1`)
- ping API (`GET /health`)
- ingest credentials (`POST /ingest/filiais` vazio)

### Teste completo de configuração

```bash
python -m agent config test --config config.enc
```

Valida:
- leitura e descriptografia do `config.enc`
- conexão SQL Server
- reachability da API
- validação das credenciais de ingestão

### Um ciclo de extração/envio

```bash
python -m agent run --once --config config.enc
```

Resetando watermark de um dataset antes do ciclo:

```bash
python -m agent run --once --reset-watermark comprovantes --config config.enc
```

Processando todos os datasets habilitados sem abortar no primeiro erro:

```bash
python -m agent run --once --continue-on-error --config config.enc
```

### Daemon

```bash
python -m agent run --loop --interval 60 --config config.enc
```

### Backfill de dataset

```bash
python -m agent backfill --dataset comprovantes --from 2026-01-01 --to 2026-03-01 --config config.enc
```

### Reset de watermark (comando dedicado)

```bash
python -m agent reset-watermark --dataset comprovantes --config config.enc
```

### Schema scan (AR/AP)

```bash
python -m agent schema-scan --keywords "PAGAR,RECEBER,TITULO,DUPLICATA,FINANC" --config config.enc
```

Saída padrão: `docs/xpert_schema_report.json`
com ranking de tabelas candidatas e amostra `TOP 5`.

## Watermarks/State

- Novo formato por dataset:
  - `state/empresa_<id>/<dataset>.json`
- Escopo interno por db/filial (chave `db:<id_db>`).
- Gravação atômica (`tmp + rename`).
- Compatibilidade: se houver `state.json` legado, ele é migrado automaticamente na inicialização.
- Watermark é persistido em ISO 8601 (`datetime.fromisoformat`).

## Spool offline

- Quando a API retorna erro/rede indisponível, o lote vai para `spool_dir`.
- No próximo `run`, o agent tenta reenviar primeiro a fila pendente.
- A fila é persistente em disco (`*.ndjson` / `*.ndjson.gz`) para evitar perda de dados.

## Datasets suportados

`filiais`, `funcionarios`, `entidades`/`clientes`, `grupoprodutos`, `localvendas`, `produtos`, `turnos`, `comprovantes`, `movprodutos`, `itensmovprodutos`, `formas_pgto_comprovantes`, `contaspagar`, `contasreceber`, `financeiro`.

## Watermark em coluna texto (SQL Server)

Se a coluna de watermark for `varchar/nvarchar`, o agent aplica:

1. `TRY_CONVERT(datetime2, <col>, 121)` (ISO)
2. fallback para `TRY_CONVERT(datetime2, <col>, 103)` quando houver watermark e a primeira estratégia retornar 0 linhas.

Você também pode fixar por dataset:

```yaml
datasets:
  comprovantes:
    watermark_column: DATAREPL
    watermark_style: 103
```

## Testes

```bash
cd apps/agent
python -m unittest discover -s tests -v
```

## Docker (opcional)

```bash
cd apps/agent
docker build -f DockerFile -t torqmind-agent .
docker run --rm torqmind-agent
```

## Compatibilidade com legado

- `main.py` continua existindo e chama a nova CLI.
- `config.local.yaml` legado (`api_url`, `id_empresa`, `id_db`) continua aceito.
- `config.yaml` / `config.local.yaml` legados podem ser migrados com `config migrate-from-yaml`.
- Mapeamento base dos datasets críticos preservado:
  - `COMPROVANTES`, `MOVPRODUTOS`, `ITENSMOVPRODUTOS`
  - watermark padrão em `DATAREPL`

## Pacote Windows premium

Build local no Windows:

```powershell
cd agent_build
.\build.ps1
```

O release gerado inclui:

- `torqmind-agent.exe`
- `torqmind-agent-service.exe`
- `torqmind-agent-service.xml.template`
- `update-config.bat`

Instalador Inno Setup:

- script: `agent_build/installer/setup.iss`
- compilar com checkout local Windows, não via `\\wsl.localhost\...`

Fluxo do instalador:

1. copia binários
2. coleta os dados no wizard
3. cria `config.enc` criptografado
4. aplica ACL local
5. instala serviço Windows
6. configura restart automático e delayed auto-start
7. inicia o serviço

Troca posterior da configuração:

```bat
update-config.bat
```

Esse utilitário chama o agent em modo interativo e regrava `config.enc` sem reinstalação.

Verificar serviço no Windows:

```powershell
Get-Service TorqMindAgent
sc.exe qc TorqMindAgent
sc.exe query TorqMindAgent
```
