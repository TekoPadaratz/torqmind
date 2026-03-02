# TorqMind Extractor Agent (apps/agent)

Agent de produção para rodar no servidor do cliente, extrair incrementalmente do SQL Server (Xpert) e enviar para a API TorqMind em **NDJSON**.

## O que este agent garante

- Extração incremental por dataset com watermark por dataset/scope.
- Envio `POST /ingest/{dataset}` no formato NDJSON (1 JSON por linha).
- Suporte opcional a `Content-Encoding: gzip`.
- Header preferencial `X-Ingest-Key` (produção), fallback `X-Empresa-Id` (dev).
- Resiliência: timeout + retry com exponential backoff.
- Observabilidade: logs por dataset/batch/tempo/erro.
- Execução contínua (`run --loop`) e pontual (`run --once`) + `backfill` + `check`.

## Pré-requisitos no servidor do cliente

1. Python 3.10+
2. Driver ODBC SQL Server instalado (`ODBC Driver 17` ou `18`).
3. Conectividade de rede:
- Servidor SQL Server (porta padrão 1433 ou custom).
- API TorqMind (ex: `https://torqmind.com/api`).

## Instalação

```bash
cd apps/agent
python -m venv .venv
source .venv/bin/activate  # no Windows: .venv\Scripts\activate
pip install -r requirements.txt
```

## Configuração

Arquivo base: `config.yaml`.

Campos principais:
- `sqlserver.dsn` **ou** `sqlserver.server/database/user/password/driver`
- `api.base_url`
- `api.ingest_key` (produção)
- `api.empresa_id` (somente dev)
- `batch_size`, `fetch_size`, `max_retries`, `timeout_seconds`, `gzip_enabled`
- `datasets.<dataset>.enabled/table/watermark_column/query/watermark_style`

### Env overrides

- `TORQMIND_API_BASE_URL`
- `TORQMIND_INGEST_KEY`
- `TORQMIND_EMPRESA_ID`
- `TORQMIND_SQLSERVER_DSN`
- `TORQMIND_SQLSERVER_SERVER`
- `TORQMIND_SQLSERVER_DATABASE`
- `TORQMIND_SQLSERVER_USER`
- `TORQMIND_SQLSERVER_PASSWORD`
- `TORQMIND_SQLSERVER_DRIVER`
- `TORQMIND_BATCH_SIZE`
- `TORQMIND_FETCH_SIZE`
- `TORQMIND_MAX_RETRIES`
- `TORQMIND_TIMEOUT_SECONDS`
- `TORQMIND_GZIP_ENABLED`
- `TORQMIND_ENABLED_DATASETS` (csv, ex: `comprovantes,movprodutos,itensmovprodutos`)
- `TORQMIND_ID_EMPRESA`, `TORQMIND_ID_DB`
- `TORQMIND_STATE_DIR`

## Comandos

### Check completo

```bash
python -m agent check --config config.yaml
```

Valida:
- conexão SQL Server (`SELECT 1`)
- ping API (`GET /health`)
- ingest credentials (`POST /ingest/filiais` vazio)

### Um ciclo de extração/envio

```bash
python -m agent run --once --config config.yaml
```

Resetando watermark de um dataset antes do ciclo:

```bash
python -m agent run --once --reset-watermark comprovantes --config config.yaml
```

### Daemon

```bash
python -m agent run --loop --interval 60 --config config.yaml
```

### Backfill de dataset

```bash
python -m agent backfill --dataset comprovantes --from 2026-01-01 --to 2026-03-01 --config config.yaml
```

### Reset de watermark (comando dedicado)

```bash
python -m agent reset --dataset comprovantes --config config.yaml
```

## Watermarks/State

- Novo formato por dataset:
  - `state/empresa_<id>/<dataset>.json`
- Escopo interno por db/filial (chave `db:<id_db>`).
- Gravação atômica (`tmp + rename`).
- Compatibilidade: se houver `state.json` legado, ele é migrado automaticamente na inicialização.
- Watermark é persistido em ISO 8601 (`datetime.fromisoformat`).

## Datasets suportados

`filiais`, `funcionarios`, `entidades`/`clientes`, `grupoprodutos`, `localvendas`, `produtos`, `turnos`, `comprovantes`, `movprodutos`, `itensmovprodutos`, `contaspagar`, `contasreceber`, `financeiro`.

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
- `config.yaml` legado (`api_url`, `id_empresa`, `id_db`) continua aceito.
- Mapeamento base dos datasets críticos preservado:
  - `COMPROVANTES`, `MOVPRODUTOS`, `ITENSMOVPRODUTOS`
  - watermark padrão em `DATAREPL`
