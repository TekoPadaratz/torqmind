# Xpert Source Explorer — Runbook de Export no Cliente

## Objetivo

Exportar comprovantes e NFE do SQL Server do cliente (CENTRALVR/ATXDADOS)
para arquivo local (CSV + manifest), que será transferido para o Linux
TorqMind para comparação contra a STG PostgreSQL.

**Este comando é somente leitura. Não executa INSERT/UPDATE/DELETE.**

---

## Onde rodar

- Na **máquina do Agent** (Windows, na LAN do cliente)
- Ou em qualquer máquina que resolva `CENTRALVR` e acesse a porta 1433
- **Nunca** na VM Linux 172.30.0.10 se ela não enxerga o SQL Server

---

## 1. Pré-requisitos

- Python 3.10+ instalado
- `pymssql` instalado:
  ```
  pip install pymssql
  ```
  Ou `pyodbc` com ODBC Driver 17 for SQL Server
- Acesso de rede ao SQL Server (porta 1433)

---

## 2. Testar rede

PowerShell:

```powershell
Test-NetConnection CENTRALVR -Port 1433
```

Se o hostname não resolver:

```powershell
nslookup CENTRALVR
```

Se necessário, usar IP direto em vez do hostname.

---

## 3. Preparar ambiente

Copiar o diretório `tools/` e `config/` do repositório TorqMind
para a máquina do cliente. O mínimo necessário é:

```
tools/xpert_source_explorer.py
config/source-explorer.env.example
```

Criar o env real:

PowerShell:
```powershell
copy config\source-explorer.env.example config\source-explorer.env
```

Linux/Mac:
```bash
cp config/source-explorer.env.example config/source-explorer.env
chmod 600 config/source-explorer.env
```

Editar `config/source-explorer.env` com os dados reais:

```
SQLSERVER_HOST=CENTRALVR
SQLSERVER_PORT=1433
SQLSERVER_DATABASE=ATXDADOS
SQLSERVER_USER=sa
SQLSERVER_PASSWORD=<SENHA_REAL>
SQLSERVER_ENCRYPT=no
SQLSERVER_TRUST_CERT=yes
SQLSERVER_TIMEOUT_SECONDS=30
```

**NUNCA commitar este arquivo no Git.**

---

## 4. Testar conexão

```
python tools/xpert_source_explorer.py test-connection --env config/source-explorer.env
```

Deve mostrar: versão do SQL Server, tabelas encontradas, COMPROVANTES e NFE.

---

## 5. Exportar comprovantes

### Filial 14458, 1-14 maio 2026

PowerShell:
```powershell
python tools\xpert_source_explorer.py export-source-comprovantes-range `
  --env config\source-explorer.env `
  --id-filial 14458 `
  --date-from 2026-05-01 `
  --date-to 2026-05-14 `
  --out logs\source_explorer\source-comprovantes-14458-20260501-20260514
```

Linux/Mac:
```bash
python tools/xpert_source_explorer.py export-source-comprovantes-range \
  --env config/source-explorer.env \
  --id-filial 14458 \
  --date-from 2026-05-01 \
  --date-to 2026-05-14 \
  --out logs/source_explorer/source-comprovantes-14458-20260501-20260514
```

### Arquivos gerados

```
source_ledger.csv           ← ledger completo (1 linha por comprovante)
source_ledger.jsonl         ← mesmo em JSON Lines
source_summary_by_day.csv   ← totais por dia
source_summary_by_day.json
source_manifest.json        ← metadata com SHA256
source_export_report.md     ← relatório legível
nfe_status5_source.csv      ← NFE inutilizadas
situacao3_source.csv        ← situação=3
cancelados_source.csv       ← cancelados
source_duplicate_keys.csv   ← (somente se houver duplicatas)
```

---

## 6. Verificar resultado

Abrir `source_export_report.md` para ver totais.
Verificar `source_manifest.json` para confirmar `row_count` e `sha256`.

---

## 7. Compactar e transferir

PowerShell:
```powershell
Compress-Archive `
  -Path logs\source_explorer\source-comprovantes-14458-20260501-20260514 `
  -DestinationPath source-comprovantes-14458-20260501-20260514.zip `
  -Force
```

Linux/tar:
```bash
tar -czf source-comprovantes-14458-20260501-20260514.tgz \
  -C logs/source_explorer \
  source-comprovantes-14458-20260501-20260514
```

Transferir o ZIP/TGZ para o Linux TorqMind via SCP, SFTP ou cópia manual:

```bash
scp -P 14022 source-comprovantes-14458-20260501-20260514.zip \
  tm@redevr.ddns.me:/home/tm/torqmind/imports/source_explorer/
```

---

## 8. No Linux TorqMind — comparar

```bash
cd /home/tm/torqmind
mkdir -p imports/source_explorer

# Descompactar
unzip imports/source_explorer/source-comprovantes-14458-20260501-20260514.zip \
  -d imports/source_explorer/
# ou: tar -xzf ... -C imports/source_explorer/

# Testar STG
python tools/xpert_source_explorer.py test-stg-connection \
  --env config/source-explorer.env

# Comparar (14 dias)
python tools/xpert_source_explorer.py compare-source-ledger-to-stg \
  --env config/source-explorer.env \
  --source-ledger imports/source_explorer/source-comprovantes-14458-20260501-20260514/source_ledger.csv \
  --source-manifest imports/source_explorer/source-comprovantes-14458-20260501-20260514/source_manifest.json \
  --id-filial 14458 \
  --date-from 2026-05-01 \
  --date-to 2026-05-14 \
  --freeze-stg-snapshot-out \
  --out logs/source_explorer/compare-real-source-to-stg-14458-20260501-20260514

# Comparar dia crítico (11 maio)
python tools/xpert_source_explorer.py compare-source-ledger-to-stg \
  --env config/source-explorer.env \
  --source-ledger imports/source_explorer/source-comprovantes-14458-20260501-20260514/source_ledger.csv \
  --source-manifest imports/source_explorer/source-comprovantes-14458-20260501-20260514/source_manifest.json \
  --id-filial 14458 \
  --date-from 2026-05-11 \
  --date-to 2026-05-11 \
  --freeze-stg-snapshot-out \
  --out logs/source_explorer/compare-real-source-to-stg-14458-20260511
```

---

## 9. Segurança

- **Nunca commitar** `config/source-explorer.env` (contém senha)
- **Nunca commitar** `logs/source_explorer/` ou `imports/source_explorer/` (contêm dados do cliente)
- Chaves de acesso NFE são mascaradas automaticamente nos CSV
- O manifest **não contém senha**

---

## 10. Troubleshooting

| Problema | Solução |
|----------|---------|
| `pymssql.OperationalError: connection refused` | Verificar firewall, porta 1433, nome do host |
| `pyodbc not installed` | `pip install pyodbc` + instalar ODBC Driver 17 |
| `COMPROVANTES table not found` | O database pode ser diferente de ATXDADOS |
| `DATA column not found` | Schema incompatível; reportar |
| `SHA256 mismatch` no compare | O CSV foi alterado após export; re-exportar |
| `Manifest id_filial mismatch` | Verificar se o --id-filial bate com o manifest |
