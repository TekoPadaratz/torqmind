# TorqMind Agent (ETL no cliente)

MVP: envia NDJSON gzip para a API.
Depois você pluga o extrator SQL Server (Xpert) e envia datasets reais.

## Rodar
```bash
python -m venv .venv
.venv\Scripts\activate
pip install -r requirements.txt
python agent.py --api http://localhost:8000
```
