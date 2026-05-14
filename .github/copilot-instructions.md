# Copilot Instructions — TorqMind

Você está trabalhando no TorqMind, SaaS BI premium para redes de postos de combustíveis.

## Prioridades

1. Segurança de dados e permissões.
2. Confiabilidade ponta a ponta.
3. Performance de API/Web.
4. Padrão de arquitetura existente.
5. UX operacional simples.
6. Provas antes de PASS.

## Arquitetura fixa

`SQL Server cliente -> Agent -> API ingest -> PostgreSQL STG -> Debezium/Redpanda -> CDC Consumer -> ClickHouse raw/current/slim/mart_rt -> API realtime -> Web`

Não pular camadas sem justificar.

## Backend

- Python/FastAPI.
- Autorização por role/telas deve ser na API.
- Não expor margem/lucro/custo para gerente/vendedor.
- Usar respostas claras 403/422/500.
- Não criar fallback pesado sem flag/log.
- Não retornar payload gigante sem necessidade.

## Frontend

- Next.js/React/TypeScript.
- Usar padrões visuais do TorqMind.
- Não criar HTML cru sem padrão.
- Não calcular regra analítica crítica no frontend.
- Frontend pode esconder menu, mas API precisa bloquear.
- Mobile precisa ser usável.

## PostgreSQL/ClickHouse

- STG é bruta e auditável. Não apagar.
- Migrations devem ser idempotentes e seguras.
- Nada destrutivo em produção sem backup e confirmação.
- ClickHouse realtime deve usar `torqmind_current` e `torqmind_mart_rt`.
- Não usar marts vazias como fonte real.

## Dados de venda

- Origem canônica: comprovantes + itens + formas de pagamento.
- Join por `id_empresa`, `id_filial`, `id_db`, `id_comprovante`.
- Data da venda vem do comprovante.
- `situacao=3` fora de tudo comercial.
- NFE `status=5` fora de venda/fraude/cancelamento, vai para inutilizações.
- Timezone: `America/Sao_Paulo`.

## Qualidade

```bash
python -m compileall apps/api apps/cdc_consumer
PATH="$PWD/.venv/bin:$PATH" pytest apps/api -q
PATH="$PWD/.venv/bin:$PATH" pytest apps/cdc_consumer/tests -q
cd apps/web && npm test && npm run build
find . -type d -name "__pycache__" -prune -exec rm -rf {} +
find . -type f -name "*.pyc" -delete
```

## Deploy

Produção: App/API/Web/Nginx `172.30.0.10`; PostgreSQL/STG/DW `172.30.0.8`; Analytics/ClickHouse/CDC `172.30.0.9`; URL `http://redevr.ddns.me:14023`.

Não fazer deploy sem health check.

## Relatório

Todo trabalho relevante precisa terminar com PASS/FAIL, arquivos alterados, causa raiz, testes, deploy, commit hash e pendências reais.
