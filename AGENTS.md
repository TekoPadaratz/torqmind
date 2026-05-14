# TorqMind — Instruções Operacionais para Agents

Estas instruções valem para qualquer agent trabalhando neste repositório.

## Produto

TorqMind é um Micro SaaS BI premium para redes de postos de combustíveis.

O produto precisa ser confiável em dados, rápido em produção, seguro por role/permissão, simples para usuário operacional, auditável ponta a ponta e vendável para dono de posto.

## Stack oficial

- Frontend: Next.js / React / TypeScript
- API: Python / FastAPI
- PostgreSQL: `app`, `auth`, `stg`, `dw`, `mart`
- ClickHouse: `torqmind_raw`, `torqmind_current`, `torqmind_mart_rt`, `torqmind_ops`
- Streaming/CDC: Debezium + Redpanda + CDC Consumer Python
- Deploy: Docker Compose multi-VM
- Produção:
  - PostgreSQL/STG/DW: `172.30.0.8`
  - Analytics/ClickHouse/Redpanda/Debezium/CDC: `172.30.0.9`
  - App/API/Web/Nginx: `172.30.0.10`
  - SSH externo: `ssh -p 14022 tm@redevr.ddns.me`
  - URL pública: `http://redevr.ddns.me:14023`
  - API pública: `http://redevr.ddns.me:14023/api`

## Regras absolutas de segurança

Nunca:
- apagar STG;
- resetar volumes;
- regenerar Ingest_Key;
- expor segredos em logs/commit;
- executar `docker compose down -v`;
- rodar DROP/TRUNCATE em produção sem plano, backup e confirmação explícita;
- fazer deploy sem teste e health check;
- declarar PASS sem prova.

## Regras canônicas de dados

- Vendas canônicas vêm de `stg.comprovantes`, `stg.itenscomprovantes`, `stg.formas_pgto_comprovantes`.
- Não usar `stg.movprodutos` / `stg.itensmovprodutos` como origem principal de venda realtime.
- Join comprovante/item: `id_empresa`, `id_filial`, `id_db`, `id_comprovante`.
- `id_db` é obrigatório.
- Faturamento vem dos itens válidos, não dos pagamentos.
- Data da venda vem do comprovante.
- Timezone: `America/Sao_Paulo`.
- Proibido fallback com `1970`, `data_key=0`, meio-dia inventado.
- `situacao=3` é ignorada comercialmente.
- NFE `status=5` é inutilização fiscal, não venda, não fraude, não cancelamento real.
- NFE usa `DATA`; nunca usar `DATAREPL` como watermark/filtro.
- Caixa/turno `0` não entra em rankings operacionais.

## Controle de acesso

- `platform_master`: acesso total, todas empresas/filiais, vê Plataforma, vê margem/lucro/custo.
- `owner`: empresa/filiais vinculadas, não vê Plataforma, vê margem/lucro/custo.
- `manager`/gerente: empresa/filiais definidas, menus por checkbox, nunca vê margem/lucro/custo.
- `tenant_kiosk`/vendedor/TV: modo TV, sem menu normal, apenas dashboards permitidos, só logout, sem margem/lucro/custo.

Permissão real precisa ser aplicada na API. Esconder menu no frontend não é suficiente.

## Regras de domínio para postos

Sempre considerar combustíveis, preço concorrente, vendas por hora, ranking de vendedores, turno/caixa, operador/frentista, cancelamentos, NFE/NFC-e, formas de pagamento, contas a pagar/receber, metas/equipe e financeiro gerencial.

Nunca expor margem, lucro, CMV, custo ou rentabilidade para gerente/vendedor.

## Qualidade obrigatória antes de PASS

```bash
python -m compileall apps/api apps/cdc_consumer
PATH="$PWD/.venv/bin:$PATH" pytest apps/api -q
PATH="$PWD/.venv/bin:$PATH" pytest apps/cdc_consumer/tests -q
cd apps/web && npm test && npm run build
```

Se mexer em produção:

```bash
curl -I http://redevr.ddns.me:14023
curl -I http://redevr.ddns.me:14023/api/health
```

Se mexer em realtime/marts:

```bash
ENV_FILE=/etc/torqmind/prod.app.env PUBLIC_URL=http://redevr.ddns.me:14023 ./deploy/scripts/realtime-product-screen-smoke.sh
ENV_FILE=/etc/torqmind/prod.app.env CLUSTER_ENV=/etc/torqmind/cluster.env ./deploy/scripts/prod-multivm-validate.sh
ENV_FILE=/etc/torqmind/prod.app.env CLUSTER_ENV=/etc/torqmind/cluster.env ./deploy/scripts/prod-multivm-proof.sh
```

## Performance obrigatória

Antes de otimizar, medir. Depois de otimizar, provar antes/depois.

```bash
curl -sS -w '
TOTAL=%{time_total}s
' -o /tmp/endpoint.json 'http://redevr.ddns.me:14023/api/health'
```

Para endpoints BI: evitar consulta pesada em STG quando Mart/snapshot existe; evitar fallback pesado silencioso; reduzir payload; cache por escopo quando seguro; preferir marts/materializações para telas críticas; alvo: endpoint quente abaixo de 2s sempre que possível.

## Estilo de trabalho

Sempre diagnosticar, explicar causa provável, alterar o mínimo necessário, testar, validar API/Web, limpar sujeira, commit/push quando solicitado e entregar relatório PASS/FAIL com prova.
