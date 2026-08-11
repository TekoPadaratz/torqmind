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

## Agent / ingest (rede local e recreate da API)

### Versão do Agent (obrigatório)

- Fonte: `apps/agent/agent/__init__.py` → `__version__`.
- **Qualquer alteração em `apps/agent/**` (dataset, query, watermark, sink, runtime, build) exige incrementar a versão no mesmo commit** e gerar/publicar novo `.exe`.
- Não reutilizar número de versão já publicado. Prova no posto: `torqmind-agent.exe --version`.
- Detalhes: `.cursor/rules/09-agent-version.mdc`, `docs/agent_runbook.md` §7, contrato de desenvolvimento (seção Agent).

### Rede e health

- No posto na mesma LAN TorqMind: `api.base_url=http://172.30.0.10` (nginx `:80`). Não usar `172.30.0.10` de fora da LAN; `:14023` público só via NAT do roteador.
- `GET /ingest/health` (com `X-Ingest-Key`) é **auth-only** e tem que responder em milissegundos. `?stats=true` é o scan pesado (COUNT em STG) só para ops — nunca default do agent.
- Após `docker compose ... up --force-recreate` / rebuild da API **sem** essa versão do código, o endpoint antigo volta e o agent toma **HTTP 504** no `config test`. Prova pós-recreate:

```bash
curl -sS -m 5 -H "X-Ingest-Key: $INGEST_KEY" http://172.30.0.10/api/ingest/health
# esperado: {"ok":true,"id_empresa":...,"mode":"auth","datasets":[]}
```

- Homolog e prod: mesma regra (containers `torqmind-api` e `torqmind-api-homolog`).

## Regras canônicas de dados

- Vendas canônicas vêm de `stg.comprovantes`, `stg.itenscomprovantes`, `stg.formas_pgto_comprovantes`.
- Não usar `stg.movprodutos` / `stg.itensmovprodutos` como origem principal de venda realtime.
- `stg.movprodutos` / `stg.itensmovprodutos` **DEVEM** continuar sendo ingeridos (agent `enabled=true`): são a base de entrada/saída de estoque (loja + combustível). Venda canônica ≠ estoque canônico.
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
- Turno operacional exibido é `stg_turnos.payload.TURNO` (1..N; `0` = caixa geral). Nunca exibir `id_turno`/`ID_TURNOS` técnico (ex.: `34292`) como número de turno; ele serve só para join/rastreabilidade. Sem número operacional resolvido, usar fallback honesto (`Turno não resolvido`).
- Documento operacional da venda (**regra absoluta**): **DOCUMENTO = número da NF-e/NFC-e** via `stg.nfe` / `stg_nfe_slim` (e/ou parse honesto do HISTORICO com NFC-e/NF-e). Sem NF → `—`. **Proibido** usar `NROCOMPROVANTE`, `id_comprovante`, `Turno + Filial`, prefixo "Cupom"/"Comprovante", ou `MOVPRODUTOS` como documento. Ver `.cursor/rules/07-documento-nota-fiscal.mdc`.
- Grids BI (contrato mestre): colunas **Filial → Data → Documento**; ordenação de linhas **Filial ASC → Data DESC → Nome ASC** (campos ausentes ignorados; sem os três → 1º campo de negócio ASC). Rankings por métrica são exceção. Ver `.cursor/rules/08-grids-colunas-ordenacao.mdc` e helper FE `apps/web/app/lib/grid-sort.ts`.
- Em telas de risco/fraude, dado sem responsável/turno/documento deve ser investigado na fonte/mart antes de criar fallback visual; grid vazio em área nobre vira empty state compacto.
- Contas a receber: `DATAREPL` NÃO reflete pagamento/baixa direta de `CONTASRECEBER` (DTAPGTO/VLRPAGO mudam sem mexer em DATAREPL). A janela de revisita do agent deve reler títulos abertos E recém-pagos (últimos ~120d por DTAPGTO). Nunca declarar PASS em inadimplência/contas a receber sem validar o cliente/título no Xpert (fonte→tela). Bug de inadimplência não se corrige no frontend — corrige a sincronização STG→DW→mart.
- `ID_CONTASRECEBER` é único por `ID_DB`, NÃO global. Reconciliação não pode ser só UPSERT: títulos deletados/renumerados/pagos-antigos no Xpert precisam ser fechados (re-upsert do pago ou tombstone), senão viram fantasma aberto na mart de inadimplência.
- `etl.refresh_customer_delinquency_summary` faz DELETE+INSERT e roda por 2 agendadores (orquestrador `*/2` + cron de reconciliação); precisa de `pg_advisory_xact_lock` por empresa, senão a corrida aborta o refresh e deixa a mart stale. Reconciliação que cura STG/DW deve garantir o DW (sync direto a prova de watermark), pois o watermark `financeiro` compartilhado é avançado pelo orquestrador e pode pular títulos recém-curados.

## Arquitetura de dados (leitura BI)

```
PG STG/DW → Debezium/CDC/ETL → ClickHouse (torqmind_mart_rt / torqmind_mart)
                                      ↓
                               API / Front (única fonte de leitura analítica)
```

Nunca servir dashboard a partir de `stg.*` / `dw.fact_*` / `mart.*` PostgreSQL.
PG `mart.*` é mash/staging para publish no CH. Ver `.cursor/rules/06-clickhouse-bi-reads.mdc`.

### ⛔ Homolog e produção compartilham o analytics (perigoso)

Em produção multi-VM, **Homolog e Prod NÃO são isolados no ClickHouse / Redpanda / Debezium / cdc-consumer** (`172.30.0.9`). O PG de app Homolog pode ser espelho, mas o **pipeline realtime CH é único**.

Consequência (incidente 31/07–03/08/2026): `ALTER` / evolução de slim (`id_funcionario` em `stg_itenscomprovantes_slim`) feita no fluxo Homolog quebrou o `cdc-consumer` de Prod (`NUMBER_OF_COLUMNS_DOESNT_MATCH` 15 vs 16) e congelou `sales_daily_rt` / dashboard.

Regras:
- Tratar DDL/schema de `torqmind_current` / `torqmind_mart_rt` e rebuild do `cdc-consumer` como **mudança de produção**.
- Nunca aplicar `ALTER` em slim/mart CH “só em Homolog” sem deploy alinhado do consumer na analytics VM.
- Repo em `172.30.0.9:/home/tm/torqmind` pode estar em branch antiga — rebuild do consumer deve usar o código canônico (não assume que Homolog = isolado).
- Depois de mudança de schema CH: provar `mart_publication_log` de `sales_daily_rt` + ausência de `NUMBER_OF_COLUMNS` nos logs do consumer.

## Controle de acesso

- `platform_master`: acesso total, todas empresas/filiais, vê Plataforma, vê margem/lucro/custo.
- `owner`: empresa/filiais vinculadas, não vê Plataforma, vê margem/lucro/custo.
- `manager`/gerente: empresa/filiais definidas, menus **e painéis/abas** por checkbox, nunca vê margem/lucro/custo.
- `tenant_kiosk`/vendedor/TV: modo TV, sem menu normal, apenas dashboards permitidos, só logout, sem margem/lucro/custo.

ACL: `apps/api/app/permissions.py` (`SCREEN_REGISTRY`). Menu = nav; painel = `menu.aba` (ex.: `profit_management.overview`). Nova aba ⇒ registry + `require_screen` + filtro FE. Cadastro: árvore em `/platform/users` (`GET /platform/screen-registry`).

## Identidade visual e UX de escopo

- Tema de superfície (claro/escuro/sistema): preferência do usuário em `localStorage` (`torqmind.theme`); padrão **escuro**. Superfícies/chrome (nav, sidebar, cards, banners, inputs) usam tokens (`--chrome-*`, `--surface-*`, `--hero-*`, etc.) e acompanham o tema; cobre/dourado/semânticos permanecem. Ver `apps/web/app/lib/theme.tsx` e `globals.css` (`html[data-theme]`).
- Personalização visual é por empresa (`app.company_branding`, chave `id_empresa`), com fallback para o padrão TorqMind; trocar imagem não exige novo deploy.
- Uploads ficam em storage persistente (volume `torqmind_branding` em `/app/var/branding`), nunca em pasta apagada no deploy.
- Validar imagem por magic-number (não só extensão/MIME declarado); rejeitar SVG e executável renomeado; limitar tamanho; nome de arquivo gerado pelo servidor (sem path traversal).
- Não trocar favicon/ícone principal do TorqMind sem decisão explícita.
- Esconder seletor de empresa/filial no frontend é UX; a API continua bloqueando escopo/permissão (frontend nunca é a fronteira de segurança).

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

## Grids (contrato UI)

Todo grid novo ou alterado deve seguir `.cursor/rules/08-grids-colunas-ordenacao.mdc`:
colunas Filial→Data→Documento, ordenação canônica, **busca geral** via
`GridSearchInput` + `useGridSearch` (largura fixa 280px; **sempre alinhada à esquerda**;
termo varre todos os campos da linha), e labels limpos (sem disclaimer/debug/mart/SQL na UI).

**Copy de produto:** nunca expor fórmula/pipeline/nota de engenharia na tela
(“custo = …”, “não entra no rateio…”, “publicado da mart…”). Isso é para o
time (contrato UI + docstring); o cliente vê só título, KPIs e dados.
Detalhe: `docs/product/TORQMIND_DEVELOPMENT_CONTRACT.md` §10.

## Estilo de trabalho

Sempre diagnosticar, explicar causa provável, alterar o mínimo necessário, testar, validar API/Web, limpar sujeira, commit/push quando solicitado e entregar relatório PASS/FAIL com prova.
