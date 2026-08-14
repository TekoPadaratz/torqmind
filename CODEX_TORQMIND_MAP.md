# TorqMind — Mapa Mestre de Continuidade

Atualizado em: **2026-08-14 — America/Sao_Paulo**

Revisão de código usada: **branch `cursor/torqmind-hardening-2026-08-3837` sobre `master` @ `944f9cc`**

Estado do documento: **fonte canônica de contexto técnico e operacional**

Hardening 2026-08-14: rules/migrador/CI/exceções PG formalizadas. CDC recover de offset abaixo da retenção zerou TOTAL-LAG. Relatório: `docs/ops/HARDENING_2026-08-14.txt`.

Este arquivo substitui as descrições históricas acumuladas neste mapa. Ele registra o que existe no repositório atual, o que foi comprovado e o que ainda precisa de validação.

Não registrar aqui senhas, tokens, chaves de ingest, secrets JWT, credenciais de banco ou conteúdo de arquivos `.env`.

## 1. Resumo executivo

TorqMind é um Micro SaaS de BI para redes de postos. O produto reúne ingestão local do ERP Xpert, processamento transacional e analítico, dashboards operacionais, módulos financeiros, controle de acesso por empresa/filial e operação em produção e homologação.

Arquitetura principal atual:

```text
SQL Server Xpert
    ↓
TorqMind Agent Windows 2.0.5
    ↓ HTTPS/HTTP LAN autenticado por Ingest Key
FastAPI /ingest
    ↓
PostgreSQL STG
    ├─→ ETL / DW / marts PG de apoio, auditoria e compatibilidade
    └─→ Debezium → Redpanda → CDC Consumer
                              ↓
                    ClickHouse raw/current/slim
                              ↓
                    ClickHouse torqmind_mart_rt
                              ↓
                    FastAPI BI → Next.js Web
```

Direção arquitetural obrigatória: telas analíticas devem ler ClickHouse. PostgreSQL `stg`, `dw` e `mart` permanece como verdade transacional, camada de transformação, auditoria, publicação e rollback; não deve ser o hot path silencioso de dashboards.

Estado público observado em 2026-08-14:

- `https://www.torqmind.com.br/`: HTTP 200.
- `https://www.torqmind.com.br/api/health`: HTTP 200, cerca de 0,21 s.
- `https://hom.torqmind.com.br/`: HTTP 200.
- `https://hom.torqmind.com.br/api/health`: HTTP 200, cerca de 0,23 s.
- As versões `http://` dos dois ambientes ainda respondem 200, sem redirecionar para HTTPS.
- `https://www.hom.torqmind.com.br/` falha no handshake TLS. O hostname profundo não é coberto pelo Universal SSL padrão em uma zona Cloudflare completa.
- A conexão SSH externa em `redevr.ddns.me:14022` **passou a responder** no hardening 2026-08-14 (host App `torqmind-app-stream`). Saúde de CDC/lag/marts exige prova dedicada nesta branch, não inferência.

## 2. Autoridade dos documentos

Ordem de autoridade para futuras sessões:

1. `AGENTS.md`: segurança, dados canônicos, permissões, deploy e critérios de PASS.
2. `CODEX_TORQMIND_MAP.md`: estado técnico consolidado do projeto.
3. `docs/product/TORQMIND_DEVELOPMENT_CONTRACT.md`: contrato de produto e UI.
4. `.cursor/rules/*.mdc`: regras especializadas por assunto.
5. Runbooks e documentos de domínio em `docs/`.
6. READMEs históricos de fases: usar apenas como contexto, nunca como prova do estado atual.

O arquivo externo `TorqMind_Resumo_Mestre_Continuidade_2026-04-29_v2.txt` é histórico. Não usar suas versões, URLs, estado de deploy ou conclusões de PASS como atuais.

## 3. Ambientes e topologia

### Produção multi-VM

- PostgreSQL / STG / DW: `172.30.0.8`.
- Analytics / ClickHouse / Redpanda / Debezium / CDC Consumer: `172.30.0.9`.
- App / API / Web / Nginx: `172.30.0.10`.
- Checkout operacional esperado nos hosts: `/home/tm/torqmind`.
- SSH externo documentado: `ssh -p 14022 tm@redevr.ddns.me`.
- URL legada/NAT para diagnóstico: `http://redevr.ddns.me:14023`.
- URL pública canônica: `https://www.torqmind.com.br`.
- API pública canônica: `https://www.torqmind.com.br/api`.
- Fuso de negócio: `America/Sao_Paulo`.

### Homologação

- URL canônica: `https://hom.torqmind.com.br`.
- API: `https://hom.torqmind.com.br/api`.
- Nginx de homologação é publicado apenas no loopback da VM App, por padrão `127.0.0.1:81`.
- O hostname `www.hom.torqmind.com.br` não é canônico e não deve ser divulgado.

### Alerta de isolamento

Produção e homologação compartilham a VM analytics, o ClickHouse, o Redpanda, o Debezium e o CDC Consumer. Uma alteração de schema ClickHouse ou rebuild do consumer feita “para homologação” pode interromper produção.

Tratar como mudança de produção:

- `ALTER`, `DROP`, recriação ou evolução de `torqmind_current` e `torqmind_mart_rt`;
- alteração de colunas slim;
- mudança nos mappings do CDC;
- rebuild/recreate do `cdc-consumer`;
- alteração de tópicos, connector ou consumer group.

Após qualquer mudança desse tipo, provar ausência de erro de quantidade de colunas, membership do consumer, `TOTAL-LAG=0` e publicação recente em `mart_publication_log`.

## 4. Entrada pública, Nginx e Cloudflare Tunnel

O repositório possui Nginx HTTP interno:

- produção: `deploy/nginx/default.conf`, porta interna `80`, bind padrão `0.0.0.0:80` no compose;
- homologação: `deploy/nginx/homolog.conf`, porta interna `80`, bind padrão `127.0.0.1:81` no host.

Configuração esperada no Cloudflare Tunnel:

```text
www.torqmind.com.br  → HTTP → http://127.0.0.1:80
hom.torqmind.com.br  → HTTP → http://127.0.0.1:81
```

Se `cloudflared` estiver em outro container ou host, o endereço do serviço precisa ser alcançável por ele; `localhost` só aponta para a própria máquina/container do `cloudflared`.

É correto o Tunnel encaminhar uma requisição pública HTTPS para um serviço local HTTP. O visitante usa TLS até a borda Cloudflare; o `cloudflared` mantém um túnel de saída criptografado; o último salto local pode permanecer HTTP.

Pendência atual: ativar **Always Use HTTPS** em `SSL/TLS → Edge Certificates`, depois validar que todo `http://` retorna 301/308 para `https://`. A Cloudflare recomenda fazer esse redirecionamento na borda, não no origin Nginx, para evitar loops.

Certificados:

- Universal SSL cobre `torqmind.com.br` e subdomínios de primeiro nível, como `www.torqmind.com.br` e `hom.torqmind.com.br`.
- Universal SSL não cobre, em uma zona full setup comum, o hostname profundo `www.hom.torqmind.com.br`.
- Recomendação: remover `www.hom` e usar somente `hom.torqmind.com.br`.
- Se `www.hom` for obrigatório, emitir certificado Advanced/Total TLS ou customizado que inclua explicitamente esse hostname antes de criar redirect HTTPS.
- Não ampliar HSTS para toda a zona enquanto todos os hostnames publicados não tiverem HTTPS válido.

Referências oficiais:

- `https://developers.cloudflare.com/tunnel/routing/`
- `https://developers.cloudflare.com/ssl/edge-certificates/additional-options/always-use-https/`
- `https://developers.cloudflare.com/ssl/edge-certificates/universal-ssl/limitations/`
- `https://developers.cloudflare.com/ssl/edge-certificates/advanced-certificate-manager/`

## 5. Componentes do repositório

Inventário rastreado na revisão:

- 745 arquivos versionados.
- 133 arquivos Python em `apps/api`.
- 40 arquivos Python em `apps/agent`.
- 15 arquivos Python em `apps/cdc_consumer`.
- 87 arquivos TypeScript/TSX em `apps/web/app`.
- 174 arquivos SQL.
- 74 scripts shell de deploy/operação.
- 49 documentos Markdown em `docs`.
- 34 páginas `page.tsx` existentes no workspace.
- 136 decorators de endpoints FastAPI encontrados em `apps/api/app`.

### API

- Stack: Python, FastAPI, Pydantic, JWT, PostgreSQL e ClickHouse.
- Entrada principal: `apps/api/app/main.py`.
- Configuração: `apps/api/app/config.py`.
- Banco PostgreSQL: `apps/api/app/db.py`.
- Cliente ClickHouse: `apps/api/app/db_clickhouse.py`.
- Rotas BI: `apps/api/app/routes_bi.py` e módulos `routes_*`.
- Repositório PG legado: `apps/api/app/repos_mart.py`.
- Repositório ClickHouse: `apps/api/app/repos_mart_clickhouse.py`.
- Repositório realtime: `apps/api/app/repos_mart_realtime.py`.
- Facade de seleção: `apps/api/app/repos_analytics.py`.
- ACL central: `apps/api/app/permissions.py`.
- Escopo de tenant/filial: `apps/api/app/scope.py`.

### Web

- Stack: Next.js 14, React e TypeScript.
- Código principal: `apps/web/app`.
- Cliente e contratos de API: `apps/web/app/lib/api.ts`.
- Tema: `apps/web/app/lib/theme.tsx` e `apps/web/app/globals.css`.
- Ordenação canônica de grids: `apps/web/app/lib/grid-sort.ts`.
- Build standalone para Docker.

### Agent Windows

- Código: `apps/agent`.
- Versão canônica: `apps/agent/agent/__init__.py`.
- Versão no código e no canal de releases: `2.0.5`.
- Publicado em `/var/torqmind/agent-releases` em 2026-08-14T16:53:02Z.
- SHA-256: `261b930ef09f96ff4230f65df689434416a3011e012ff998a43311eb1ae3119c`.
- Tamanho: `11390800` bytes. URL: `https://www.torqmind.com.br/api/agent/update/download/2.0.5`.
- `mandatory=false`, `min_version=2.0.0`. Prova: manifest + download autenticado com mesmo SHA.
- 44 datasets configurados: 40 habilitados e 4 desabilitados por padrão.
- Desabilitados: `filiais`, `clientes`, `localvendas`, `financeiro`.
- `movprodutos` e `itensmovprodutos` permanecem habilitados para estoque.
- NFE usa `DATA`, nunca `DATAREPL`, como referência temporal.

Qualquer mudança em `apps/agent/**` exige bump de versão no mesmo commit e geração/publicação de um novo `.exe`. Nunca reutilizar versão publicada. Prova no posto: `torqmind-agent.exe --version`.

### CDC Consumer

- Código: `apps/cdc_consumer/torqmind_cdc_consumer`.
- Mappings: `apps/cdc_consumer/torqmind_cdc_consumer/mappings.py`.
- Foram encontrados 38 registros de mapping.
- Escrita ClickHouse: `clickhouse_writer.py`.
- Construção/publicação de marts: `realtime_mart.py`.
- A compatibilidade entre mappings e DDL slim precisa ser atômica; divergência de colunas paralisa o consumer compartilhado.

### SQL e migrações

- Migrações PostgreSQL: `sql/migrations`.
- DDL/streaming ClickHouse: `sql/clickhouse`.
- Reset local/homolog: `sql/torqmind_reset_db_v2.sql`.
- Migrador: `apps/api/app/migrate.py`.

## 6. Contratos canônicos de dados

### Venda

- Origem: `stg.comprovantes`, `stg.itenscomprovantes`, `stg.formas_pgto_comprovantes`.
- Join: `id_empresa`, `id_filial`, `id_db`, `id_comprovante`.
- `id_db` é obrigatório.
- Faturamento vem dos itens válidos, não dos pagamentos.
- Data da venda vem do comprovante.
- `situacao=3` é ignorada comercialmente.
- Não usar `stg.movprodutos` ou `stg.itensmovprodutos` como origem principal da venda realtime.

### Estoque

- `stg.movprodutos` e `stg.itensmovprodutos` continuam sendo ingeridos.
- Eles suportam a verdade de entrada/saída de estoque de loja e combustível.
- “Venda canônica” e “estoque canônico” são contratos distintos.

### Fiscal e documento

- NFE `status=5` é inutilização fiscal; não é venda, fraude nem cancelamento real.
- NFE usa `DATA`; não usar `DATAREPL` como filtro ou watermark.
- Documento operacional é o número NF-e/NFC-e via `stg.nfe`, `stg_nfe_slim` ou parse honesto de histórico.
- Sem nota: exibir `—`.
- Proibido usar comprovante técnico, `NROCOMPROVANTE`, `id_comprovante`, turno/filial ou `MOVPRODUTOS` como documento.

### Caixa e turno

- Caixa/turno `0` não entra em rankings operacionais.
- O turno exibido é `stg_turnos.payload.TURNO`.
- `id_turno`/`ID_TURNOS` é identificador técnico, não número operacional.
- Sem resolução real: `Turno não resolvido`.

### Contas a receber

- `DATAREPL` não representa pagamento/baixa direta.
- A revisita do Agent precisa reler títulos abertos e recém-pagos.
- `ID_CONTASRECEBER` é único por `ID_DB`, não globalmente.
- Reconciliação precisa curar títulos renumerados, deletados ou pagos antigos; UPSERT simples não basta.
- O refresh de inadimplência exige lock transacional por empresa para evitar corrida entre agendadores.

### Tempo

- Timezone de negócio: `America/Sao_Paulo`.
- Não criar fallback `1970`, `data_key=0` ou horário de meio-dia inventado.
- Timestamps técnicos devem ser ISO 8601 com offset explícito.

## 7. ClickHouse, realtime e leitura BI

Fluxo canônico:

```text
PG STG/DW → Debezium/CDC/ETL → ClickHouse → API → Web
```

Bancos principais:

- `torqmind_raw`: eventos brutos/CDC.
- `torqmind_current`: estado atual e tabelas slim.
- `torqmind_mart_rt`: marts realtime servidas à API.
- `torqmind_ops`: telemetria/operação.
- `torqmind_dw` e `torqmind_mart`: trilho batch/histórico ainda presente.

Flags relevantes:

- `USE_CLICKHOUSE=true`: habilita backend analítico ClickHouse quando há implementação.
- `DUAL_READ_MODE=true`: compara PG e ClickHouse quando suportado.
- `USE_REALTIME_MARTS=true`: ativa `repos_mart_realtime` para contratos registrados. **`false` não ativa realtime.**
- `REALTIME_MARTS_SOURCE=stg`: origem desejada do MartBuilder.
- `REALTIME_MARTS_FALLBACK=false`: requisito do cutover final para não esconder falhas.
- `ENABLE_MART_BUILDER=true`: permite publicação de marts a partir do consumer.

Risco ainda aberto: em `repos_analytics.py`, quando realtime está habilitado mas a função não possui contrato realtime, a facade volta para PostgreSQL. Existem também leituras analíticas diretas de `stg`, `dw` e `mart` PG, por exemplo créditos/fraude, despesas do DRE e fallbacks de cheques/solvência. Isso viola a regra de leitura analítica única no ClickHouse e precisa ser eliminado por domínio, com mart e teste de paridade antes de remover cada fallback.

## 8. Controle de acesso

- `platform_master`: todas as empresas/filiais, menu Plataforma e dados de margem/lucro/custo.
- `owner`: somente empresa/filiais vinculadas; sem Plataforma; vê margem/lucro/custo.
- `manager`: empresa/filiais definidas; menu e abas por checkbox; nunca vê margem/lucro/custo.
- `tenant_kiosk`: modo TV, apenas dashboards autorizados e logout; nunca vê margem/lucro/custo.

Nova tela ou aba exige:

1. entrada em `SCREEN_REGISTRY`;
2. `require_screen` na API;
3. filtro correspondente no frontend;
4. teste de escopo/role.

Esconder componente no frontend não é segurança. Toda permissão e todo escopo precisam ser aplicados na API.

## 9. Produto, tema, branding e grids

- Tema claro/escuro/sistema em `localStorage`, chave `torqmind.theme`; padrão escuro.
- Superfícies usam tokens de tema; cores cobre/dourado e semânticas permanecem.
- Branding é por empresa em `app.company_branding`.
- Uploads persistem no volume `torqmind_branding`, em `/app/var/branding`.
- Upload deve validar magic-number, rejeitar SVG/executável renomeado, limitar tamanho e gerar nome no servidor.
- Não trocar favicon/ícone principal sem decisão explícita.

Contrato de grids:

- Colunas: Filial → Data → Documento.
- Ordenação: Filial ASC → Data DESC → Nome ASC.
- Busca geral com `GridSearchInput` + `useGridSearch`, 280 px, alinhada à esquerda e cobrindo todos os campos.
- Rankings por métrica são exceção explícita.
- UI não exibe SQL, nome de mart, fórmula interna, fallback ou nota de engenharia.

## 10. Deploy e operação

Composes oficiais:

- App/API/Web/Nginx: `docker-compose.app.yml`.
- PostgreSQL: `docker-compose.pg.yml`.
- Analytics/realtime: `docker-compose.analytics.yml`.
- Homolog: `docker-compose.homolog.yml`.

Arquivos de ambiente esperados nos servidores:

- `/etc/torqmind/prod.app.env`
- `/etc/torqmind/prod.pg.env`
- `/etc/torqmind/prod.analytics.env`
- `/etc/torqmind/cluster.env`

Nunca:

- apagar STG;
- resetar volumes;
- regenerar Ingest Key sem plano explícito;
- executar `docker compose down -v`;
- executar DROP/TRUNCATE em produção sem plano, backup e confirmação;
- fazer deploy sem testes e health checks;
- declarar PASS sem prova.

Após recreate da API, validar que `/api/ingest/health` continua auth-only e rápido. O modo padrão não pode executar `COUNT` pesado em STG.

## 11. Qualidade observada nesta revisão

Hardening 2026-08-14 (`cursor/torqmind-hardening-2026-08-3837`):

| Gate | Evidência | Resultado |
|------|-----------|-----------|
| compileall API/CDC | worktree remoto | **confirmado em teste** |
| API static (migrador + registry PG) | unittest local + job `api-static` GHA | **confirmado em teste** |
| CDC Consumer pytest | offset recovery + suite no worktree | **confirmado em teste** |
| Agent unittest | 41 passed no worktree | **confirmado em teste** |
| Web tests + build | 121 testes; Next 14.2.35 standalone | **confirmado em teste** |
| npm audit runtime | PostCSS 8.5.26 (override); 1 high residual Next 14.2.35 | **PASS COM RISCO FORMAL** |
| CI validate | GHA `services.postgres:16` + migrate + pytest unit + smoke login | **PASS (GHA)** |
| Homolog API/Web | jose 3.5.0 / Next 14.2.35; health 200 + HTTPS 301→200 | **confirmado em homologação** |
| Prod API/Web | recreate `--no-deps`; jose 3.5.0; smoke PASS + HTTPS | **confirmado em produção** |
| `prod-multivm-validate.sh` | 19/19 OK (closeout 2026-08-14 16:57Z) | **confirmado em produção** |
| `prod-multivm-proof.sh` | result=PASS (`tmp/prod-multivm-proof-20260814_165930.json`) | **confirmado em produção** |
| Debezium `.9` | `torqmind-postgres-cdc` RUNNING, task 0 RUNNING | **confirmado em produção** |
| CDC member | `torqmind-cdc-consumer-live` Stable, 1 membro | **confirmado em produção** |
| rpk TOTAL-LAG | 295 530 851 → 0 (commit log-start); reconfirmado 0 no closeout | **confirmado em produção** |
| `NUMBER_OF_COLUMNS` | 0 (6h de logs) | **confirmado em produção** |
| Performance BI | dash hot p50 1.45s p95 1.53s; frio 4.78s; fallback=realtime | **confirmado em produção** |
| Reset/migrations | nenhuma aplicada em prod/homolog | **confirmado em produção** |
| TorqMind-Ops | StartedAt closeout idêntico ao baseline | **confirmado em produção** |
| Agent `.exe` 2.0.5 | publicado; SHA-256 comprovado Drive=disco=API | **PASS** |
| Checkout operacional | `/home/tm/torqmind` @ `7d87415` (ff-only; sem rebuild) | **PASS** |
| Isolamento analytics Hom | ADR; não executado | **planejado (próxima fase)** |

### 11.1 Lag e freshness

TOTAL-LAG era fantasma: LOG-START-OFFSET = HIGH-WATERMARK e o grupo ainda reportava CURRENT-OFFSET 0 (broker committed -1001). Vendas sempre em LAG 0. O consumer 848a309 comita log-start nessas partições vazias. TOTAL-LAG=0.

`fact_caixa_turno` só muda no fechamento de turno. STG turnos e DW têm o mesmo `max(source_ts)` (2026-08-14 13:15Z). Validator usa SLA de 8h só nesse domínio; demais críticos permanecem em 3600s.

Gate obrigatório de código:

```bash
python -m compileall apps/api apps/cdc_consumer
PATH="$PWD/.venv/bin:$PATH" pytest apps/api -q
PATH="$PWD/.venv/bin:$PATH" pytest apps/cdc_consumer/tests -q
cd apps/web && npm test && npm run build
```

Gate de produção:

```bash
curl -I https://www.torqmind.com.br
curl -sS https://www.torqmind.com.br/api/health
ENV_FILE=/etc/torqmind/prod.app.env PUBLIC_URL=https://www.torqmind.com.br ./deploy/scripts/realtime-product-screen-smoke.sh
ENV_FILE=/etc/torqmind/prod.app.env CLUSTER_ENV=/etc/torqmind/cluster.env ./deploy/scripts/prod-multivm-validate.sh
ENV_FILE=/etc/torqmind/prod.app.env CLUSTER_ENV=/etc/torqmind/cluster.env ./deploy/scripts/prod-multivm-proof.sh
```

## 12. Riscos e dívida técnica priorizados

### P0 — corrigir primeiro

1. **HTTPS na borda:** Cloudflare corrigido em 2026-08-14 (HTTP 301→HTTPS; www/hom HTTPS 200). Revisar HSTS/CSP e `www.hom` residual se ainda aparecer.
2. **Blast radius homolog/prod:** analytics e CDC compartilhados tornam DDL/rebuild de homolog uma mudança de produção. ADR: `docs/adr/2026-08-14-homolog-analytics-isolation.md` (Opção B preferencial; **não executar** sem aprovação).
3. **Freshness de `fact_caixa_turno`:** só atualiza no fechamento de turno; SLA do validator é 8h. Se STG turnos avançar e DW não, é stall.
4. **Dívidas analíticas PG formalizadas** em `apps/api/app/analytics_pg_exceptions.json` (prazo 2026-09-15): fraude créditos, cheques, budget, solvência. CI impede nova exceção não registrada.

### P1 — tratar na sequência

5. **Dependências Web:** 1 high residual Next 14.2.35; PostCSS 8.5.26 ok. Next 15/16 é breaking (prazo 2026-09-15).
6. **Migrações com drift:** existem 140 arquivos, prefixos duplicados `012`, `013` e `135`; o reset referencia dois arquivos inexistentes e não acompanha toda a cadeia atual.
7. **Encoding/checksum:** há migrações legadas fora de UTF-8 e estratégia de checksum que pode divergir entre baseline e aplicação gerenciada.
8. **Módulos excessivamente grandes:** `repos_mart.py`, `repos_mart_realtime.py` e rotas centrais concentram muita lógica e aumentam risco de regressão.
9. **Proxy e rate limit:** confirmar que a API usa IP real confiável do Cloudflare/Nginx; não confiar cegamente em cabeçalho forjado.
10. **Backup/restore e observabilidade:** política, retenção, teste de restore, alertas de lag/freshness e RPO/RTO precisam de prova operacional recente.

## 13. Plano recomendado

Ordem de execução:

1. Corrigir HTTPS/hostnames no Cloudflare e validar redirect, certificado e headers.
2. Fechar as falhas dos testes e transformar os gates em comandos reproduzíveis.
3. Provar o estado interno dos três servidores: containers, connector, consumer group, lag, freshness e `mart_publication_log`.
4. Isolar a mudança de schema analytics com rollout compatível e checklist obrigatório.
5. Migrar os hot paths PG restantes para marts ClickHouse, domínio por domínio.
6. Normalizar migrações/reset/checksums sem reescrever histórico já aplicado em produção.
7. Atualizar dependências em lotes pequenos com teste e build.
8. Modularizar os arquivos gigantes somente depois de cobertura de contrato suficiente.
9. Formalizar backup, restore, alertas e evidência de RPO/RTO.

## 14. Arquivos de referência rápida

- `AGENTS.md`: regras absolutas.
- `README.md`: entrada do repositório.
- `apps/agent/README.md`: instalação e operação do Agent.
- `docs/agent_runbook.md`: diagnóstico e publicação do Agent.
- `docs/product/TORQMIND_DEVELOPMENT_CONTRACT.md`: contrato do produto.
- `docs/product/TORQMIND_UI_CANONICAL_PATTERNS.md`: padrões visuais e componentes.
- `docs/data/TORQMIND_SEMANTIC_FIELD_MAP.md`: semântica de dados.
- `docs/data/NFE_FISCAL_CLASSIFICATION.md`: classificação fiscal.
- `deploy/scripts/prod-multivm-validate.sh`: validação multi-VM.
- `deploy/scripts/prod-multivm-proof.sh`: pacote de prova.
- `deploy/scripts/realtime-product-screen-smoke.sh`: smoke de produto realtime.
- `deploy/scripts/cloudflare-tunnel-check.sh`: smoke público do Tunnel.

## 15. Checklist de continuidade

Ao retomar o projeto:

1. Ler `AGENTS.md` e este mapa.
2. Conferir branch, commit e alterações locais; preservar arquivos do usuário.
3. Não assumir que o checkout da VM analytics está na mesma revisão do App.
4. Verificar URLs públicas e `/api/health`.
5. Se o assunto for dados, provar fonte → STG → DW/publicação → ClickHouse → API → tela.
6. Se o assunto for realtime, conferir connector, membership, `TOTAL-LAG`, logs e `mart_publication_log`.
7. Se alterar Agent, incrementar versão e publicar novo executável.
8. Se alterar schema ClickHouse/CDC, tratar como produção mesmo quando a intenção for homologação.
9. Executar os testes proporcionais ao risco e registrar prova objetiva.
10. Atualizar este mapa quando arquitetura, URLs, versões, riscos ou contratos mudarem.

## 16. Registro desta atualização

Atualização de 2026-08-14 (hardening):

- rules, migrador/MANIFEST, registry de exceções PG e jobs CI (API/CDC/Agent/Web);
- Next 14.2.35, Axios 1.19.0, python-jose 3.5.0;
- deploy específico Hom depois Prod (`--no-deps`), TorqMind-Ops intocado;
- prova interna: Debezium RUNNING, publicação `sales_daily_rt` no dia, smoke produto PASS;
- validate oficial FAIL em lag/freshness pré-existentes;
- Agent `.exe` 2.0.5 não publicado;
- nenhum reset, nenhuma migration aplicada, nenhum `docker compose down`.

## Financeiro / Despesas (Razão)

- Fonte canônica Xpert: `dbo.MOVLCTOS` + `DTACONTA` (não `CONTASPAGAR`/`DTAVCTO`).
- Semântica Entradas/Saídas = TIPO débito(0/2)/crédito(1); Saída ≠ baixa.
- Pipeline: STG `movlctos` → publish `mart_finance_despesas_rt` → `finance_despesas_overview`.
- Texto da linha = `DOCUMENTO` do MOVLCTOS.
- Prova ouro VR01 jul/2026 conta `3.2.02.23` = R$ 3.688,64 (101 lançamentos).

