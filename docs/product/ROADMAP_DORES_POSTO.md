# TorqMind — Dores a Resolver (Roadmap de Produto para Postos)

> Documento de planejamento e mapeamento. Não é changelog. Registra dores reais
> de dono de posto que o TorqMind ainda não resolve, o que já existe na base de
> dados/arquitetura, o gap técnico de cada uma e o plano de rastreio antes de
> implementar. Atualizar conforme o cliente validar prioridades.
>
> Data inicial: 2026-06-03. Cliente de referência: rede Xpert (SQL Server origem).
> Arquitetura fixa: `SQL Server Xpert -> Agent -> API ingest -> PostgreSQL STG ->
> Debezium/Redpanda -> CDC Consumer -> ClickHouse current/mart_rt -> API -> Web`.
>
> **Atualização 2026-06-03 (rodada de mapeamento):** conectamos direto na base
> Xpert (`ATXDADOS`, SQL Server 2017, via `tools/xpert_source_explorer.py`) e
> mapeamos as tabelas reais de estoque/tanque/bico/aferição/cartão. Os achados
> confirmados estão registrados em cada seção abaixo (§1, §2, §6). O status do
> Telegram (§4) foi corrigido após auditoria do código real: a infraestrutura é
> bem mais completa do que estava documentado — ver §4. Novas dores acrescentadas:
> §8 (revenda de vale-combustível por funcionário) e §9 (validação de
> assinatura/cheque por IA — só documentação).

---

## Sumário das decisões (o que entra e o que fica de fora)

| # | Tema | Decisão do dono | Onde encaixa |
|---|------|-----------------|--------------|
| 1 | Controle de estoque de combustível por bomba/tanque | **ENTRA — prioridade máxima.** Quer logo no dashboard geral. | Nova trilha de dados (encerrante/tanque) + KPI no dashboard |
| 2 | Conciliação de cartão/TEF (bandeiras + taxas) | **ENTRA.** Tratar no Financeiro, após rastreio de cadastro de bandeiras/taxas. | `/finance` + novo mart de conciliação |
| 3 | Contas a pagar | **ENTRA (a levantar).** Sempre quebrado no cliente; detalhar requisitos com ele depois. | `/finance` |
| 4 | Alerta de caixa aberto >24h no celular (Telegram) | **FEITO (2026-06-03, migration 090).** Infra Telegram já existia e disparo já estava ligado; faltava refrescar a mart PG no fast-path operacional. Corrigido e validado em prod (2 alertas reais). Ver §4. | Notificação externa (Telegram) |
| 5 | Benchmark entre filiais | **ENTRA como enriquecimento visual.** Ciência da injustiça estrada × cidade. | Tela de Metas & Equipe |
| 6 | Aferição/INMETRO de bombas | **ENTRA — confirmado.** Tabela `AFERICAO` existe na Xpert, viva e atual. Ver §6. | Ingestão Xpert + Telegram |
| 7 | Jarvis IA (briefing no dashboard) | **REAVALIAR.** Dono não vê mais sentido hoje. Ver §7 para recomendação. | Decisão: repaginar ou remover |
| 8 | Revenda de vale-combustível por funcionário | **ENTRA (a levantar).** Dor real de fraude: funcionário revende o vale dele. Verificar se há controle no sistema. Ver §8. | Antifraude / RH operacional |
| 9 | Validação de assinatura/cheque por IA | **FUTURO — só documentação.** Complexo; sem foto de documento assinado mapeada hoje. Ver §9. | Futuro / pesquisa |
| — | Loja de conveniência em tela separada | **FORA.** O que é separado já está separado internamente nas telas. | — |
| — | Programa de fidelidade | **FORA.** Já oferecido ao cliente, não foi aceito. | — |

---

## 1. Controle de estoque de combustível por bomba/tanque (PRIORIDADE)

### A dor real
O dono pensa em **litros**, mas o TorqMind hoje só raciocina em **R$**. Ele não
tem um "estoque absoluto" confiável por bomba/tanque. Hoje:
- O cliente **controla as notas de entrada** (compra da distribuidora), mas isso
  não vira saldo de tanque dentro do TorqMind.
- Existe um **sensor/sonda** que faz **mapeamento diário** do quanto há no tanque,
  mas **não se sabe se é confiável nem onde esse dado é gravado** (provável tabela
  no SQL Server da Xpert; precisa ser localizada).
- Não há cruzamento entre **entrada (nota) − saída (venda em litros) = saldo
  esperado** versus **medição física (sensor)**, que é o que revela perda,
  sobra, evaporação ou furto.

Sem isso, o dono perde milhares de reais por mês no tanque sem enxergar.

### O que JÁ existe na base (não partimos do zero)
- **Encerrante de fechamento já é coletado.** O Agent ingere o campo
  `ENCERRANTEFECHAMENTO` da tabela `dbo.TURNOS` (ver
  `apps/agent/agent/config.py`, bloco `turnos.preflight_tables`). Hoje esse valor
  é usado **apenas** para detectar "caixa aberto" no `mart_builder` (CDC Consumer),
  **não** para volumetria. O encerrante é a fotografia mecânica/eletrônica do bico
  — base natural para litros vendidos por bico/turno.
- **Vendas em litros existem implicitamente** nos itens de combustível
  (`stg.itenscomprovantes`): quantidade × produto combustível. Hoje agregamos
  faturamento e margem, não volume.
- **Dimensão de produtos** distingue combustível (já usada na curva ABC com
  exclusão de combustível e em `competitor_fuel_product_ids`).

### O que FALTA (gap técnico)
1. **Cadastro físico**: mapa de **tanques** e **bicos/bombas** por filial e a
   ligação bico → produto → tanque. Hoje não é ingerido. Precisa ser localizado
   no SQL da Xpert (ver §Rastreio).
2. **Entradas de combustível**: a nota de compra da distribuidora (volume
   recebido por tanque/data). O cliente controla isso na Xpert — localizar a
   tabela e ingerir como nova entidade STG.
3. **Leitura do sensor/sonda**: onde a Xpert grava a medição diária do tanque.
   Localizar, avaliar confiabilidade (comparar série do sensor com saldo
   calculado em alguns dias) e ingerir.
4. **Volumetria de venda**: agregar litros vendidos por produto/tanque/bico/dia a
   partir dos itens de combustível (e/ou pela variação de encerrante por bico).
5. **Conciliação de combustível** (o KPI que importa):
   `saldo_esperado = saldo_inicial + entradas − vendas_litros`
   `quebra = medicao_sensor − saldo_esperado`
   classificada em faixas: dentro da tolerância (evaporação aceitável por
   produto), sobra anormal, perda anormal / possível furto.

### Plano de dados (alinhado à arquitetura STG → CH → API)
- Novas entidades STG (idempotentes, brutas, auditáveis): `stg.tanques`,
  `stg.bicos`, `stg.combustivel_entradas`, `stg.tanque_medicoes` (sensor). Nomes a
  confirmar conforme origem Xpert.
- Nova mart realtime em `torqmind_mart_rt`: `mart_estoque_combustivel_rt` por
  `id_empresa, id_filial, id_tanque, data_key` com:
  `saldo_inicial, entradas_litros, vendas_litros, saldo_esperado,
  medicao_sensor, quebra_litros, quebra_pct, faixa_quebra`.
- Endpoint `/bi/stock/fuel/overview` (resolve a dívida técnica
  `stock_position_summary`).
- **Dashboard geral**: card de topo "Estoque de combustível" mostrando, por
  produto/tanque, saldo atual estimado, % do tanque e **alerta de quebra** quando
  passar a tolerância. Esse é o "uau" que o dono pediu de cara.

### Mapeamento confirmado na Xpert (2026-06-03)
Conexão direta no SQL Server `ATXDADOS` via `tools/xpert_source_explorer.py`.
Tabelas reais encontradas (linhas aproximadas):

| Camada | Tabela Xpert | Chave / colunas úteis | Linhas | Estado |
|--------|--------------|-----------------------|--------|--------|
| Estoque geral (por produto) | `ESTOQUE` | `ID_FILIAL, ID_PRODUTOS, ID_LOCALVENDAS, QTDEATUAL` | 757.873 | populada |
| Cadastro de tanque | `TANQUES` | `ID_TANQUES, ID_FILIAL, ID_PRODUTOS, CAPACIDADE, QTDEMINIMA, ATIVO` | 87 | populada |
| Saldo atual por tanque | `QTDESTANQUES` | `ID_TANQUES, ID_FILIAL, QTDEATUAL` | 87 | populada |
| Medição física do tanque | `MOVTANQUES` | `ID_TURNOS, ID_TANQUES, LEITURA, QTDESISTEMA, CENTIMETROS, ABERTURA, DTACONTA` | 46.206 | **viva** (2026-06-02) |
| Cadastro de bico | `BICOS` | `ID_BICOS, ID_FILIAL, ID_TANQUES, LEITURAATUAL, BICODEFEITO` | 267 | populada |
| Cadastro de bomba | `BOMBAS` | `ID_BOMBA, ID_FILIAL` | 76 | populada |
| Volumetria por bico/turno | `ENCERRANTESTURNOS` | `ID_BICOS, ID_TURNOS, ENCERRANTEABERTURA, ENCERRANTEFECHAMENTO, AFERICAO, PPL` | 1.267.978 | **CONGELADA** (máx 2025-09-19) |
| LMC (livro ANP) — cabeçalho | `LMC` | livro de movimentação de combustíveis | 95.163 | **viva** (2026-05-26) |
| LMC por tanque | `LMCTANQUES` | `ID_TANQUES, QTDETANQUE, QTDECONCILIACAO` | 106.601 | **viva** (2026-05-26) |
| LMC por bico | `LMCBICOS` | `ID_BICOS, ID_TANQUES, ENCERRANTEABERTURA/FECHAMENTO, AFERICAO, VENDAS, PPL` | 324.070 | **viva** (2026-05-26) |
| Entradas de combustível | `LMCENTRADATANQUES` | entradas por tanque | 34.970 | populada |
| Aferição (ver §6) | `AFERICAO` | `ID_BICOS, ID_TURNOS, QTDE, DATA, ID_USUARIOS, ID_USUARIOS_LIB` | 19.525 | **viva** (2026-06-03) |

**Conclusões do mapeamento (respondem ao que o dono perguntou):**
- **Estoque total por produto:** controlado para TODAS as filiais ativas
  (`ESTOQUE`, ~17,5 mil produtos por filial = catálogo completo, inclui
  conveniência). É a base de "estoque absoluto" geral.
- **Estoque por tanque:** controlado em todos os postos (4 a 7 tanques cada) via
  `TANQUES` + `QTDESTANQUES` (saldo) + `MOVTANQUES` (medição física régua/
  centímetros vs sistema, **atual** 2026-06). Filiais administrativas/parada/
  distribuidora (14126, 14779, 14780, 14930, 15121, 17719) não têm tanque — é o
  esperado, não são pontos de combustível.
- **Controle por bico:** existe — 267 bicos cadastrados (266 OK, 1 em defeito
  por `BICODEFEITO`; o campo `STATUS` está vazio e não deve ser usado como flag
  de ativo). Cada bico liga a um tanque (`BICOS.ID_TANQUES`).
- **Volumetria viva é o LMC, NÃO o encerrante-turno.** `ENCERRANTESTURNOS` e
  `ENCERRANTES` pararam de replicar em **2025-09-19** (congeladas há ~8 meses).
  A fonte canônica e atual de litros por bico/tanque é a família **`LMC` /
  `LMCBICOS` / `LMCTANQUES` / `LMCENTRADATANQUES`** (Livro de Movimentação de
  Combustíveis, obrigatório ANP), atualizada até 2026-05-26. **Implicação direta:
  a trilha de estoque de combustível deve ler do LMC, não do encerrante-turno.**

### Rastreio que ainda falta (com o cliente / SSH Produção)
- [x] ~~Localizar tabelas de tanque/bico/bomba e ligação bico↔produto↔tanque~~
      → `TANQUES`, `BICOS`, `BOMBAS`, `QTDESTANQUES` (mapeado acima).
- [x] ~~Localizar a entrada de combustível~~ → `LMCENTRADATANQUES` (+ `LMC`).
- [ ] **Sensor/sonda físico:** `MOVTANQUES` traz `LEITURA`/`CENTIMETROS`
      (medição física) e `QTDESISTEMA`; confirmar com o cliente se a `LEITURA`
      vem de sonda automática ou medição manual, e avaliar confiabilidade
      (série sensor × saldo calculado em alguns dias). `LEITURASTANQUES`
      (5 linhas) e `TANQUE_MEDIDOR` (0) sugerem que a sonda automática quase não
      é usada — provável medição manual via régua.
- [ ] **Tolerância de quebra por produto** com o cliente (% evaporação aceitável
      etanol vs diesel) — `LMC_DIVERGENCIAS` (103 linhas) já registra divergências
      no padrão do próprio sistema; vale comparar nosso cálculo com o dele.

### Risco / cuidado
- Não inventar volume nem custo: sem dado confiável de sensor/entrada, mostrar
  "sem leitura" em vez de número fabricado. Nunca expor margem/custo a
  gerente/vendedor.

---

## 2. Conciliação de cartão / TEF (bandeiras + taxas) — Financeiro

### A dor real
Posto vende pesado no cartão. O TorqMind mostra formas de pagamento, mas não
mostra o que dói: **quanto a adquirente (Cielo/Rede/Stone) efetivamente pagou**
versus o que foi vendido, **descontada a taxa**. A margem real vaza aqui e
ninguém vê. Também não há detecção de "vendi no cartão mas não caiu na conta".

### O que JÁ existe na base
- A STG de formas de pagamento **já captura** `bandeira`, `rede`, `tef`, `nsu` e
  `autorizacao` (ver `apps/cdc_consumer/torqmind_cdc_consumer/mappings.py`,
  formas de pagamento de comprovante). Ou seja, **o lado "vendas por bandeira"
  já é possível** com o dado atual.
- Pagamentos já têm marts (`agg_pagamentos_diaria`, `agg_pagamentos_turno`) e
  tela `/payments`.

### O que FALTA (gap técnico)
1. **Cadastro de taxas por bandeira/produto/prazo**: quanto cada bandeira cobra
   (débito, crédito à vista, parcelado, antecipação). Hoje **não existe**. É a
   peça-chave para calcular o líquido.
2. **Extrato da adquirente**: o que a operadora realmente depositou (por
   data/bandeira/valor bruto/taxa/valor líquido). Hoje **não entra no sistema**.
   Pode vir por: arquivo de conciliação (EDI/CNAB da adquirente), API da
   operadora, ou export manual.
3. **Mart de conciliação**: cruzar "vendas por bandeira/dia" (que já temos) com
   "depósitos da adquirente" e com "taxa esperada", apontando divergências
   (venda sem crédito, taxa acima do contratado, atraso de repasse).

### Plano de rastreio (faseado, como o dono pediu)
- **Fase 0 — Rastreio de cadastro (primeiro passo, antes de qualquer cruzamento):**
  - [ ] Verificar se as **bandeiras estão corretamente cadastradas/normalizadas**
        nos comprovantes (qualidade do campo `bandeira`/`rede`/`tef` na STG real;
        quanto vem nulo/sujo).
  - [ ] Verificar se existe **cadastro de taxas** em algum lugar (Xpert ou a
        criar no TorqMind, schema `app`).
  - [ ] Mapear de onde virá o **extrato da adquirente** (arquivo, API ou manual).
- **Fase 1 — Vendas por bandeira (entrega rápida com dado atual):**
  - Mart `mart_pagamentos_bandeira_rt` (bruto por bandeira/dia/turno) → já dá
    visão "quanto vendi em cada bandeira". Sem depender de taxa.
- **Fase 2 — Líquido esperado:** aplicar taxa cadastrada → valor líquido previsto.
- **Fase 3 — Conciliação real:** importar extrato da adquirente → divergências.

### Mapeamento confirmado na Xpert (2026-06-03)
Tabelas reais de cartão/convênio encontradas:

| Função | Tabela Xpert | Linhas | Observação |
|--------|--------------|--------|------------|
| Cadastro operadora/cartão débito | `CARTAODEBITO` | 748 | operadoras/redes |
| Redes TEF | `REDETEF` | 748 | — |
| Bandeiras | `BANDEIRASTEF` / `CFGBANDEIRASTEF` | 1.078 / 968 | normalização de bandeira |
| Códigos administradoras TEF | `CODIGOSADMTEF` | 34 | — |
| Cadastro de convênios (faturado) | `CONVENIOS` | 2.221 | clientes/empresas convênio |
| Movimento cartão | `MOVCARTAODEBITO` | 2.827.823 | transações cartão |
| Movimento convênio | `MOVCONVENIOS` | 3.105.392 | transações convênio |
| Baixa/conciliação cartão | `BAIXACARTAO` / `CARTAODABAIXACARTAO` | 6.659 / 8.711 | baixa de recebíveis |
| Conciliação EDI | `CONCILIACAOVENDASCARTOES_EDI` | 85 | extrato adquirente (EDI) |
| Taxas venda a prazo | `TAXASVENDASPRAZO` | 22 | parcial |
| **Taxas da administradora** | `TAXASADMINISTRADORA` | **0** | **VAZIA** |

**Achado crítico para a Fase 0:** `TAXASADMINISTRADORA` está **vazia** — o cliente
**não cadastra a taxa por adquirente/bandeira na Xpert**. Logo, o cálculo de
líquido (Fase 2) **não tem fonte de taxa hoje**. Caminhos: (a) criar cadastro de
taxas no próprio TorqMind (schema `app`), ou (b) extrair a taxa efetiva do
extrato EDI (`CONCILIACAOVENDASCARTOES_EDI`, que já tem 85 linhas) comparando
bruto × líquido depositado. A Fase 1 (vendas por bandeira) é viável já, pois o
movimento e as bandeiras existem. A conciliação real (Fase 3) já tem semente no
EDI — vale avaliar a qualidade dessas 85 linhas com o cliente.

### Onde aparece
- Tela `/finance` (Financeiro), seção "Conciliação de cartões". Nunca expor
  margem/custo a gerente/vendedor; taxa de cartão é informação financeira
  sensível — checar role.

---

## 3. Contas a pagar (a levantar com o cliente)

### A dor real
O financeiro hoje cobre **recebíveis** (vencimentos, aging — `finance_aging_daily`,
`financeiro_vencimentos_diaria`). Falta a ponta de **pagar**: fornecedores,
distribuidora, folha, tributos. O dono diz que "contas a pagar sempre vem
quebrado no cliente" — ou seja, o dado de origem é problemático e precisa de
levantamento dedicado.

### O que FALTA / próximo passo
- **Não implementar ainda.** Primeiro o dono vai **levantar com o cliente** o que
  ele realmente precisa e por que o dado vem quebrado.
- Quando levantado, mapear a origem na Xpert (contas a pagar / títulos /
  fornecedores), avaliar qualidade, e só então desenhar STG + mart de aging de
  **pagar** (espelho do que já existe para receber) e um **fluxo de caixa
  projetado** (saldo previsto 30/60 dias = receber − pagar).

### Pendência de levantamento
- [ ] Cliente detalha requisitos de contas a pagar.
- [ ] Mapear origem Xpert e diagnosticar o "quebrado".

---

## 4. Alerta de caixa aberto >24h no celular (Telegram)

### A dor real
O cliente já pediu e **já alertamos no sistema** (in-app:
`alerta_caixa_aberto` / `open_cash_monitor`), mas **não chega no celular** o
alerta de caixa. O dono quer o "ping" no telefone, com mensagem bonita e dados
reais (sem "caixa ?" sem nome).

### O que JÁ existe — MUITO mais do que parecia (auditoria 2026-06-03)
A infraestrutura de Telegram **já é madura e funcional** (o alerta de
**cancelamento de venda** já roda em produção por ela):
- **Serviço completo:** `apps/api/app/services/telegram.py` — `send_telegram_alert(id_empresa, payload, force)`
  com: gate por severidade (`settings.notify_min_severity`, default CRITICAL),
  resolução de destinatários (`app.telegram_settings` por empresa, ou OWNER/MASTER
  via `app.user_notification_settings`), **de-duplicação diária**
  (`app.telegram_dispatch_log`, hash `empresa|filial|insight|data`), envio com
  retry/backoff. Token em env (`TELEGRAM_BOT_TOKEN`).
- **Disparo de caixa JÁ CODADO E JÁ LIGADO:** `_dispatch_cash_telegram_alerts`
  em `apps/api/app/services/etl_orchestrator.py` já lê o mart de caixa aberto,
  monta o payload `CASH_OPEN_OVER_24H` (filial, operador, horas) e chama
  `send_telegram_alert`. É invocado no pós-refresh do orquestrador quando
  `cash_notifications > 0`. **Não é "criar do zero".**
- **Mensagem já é boa:** a `mart.alerta_caixa_aberto` (migration 033) já gera
  título *"Caixa {turno} aberto há {h} horas"* e corpo com filial + operador,
  com `COALESCE` de fallback (*"Filial {id}"* / *"não identificado"*). **Nunca
  produz "caixa ?"** — o número do caixa (`id_turno`) está sempre presente.

### Causa-raiz REAL de não chegar no celular (o gap verdadeiro)
O disparo existe, mas **a fonte que ele lê está morta em produção**:
- `mart.alerta_caixa_aberto` (PostgreSQL) está **vazia em prod** (0 linhas,
  `updated_at` NULL). No **cutover realtime**, os marts migraram para ClickHouse
  `torqmind_mart_rt` e essa materialized view do PG **deixou de ser atualizada**.
- No ClickHouse **não existe** `torqmind_mart_rt.alerta_caixa_aberto`. O dado de
  caixa aberto vive em `torqmind_mart_rt.cash_overview_rt` (usado por
  `repos_mart_realtime.open_cash_monitor`: `is_aberto=1`, `abertura_ts`,
  `nome_operador`).
- Por isso o **cancelamento** chega (usa caminho raw realtime independente:
  `raw_comprovante_is_cancelled` → `app.alert_comprovante_cancelado`), mas o
  **caixa aberto não** (depende do mart PG morto).

### Correção aplicada (2026-06-03) — migration 090
Em vez de criar um dispatcher novo lendo ClickHouse (que **perderia o filtro
`is_operational_live` de 96h** e dispararia ~145 turnos antigos/ruído nunca
fechados), a correção mais simples e semanticamente correta foi **restaurar o
refresh da própria mart PG dentro do fast-path operacional**:
- **`sql/migrations/090_cash_open_notifications_refresh_marts.sql`**: redefine
  `etl.sync_cash_open_notifications(int)` para dar
  `REFRESH MATERIALIZED VIEW mart.agg_caixa_turno_aberto` e depois
  `mart.alerta_caixa_aberto` **antes** de ler/upsertar em `app.notifications`.
- Essa função **já é chamada todo ciclo** pelo track operacional sempre que há
  caixa aberto (`cash_changed OR clock_cash_notifications`, via
  `etl.collect_tenant_clock_meta` → `clock_cash_notifications`), inclusive
  **sem ingestão nova** (clock-driven). Logo as marts voltam a ficar frescas a
  cada execução do cron operacional (`*/2 min`).
- O REFRESH é barato (fonte é `dw.fact_caixa_turno` filtrada por `is_aberto`) e
  o filtro `is_operational_live` (atividade ≤ 96h) + `horas_aberto >= 24` da
  própria `mart.alerta_caixa_aberto` mantém só o **sinal real**.
- O dispatcher `_dispatch_cash_telegram_alerts` (já codado/ligado) então
  encontra `cash_notifications > 0` e dispara o Telegram com a mensagem boa.

**Validação prod (2026-06-03):** após aplicar a 090,
`etl.sync_cash_open_notifications(1)` ⇒ `sync_rows=2`; `mart.alerta_caixa_aberto`
⇒ `2` linhas (sinal real, não os 145 de ruído); `app.notifications` de caixa nos
últimos 5 min ⇒ `2`. Mensagem sem "caixa ?" (turno + filial + operador presentes).

> Nota: `etl.sync_payment_anomaly_notifications` tem o **mesmo padrão** (lê mart
> sem refrescar). Pagamentos seguem outro caminho de refresh, mas vale auditar
> em rodada futura se as anomalias de pagamento também precisam do mesmo fix.

### Cuidado
- Token do bot é segredo → só em `/etc/torqmind/prod.app.env`, nunca no código.
- Não vazar margem/custo em mensagem para grupo de gerentes/vendedores.
- Reusar o de-dup para não floodar (1 alerta por turno por dia).

---

## 5. Benchmark entre filiais — Metas & Equipe (enriquecimento)

### A dor / ressalva
O dono achou interessante, mas **a comparação não é justa**: postos de beira de
estrada vendem absurdamente mais que os de dentro da cidade. Logo, **não** é um
ranking cru de faturamento — vira injustiça e desmotiva.

### Como fazer ficar bom (e justo o suficiente)
- Posicionar como **enriquecimento visual da tela de Metas & Equipe**, não como
  verdade absoluta.
- Comparar por **indicadores normalizados**, não por volume bruto:
  - faturamento por m³ vendido (eficiência de margem por litro);
  - ticket médio;
  - venda por frentista / por turno;
  - % de cancelamento / risco (quanto menor, melhor);
  - atingimento de meta (% da própria meta — já temos metas/comissão).
- Permitir **agrupar filiais por perfil** (estrada × cidade) para comparar
  "laranja com laranja".

### O que JÁ existe
- Rankings de funcionários, metas e comissão prontos (`agg_funcionarios_diaria`,
  módulo de comissão, `leaderboard_employees`). Falta a **visão comparativa
  entre filiais** com normalização.

### O que FALTA
- Mart/consulta `benchmark_filiais` consolidando os indicadores normalizados por
  filial no período, respeitando o escopo de permissão (só vê as filiais a que
  tem acesso).
- Componente visual na tela de Metas & Equipe (cards/tabela comparativa, talvez
  com agrupamento por perfil).

---

## 6. Aferição / INMETRO de bombas — via Xpert + Telegram

### A dor real
A aferição (calibração legal das bombas) **já existe no sistema da Xpert** e o
cliente cuida manualmente. O valor para o TorqMind é: **se conseguirmos ler
isso do SQL da Xpert**, podemos exibir e — principalmente — **notificar no
Telegram** quando uma aferição estiver vencendo/vencida (risco de multa).

### O que FALTA (gap técnico)
1. ~~Mapear a tabela de aferição~~ — **FEITO (2026-06-03).** Tabela `AFERICAO`
   confirmada na Xpert: `ID_AFERICAO, ID_FILIAL, ID_BICOS, ID_TURNOS, QTDE,
   DATA, ID_USUARIOS, ID_USUARIOS_LIB`. **19.525 linhas, viva** (última
   2026-06-03). Há também o campo `AFERICAO` dentro de `ENCERRANTESTURNOS` e de
   `LMCBICOS`. É a aferição operacional diária do bico (com usuário que executou
   e usuário liberador) — cobertura por filial já validada (quase todos os
   postos com aferição recente; ver matriz em §1).
2. **Atenção ao conceito:** `AFERICAO` registra o **ato** de aferir o bico (qtde
   aferida no turno), não uma "data de validade INMETRO". O alerta de
   *vencimento legal* (calibração INMETRO periódica) depende de outra fonte —
   confirmar com o cliente se a validade INMETRO é controlada em algum cadastro
   (candidatos: `BICOS.DATALACRE`/`NROLACREBOMBA`, que trazem lacre e data).
3. **Ingerir** como entidade STG (`stg.afericoes` — nome a confirmar).
4. **Exibir** num lugar discreto (provável: dentro do contexto de bombas/estoque
   da §1, ou um card de "conformidade").
5. **Notificar no Telegram** (reaproveita §4) — ex.: bico sem aferição no período
   esperado, ou lacre vencendo (`BICOS.DATALACRE`).

### Rastreio
- [x] ~~Localizar a estrutura de aferição no banco Xpert~~ → `AFERICAO` (viva).
- [ ] Confirmar com o cliente onde mora a **validade legal INMETRO** (lacre/
      calibração periódica) vs a aferição operacional diária.

---

## 7. Jarvis IA — manter, repaginar ou remover? (recomendação honesta)

### O que existe hoje
- `/bi/jarvis/briefing` (resumo determinístico sobre marts) e
  `/bi/jarvis/generate` (gera planos com IA — OpenAI Responses API) com:
  cache por hash (`app.insight_ai_cache`), top-N por impacto, custo controlado
  por token, fallback determinístico quando a IA falha/não está configurada.
  Implementação em `apps/api/app/services/jarvis_ai.py`. Exibido no dashboard
  geral como briefing do dia.

### Diagnóstico honesto
A sensação do dono ("não faz tanto sentido hoje") tem fundamento. O formato
atual é um **briefing genérico e passivo**: a IA reescreve em prosa o que os
KPIs já mostram. Isso entrega pouco valor incremental e ainda gera custo de
token. **IA só vale a pena quando faz algo que o número sozinho não faz.**

### Recomendação: NÃO deletar a infraestrutura, e sim repaginar para "IA sob
demanda e contextual"
Em vez de um texto diário automático, a IA passa a responder **quando o usuário
pede** e **ancorada num evento concreto**:

1. **"Explique este alerta / o que eu faço?"** — botão ao lado de cada alerta
   real (caixa aberto >24h, quebra de combustível anormal §1, divergência de
   conciliação §2, pico de cancelamento). A IA recebe **só os números daquele
   evento** e devolve causa provável + ação recomendada. Aí sim a IA agrega:
   transforma dado em decisão, no momento da dor.
2. **Resumo executivo semanal opcional** (não diário automático), gerado a
   pedido, com comparativo período a período.
3. **Desligar a geração automática diária** (economiza token e remove ruído).

Por que manter o encanamento: já temos cache, fallback, controle de custo e
governança de prompt prontos — jogar fora seria desperdício. O ajuste é de
**produto/posicionamento**, não de engenharia pesada.

### Critério de decisão (se o dono preferir simplificar)
- Se, mesmo repaginado, o uso for baixo após algumas semanas → **remover**:
  tirar o card do dashboard, desativar `/bi/jarvis/generate` por flag, manter o
  briefing determinístico (sem IA) ou removê-lo também. A remoção é limpa
  (endpoints isolados, flag de ambiente, sem acoplamento com o hot path de
  vendas).

### Pendência de decisão
- [ ] Dono decide: **repaginar para IA sob demanda** (recomendado) **ou remover**.

---

## 8. Revenda de vale-combustível por funcionário (fraude — a levantar)

### A dor real
O posto concede a funcionários um **valor mensal de vale-combustível** (ex.: "X
reais para abastecer"). O dono relata que **funcionários revendem esse vale para
terceiros**: o terceiro abastece, o funcionário "passa no nome dele" (consome a
cota), e há a troca por dinheiro por fora. É **desvio de benefício / fraude
operacional** — o benefício deixa de cumprir sua função e vira renda paralela.

### O que precisa ser levantado ANTES de codar
- [ ] Confirmar **como o vale é controlado hoje** na Xpert: é lançado como uma
      forma de pagamento/convênio interno, como abastecimento "no nome do
      funcionário", ou fora do sistema (caderneta)? Candidatos mapeados:
      `VALECOMBUSTIVEL` (0 linhas hoje), `INSVALECOMBUSTIVEL`/`...BAIXA` (0),
      `CONVENIOS`/`MOVCONVENIOS` (se o vale for modelado como convênio do
      funcionário). **Hoje as tabelas dedicadas de vale estão zeradas** — forte
      indício de que o vale **não é controlado no sistema** ou usa outro caminho.
- [ ] Se for via convênio/funcionário: cruzar **abastecimentos no nome do
      funcionário × cota** e procurar padrões de fraude (volume acima do razoável,
      múltiplos veículos/placas, horários incompatíveis com turno do funcionário).

### Possível direção (depois do levantamento)
- Se houver dado: card/relatório de **consumo de vale por funcionário** com
  alertas (cota estourada, placa recorrente que não é do funcionário). Encaixa no
  módulo de **Antifraude** ou num "RH operacional". Reusa o canal Telegram (§4).
- Se não houver dado confiável: documentar como **limitação de origem** e propor
  ao cliente passar a registrar o vale no sistema (pré-requisito).

### Cuidado
- Tema sensível (relação trabalhista). Tratar como indicador de auditoria, não
  acusação automática. Permissão restrita (não é tela de vendedor/gerente comum).

---

## 9. Validação de assinatura / cheque por IA (FUTURO — só documentação)

### A ideia
Validar automaticamente **assinaturas em comprovantes/recibos** e **cheques**
(preenchimento, assinatura, CMC-7, valor por extenso × numérico) usando IA de
visão, para reduzir fraude e devolução de cheque.

### Por que é só documentação agora
- **Não há, no mapeamento atual, fonte com a imagem do documento assinado.** A
  Xpert guarda dados estruturados (valores, datas, NSU), não a digitalização do
  cheque/assinatura. Sem a imagem, não há o que a IA analise.
- É um item **complexo e de alto custo** (captura de imagem no PDV, OCR/visão,
  base de assinaturas de referência, LGPD sobre documentos pessoais).

### Pré-requisitos para sair do papel (futuro)
- [ ] Existir captura/armazenamento da **imagem** do cheque/assinatura (no PDV ou
      por upload), com consentimento/LGPD.
- [ ] Definir o caso de uso preciso (cheque pré-datado? recibo de fiado?
      autorização de frota?).
- [ ] Só então avaliar modelo de visão + custo + acurácia mínima aceitável.

### Decisão
- **Não implementar.** Manter registrado como visão de futuro; reabrir quando
  houver fonte de imagem e demanda concreta do cliente.

---

## Ordem sugerida de execução (proposta)

1. **§1 Estoque de combustível** — maior dor e maior diferencial; começa pelo
   rastreio das tabelas Xpert (tanque/bico/sensor/entrada) e tolerância de quebra.
2. **§4 Telegram (caixa aberto >24h)** — barato, rápido, alta percepção de valor;
   cria a base de notificação externa que §1 e §6 vão reusar.
3. **§2 Conciliação de cartão** — Fase 0 (rastreio de bandeiras/taxas) em
   paralelo, pois depende de qualidade de cadastro e de origem do extrato.
4. **§6 Aferição** — depende do mesmo rastreio Xpert da §1 e do canal Telegram da §4.
5. **§5 Benchmark entre filiais** — enriquecimento visual; sem dependência externa.
6. **§3 Contas a pagar** — só após levantamento do dono com o cliente.
7. **§8 Revenda de vale-combustível** — após levantar como o vale é (ou não)
   controlado no sistema; hoje as tabelas dedicadas estão zeradas.
8. **§7 Jarvis** — decisão de produto; pode andar a qualquer momento.
9. **§9 Validação de assinatura/cheque por IA** — futuro; só quando houver fonte
   de imagem e demanda concreta.

---

## Princípios que valem para tudo acima

- **Não inventar dado.** Sem leitura confiável (sensor, taxa, extrato), mostrar
  "sem cadastro / sem leitura", nunca número fabricado (regra de ouro TorqMind:
  proibido fallback 1970/data_key=0/meio-dia inventado).
- **STG é bruta e auditável.** Toda nova origem entra como STG idempotente antes
  de virar mart.
- **Permissão na API.** Estoque/custo/taxa são sensíveis: gerente/vendedor nunca
  veem margem/lucro/custo. Esconder no front não basta.
- **Segredos só em env.** Tokens de bot, chaves de adquirente e de IA jamais no
  código ou em commit.
- **Medir antes de otimizar.** Novas marts/telas seguem o alvo de endpoint quente
  < 2s.
- **Atualizar o `CODEX_TORQMIND_MAP.md`** a cada item entregue.
