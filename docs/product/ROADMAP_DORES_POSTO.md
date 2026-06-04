# TorqMind — Dores a Resolver (Roadmap de Produto para Postos)

> Documento de planejamento e mapeamento. Não é changelog. Registra dores reais
> de dono de posto que o TorqMind ainda não resolve, o que já existe na base de
> dados/arquitetura, o gap técnico de cada uma e o plano de rastreio antes de
> implementar. Atualizar conforme o cliente validar prioridades.
>
> Data inicial: 2026-06-03. Cliente de referência: rede Xpert (SQL Server origem).
> Arquitetura fixa: `SQL Server Xpert -> Agent -> API ingest -> PostgreSQL STG ->
> Debezium/Redpanda -> CDC Consumer -> ClickHouse current/mart_rt -> API -> Web`.

---

## Sumário das decisões (o que entra e o que fica de fora)

| # | Tema | Decisão do dono | Onde encaixa |
|---|------|-----------------|--------------|
| 1 | Controle de estoque de combustível por bomba/tanque | **ENTRA — prioridade máxima.** Quer logo no dashboard geral. | Nova trilha de dados (encerrante/tanque) + KPI no dashboard |
| 2 | Conciliação de cartão/TEF (bandeiras + taxas) | **ENTRA.** Tratar no Financeiro, após rastreio de cadastro de bandeiras/taxas. | `/finance` + novo mart de conciliação |
| 3 | Contas a pagar | **ENTRA (a levantar).** Sempre quebrado no cliente; detalhar requisitos com ele depois. | `/finance` |
| 4 | Alerta de caixa aberto >24h no celular (Telegram) | **ENTRA — fácil e rápido.** Já existe alerta in-app; falta o push. | Notificação externa (Telegram) |
| 5 | Benchmark entre filiais | **ENTRA como enriquecimento visual.** Ciência da injustiça estrada × cidade. | Tela de Metas & Equipe |
| 6 | Aferição/INMETRO de bombas | **ENTRA se a Xpert guardar isso no SQL.** Mapear tabela; principal valor é notificação Telegram. | Ingestão Xpert + Telegram |
| 7 | Jarvis IA (briefing no dashboard) | **REAVALIAR.** Dono não vê mais sentido hoje. Ver §7 para recomendação. | Decisão: repaginar ou remover |
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

### Rastreio necessário ANTES de codar (com SSH Produção / origem Xpert)
- [ ] Localizar no SQL Server Xpert as tabelas de **tanque, bico/bomba e
      ligação bico↔produto↔tanque** (candidatas: nomes com BICO/BOMBA/TANQUE/
      ENCERRANTE/ABASTECIMENTO; o explorer `tools/xpert_source_explorer.py` já
      classifica domínio `estoque`).
- [ ] Localizar a tabela de **leituras do sensor/sonda** (medição diária) e
      avaliar **confiabilidade** com amostra real (sensor × saldo calculado).
- [ ] Localizar a **entrada de combustível** (nota da distribuidora: volume,
      tanque, data, custo). Confirmar se já está em alguma STG hoje.
- [ ] Definir a **tolerância de quebra por produto** com o cliente (ex.: % de
      evaporação aceitável para etanol vs diesel).

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

## 4. Alerta de caixa aberto >24h no celular (Telegram) — RÁPIDO

### A dor real
O cliente já pediu e **já alertamos no sistema** (in-app:
`alerta_caixa_aberto` / `open_cash_monitor`), mas **não alertamos no celular**.
O dono quer o "ping" no telefone.

### O que JÁ existe
- Detecção de caixa aberto pronta: mart `alerta_caixa_aberto` com `status`,
  `hours_open`, `severity`; função `open_cash_monitor`; alertas de cancelamento
  já existem in-app. Só falta o **canal externo**.

### O que FALTA (gap técnico — pequeno)
1. **Canal Telegram**: bot + envio de mensagem. Mais simples e gratuito que
   WhatsApp (sem custo por mensagem, sem aprovação de template).
2. **Serviço de notificação externa** reaproveitável (igual fizemos o
   `email_service.py`): `telegram_service.py` env-driven, no-op seguro quando não
   configurado (`TELEGRAM_ENABLED`, `TELEGRAM_BOT_TOKEN`, `TELEGRAM_CHAT_ID` por
   destinatário/filial).
3. **Regra de disparo + de-duplicação**: dispara uma vez quando cruza 24h, não a
   cada ciclo do cron. Reaproveitar o hash de insights / tabela de notificações.
4. **Destinatários por escopo**: qual chat recebe alertas de qual filial.

### Plano
- Começar por **caixa aberto >24h** (já pedido). Estrutura serve depois para
  fraude, quebra de combustível (§1) e aferição (§6).
- Disparo a partir do cron operacional que já roda (`prod-etl-pipeline.sh`).

### Cuidado
- Token do bot é segredo → só em `/etc/torqmind/prod.app.env`, nunca no código.
- Não vazar margem/custo em mensagem para grupo de gerentes/vendedores.

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
1. **Mapear a tabela de aferição** no SQL Server da Xpert (data da última
   aferição, validade, bomba/bico, status). Usar `xpert_source_explorer.py`.
2. **Ingerir** como entidade STG (`stg.afericoes` — nome a confirmar).
3. **Exibir** num lugar discreto (provável: dentro do contexto de bombas/estoque
   da §1, ou um card de "conformidade").
4. **Notificar no Telegram** (reaproveita §4) quando faltar X dias para vencer.

### Rastreio
- [ ] Localizar a estrutura de aferição no banco Xpert e validar que é confiável
      e atualizada.

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
7. **§7 Jarvis** — decisão de produto; pode andar a qualquer momento.

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
