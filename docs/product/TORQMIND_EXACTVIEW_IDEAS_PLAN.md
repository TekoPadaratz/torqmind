# TorqMind — Plano de ideias a partir do ExactView (postobi.com.br)

> Documento vivo para refinar com o time.  
> Origem: revisão da demo ExactView em 2026-07-29 (`demo@email.com`).  
> Domínio `postobi.com.br` = produto **ExactView** (BI para rede de postos).  
> Provas: API autenticada (login JWT, filtros, dashboard, tanques, simulador, pivot, notificações).  
> **Última atualização:** 2026-07-29.

---

## 1. Contexto em uma frase

O ExactView vende clareza para o **dono**: “quanto tenho no tanque?”, “dá pra quantos dias?”, “e se eu mudar o preço?”, “monta esse relatório pra mim”, “o que está crítico agora?”.  
O TorqMind já é forte em **tempo real, antifraude, caixa/turno e agent Xpert**. Este plano não copia o ExactView — **pega o que falta na rotina do posto** e encaixa na nossa arquitetura (ClickHouse-first, multi-tenant, ACL).

---

## 2. Prioridade acordada (ponto de partida deste refine)

| Prioridade | Tema | Status no refine |
|------------|------|------------------|
| **P0 (você já entendeu e quer)** | Estoque + tanques + cobertura em dias + quantidade/valor | **Âncora do plano** |
| P1 | Explicar e decidir: alertas, simulador, OLAP, etc. | Abaixo, em português de posto |
| Fora de escopo agora | Virar ERP (compras fiscais, carta-frete, patrimônio contábil completo) | Só mencionar |

---

## 3. P0 — Estoque, tanques e cobertura (detalhe)

### 3.1 O problema no posto (dor real)

Hoje o gerente/dono muitas vezes:

- Abre o Xpert ou planilha para ver **nível do tanque**.
- Não sabe de cabeça: “Diesel S500 acaba em **quantos dias** no ritmo atual?”.
- Mistura **litros** (combustível) com **unidades/R$** (loja) sem uma tela só.
- Descobre ruptura tarde (bico seco / gondola vazia).

Isso é operacional **todo dia**, não “relatório de fim de mês”.

### 3.2 O que o ExactView mostra (prova)

Na demo, `GET /estoque/tanques` devolve por tanque/produto:

- capacidade, estoque atual, % ocupado, volume disponível  
- **média diária** de saída  
- **`dias_cobertura`** (estoque ÷ média)  

E `GET /estoque/sugestao-compra` sugere quanto comprar para atingir N dias de cobertura (ex.: 7).

Alertas do tipo `tanque_baixo` / `estoque_critico` apontam SKU/tanque com cobertura curta.

### 3.3 O que queremos no TorqMind (visão de produto)

Uma tela (sugestão: **`/inventory`** ou **Estoque**) com três blocos:

1. **Tanques (combustível)**  
   - Por filial / tanque / produto  
   - Litros atuais, capacidade, % cheio  
   - Média diária (7 e/ou 14 dias)  
   - **Dias de cobertura** (com semáforo: crítico / atenção / ok)  
   - Opcional: evolução dos últimos dias (sparkline)

2. **Estoque de loja / não-combustível**  
   - Quantidade em unidades  
   - **Valor em R$** (custo e/ou venda — **custo/margem só owner**)  
   - Cobertura em dias quando houver giro confiável  
   - Top críticos (menor cobertura / maior valor parado)

3. **Sugestão de reposição (simples)**  
   - “Para ficar com X dias de diesel, faltam Y litros”  
   - Lista priorizada (não precisa ser pedido de compra fiscal ainda)

### 3.4 Como o dono usaria no TorqMind (cenários)

| Momento | Ação na tela |
|---------|----------------|
| Manhã | Olha semáforo dos tanques da rede / filial |
| Antes de pedir carga | Vê dias de cobertura + sugestão em litros |
| Fim de semana / feriado | Simula mentalmente “se a média subir 20%, quantos dias sobram?” (fase 2) |
| Gerente (sem margem) | Vê litros e quantidade; **não** vê custo/CMV |
| Owner | Vê também valor de estoque e risco de capital parado |

### 3.5 Encaixe técnico (rascunho — ClickHouse-first)

Já temos ingest de datasets de tanque/estoque no agent (`tanques`, `movtanques`, `estoque`, etc.). O trabalho é:

1. **Mart CH** (ex.: `inventory_tanks_rt`, `inventory_sku_rt`) com grão claro + `id_empresa` / `id_filial`.  
2. Fórmulas explícitas e auditáveis:
   - `dias_cobertura = estoque_atual / NULLIF(media_diaria, 0)`  
   - `media_diaria` = saída líquida dos últimos N dias (definir N: 7 padrão, 14 opcional)  
3. API BI só lê CH (nunca `stg` no hot path).  
4. ACL: quantidade/litros ok para gerente; **valor a custo / margem** só owner/platform.  
5. Ligar alertas (quando existir o centro de alertas) a esses mesmos campos.

### 3.6 Aceite (definição de pronto)

- [ ] Números de 1–2 tanques batem com Xpert (fonte → tela).  
- [ ] Cobertura em dias compreensível (tooltip com fórmula).  
- [ ] Quantidade e, para owner, valor (R$).  
- [ ] Multi-filial com filtro de escopo TorqMind.  
- [ ] Endpoint quente &lt; 2s.  
- [ ] Sem expor custo/margem para gerente.

### 3.7 Riscos específicos deste módulo

- Média diária instável em posto com pouca venda → alerta falso (usar mínimo de dias / piso).  
- Tanque vs produto (mapeamento Xpert) precisa validação por filial.  
- Estoque de loja sem movimento recente → cobertura infinita ou N/A (mostrar “sem giro”).  
- Valor de estoque depende de custo confiável (mesmo cuidado do DRE).

---

## 4. Demais ideias — o que é, para que serve, como entraria no TorqMind

Abaixo está o que ficou abstrato. Linguagem de posto, não de “módulo de BI”.

### 4.1 Centro de alertas (o “sino” do sistema)

**O que é**  
Uma lista única no topo do app: “o que está vermelho agora”, cada item com texto humano e botão que abre a tela certa.

**Exemplos concretos (como no ExactView)**  
- “Diesel S500 — só **2,3 dias** de cobertura” → abre Estoque/Tanques  
- “Tanque 2 gasolina abaixo de 20%” → Tanques  
- “Vendas de ontem 18% abaixo da média da semana” → Vendas  
- “Cliente X estourou limite / título vencido” → Clientes / Financeiro  
- “Margem do etanol caiu X pontos” → Lucro (só owner)

**Como você usaria no TorqMind**  
Em vez de caçar problema em 6 menus, o dono abre o TorqMind e em 10 segundos sabe se precisa agir. É o “jornal da operação”.

**O que já temos**  
Insights / notificações parciais — mas não um **inbox operacional** tipado + severidade + deep link padronizado.

**Vale a pena?**  
Sim, **depois ou junto** do estoque: os melhores alertas nascem da cobertura de tanque/SKU. Sem estoque bom, o sino fica genérico.

**Decisão sugerida:** P0.5 — fazer na mesma épica do estoque (alertas de tanque/cobertura primeiro).

---

### 4.2 Simulador de preço / produtividade (“e se…?”)

**O que é**  
Uma calculadora em cima do histórico real do posto:

- “Quero faturar R$ X neste combustível”  
- “Aceito margem Y%”  
- “Se eu subir R$ 0,05 no litro, o que acontece com lucro e litros?”

O ExactView chama isso de **Simulador de Produtividade**: meta × preço × margem → litros faltantes / preço ideal / lucro projetado.

**Como você usaria no TorqMind**  
Reunião de preço com a bandeira / concorrência: em 1 minuto testa cenários **sem planilha**.  
Hoje a aba de oportunidades/repricing é um pedaço disso; o simulador é o pacote completo, pensado para o dono.

**O que já temos**  
`profit_management` / repricing / ANP — base de preço e margem. Falta a UX de **cenário interativo**.

**Vale a pena?**  
Alto para **owner**. Zero para gerente (não deve ver margem).  
Natural depois que estoque/cobertura estiver estável (ou em paralelo leve).

**Decisão sugerida:** P1 — após P0 estoque, ou sprint curto se preço for dor #1 da Verenka.

---

### 4.3 OLAP / Pivot (“monta o relatório que eu quiser”)

**O que é**  
Não é um dashboard fixo. É uma tela onde o usuário **arrasta**:

- Linhas: produto, grupo, cliente, filial…  
- Colunas: mês, semana, dia…  
- Valores: venda R$, litros, custo, margem, preço médio  

…e exporta CSV/Excel. No ExactView: `/comercial/pivot` + `POST /pivot/query`.

**Como você usaria no TorqMind**  
- Contador pediu “venda por grupo × mês”.  
- Dono quer “top clientes × semana”.  
- Sem abrir chamado para o time criar um gráfico novo.

**O que já temos**  
Muitos dashboards prontos (vendas, DRE, financeiro). **Não** temos explorador self-serve.

**Vale a pena?**  
Sim para redes maiores e para reduzir pedido de “mais um relatório”.  
É o item **mais caro** (cubo, limites, performance CH, ACL de margem).

**Decisão sugerida:** P2 — só depois de estoque + alertas (+ talvez simulador). Não é o primeiro “uau” do posto operacional.

---

### 4.4 Heatmap de hora × dia da semana

**O que é**  
Uma grade (domingo–sábado × horas) mostrando onde a pista esquenta. Clique no pico → detalhe.

**Como você usaria**  
Escala de frentista, promoção de conveniência no horário morto, comparar filiais.

**O que já temos**  
Vendas por hora + TV. O heatmap é outra **forma de ler** o mesmo dado.

**Decisão sugerida:** P2 polish dentro de `/sales` (baixo esforço relativo se a mart horária já existe).

---

### 4.5 Badge “dados atualizados às HH:MM”

**O que é**  
Um selo fixo: “STG/agent ok · última venda há 2 min” (ou “atrasado”).

**Como você usaria**  
Confiança: o dono sabe se está olhando realtime ou lixo velho (lição do dia das formas de pagamento).

**O que já temos**  
Freshness no CH / health; dá para subir isso de forma óbvia no shell.

**Decisão sugerida:** P1 barato — pode ir junto com qualquer entrega.

---

### 4.6 Comparativo pista × loja, cross-sell, frota, DRE “equilíbrio”

**O que é (bem curto)**  
- Pista × loja: quanto veio da bomba vs conveniência.  
- Cross-sell: quem abastece também leva X.  
- Frota: consumo por cliente frota.  
- Equilíbrio DRE: ponto de equilíbrio / estrutura de custo.

**Como entraria**  
Só se um cliente pedir com nome e ROI. TorqMind já tem DRE/lucro e vendas por grupo — parte disso é reempacotar, não inventar mundo novo.

**Decisão sugerida:** backlog, não roadmap próximo.

---

## 5. Roadmap proposto (refinado)

```text
Épica A (agora)     Estoque + Tanques + cobertura + qtd/valor
                    + alertas de tanque/cobertura (sino mínimo)

Épica B (seguinte)  Badge de freshness no app
                    Simulador de preço/produtividade (owner)

Épica C (depois)    OLAP/Pivot self-serve
                    Heatmap na Vendas

Backlog             Pista×loja, cross-sell, frota avançada, compras ERP
```

### Ordem de implementação sugerida (Épica A)

1. Inventário de fontes Xpert → STG → CH (tanques, movimentos, estoque SKU).  
2. DDL marts CH + publish.  
3. API + ACL.  
4. Tela Estoque (tanques primeiro, SKU depois).  
5. Alertas mínimos (tanque baixo / cobertura &lt; N dias).  
6. Prova fonte→tela em 2 filiais.

---

## 6. Fora do plano (consciente)

- Copiar visual “órbita / universo” do ExactView.  
- Trocar nosso foco realtime por BI só mensal.  
- Enfraquecer antifraude/caixa para “ficar parecido” com eles.  
- OLAP antes de estoque (você já escolheu a dor certa).

---

## 7. Perguntas abertas (para refinarmos juntos)

1. A primeira entrega deve ser **só combustível/tanques**, ou já na v1 incluir **loja (SKU + valor)**?  
2. Cobertura padrão: **7 dias**, **14**, ou configurável por filial?  
3. Quem vê a tela Estoque: **gerente + owner**, ou só owner no início?  
4. Valor de estoque: **custo**, **preço de venda**, ou os dois (owner)?  
5. Queremos o **sino de alertas** na mesma épica dos tanques (recomendado) ou tela estoque pura primeiro?  
6. Simulador: entra na sequência logo após estoque, ou fica para um trimestre?

---

## 8. Referência rápida das provas ExactView (2026-07-29)

| Endpoint | Para quê |
|----------|----------|
| `POST /api/auth/login` | `{email, senha}` → JWT `visualizador` |
| `GET /api/sistema/etl-status` | Freshness |
| `GET /api/notificacoes` | Alertas tipados + link |
| `GET /api/estoque/tanques` | Capacidade, estoque, dias_cobertura |
| `GET /api/estoque/sugestao-compra` | Litros a comprar p/ N dias |
| `GET /api/simulador/combustiveis` | Base do what-if |
| `GET /api/pivot/metadata` + `POST /api/pivot/query` | OLAP |
| `GET /api/dashboard/todos` | KPIs comerciais do mês |

Arquitetura TorqMind alvo (inalterada):

`Xpert → Agent → API ingest → PG STG → CDC → ClickHouse mart_rt → API → Web`

---

## 9. Próximo passo deste documento

Responder as perguntas da §7 (mesmo que informal). Com isso fechamos:

- escopo v1 da tela Estoque  
- se alertas entram na mesma entrega  
- se simulador é B ou C  

Aí sim viramos **spec de implementação** (rotas, registry ACL, DDL CH, critérios de aceite por PR).

---

## 10. Mapa profundo ExactView (vasculha API + rotas — 2026-07-29)

> Não temos acesso ao schema SQL interno deles. O “modelo de dados” abaixo é **inferido dos JSON** da API demo. Útil para copiar **contratos de tela** no estilo TorqMind (CH mart → API → FE), não para clonar tabelas 1:1.

### 10.1 Menus / rotas de front

| Área | Rotas |
|------|--------|
| Comercial | `/comercial`, `/comercial/comercial02`, `/comercial/comercial03`, `/comercial/comparativo`, `/comercial/compras`, `/comercial/estoque`, `/comercial/metas`, `/comercial/pivot`, `/comercial/simulador`, `/comercial/carta-frete` |
| Financeiro | `/financeiro`, `/financeiro/contas-a-pagar`, `/financeiro/contas-a-receber`, `/financeiro/dre`, `/financeiro/fluxo-caixa` |
| Contábil | `/contabil`, `/contabil/dre`, `/contabil/balanco`, `/contabil/disponibilidades`, `/contabil/indices`, `/contabil/ponto-equilibrio`, `/contabil/posicao-empresa`, `/contabil/resumo-patrimonio` |
| Operacional | `/operacional`, `/operacional/bombas` |

### 10.2 Catálogo de endpoints (JS bundle)

Autenticação: `POST /auth/login` (`email`, `senha`), `GET /auth/me`.  
Sistema: `GET /sistema/etl-status`, `GET /notificacoes`.

**Comercial / dashboard:**  
`/dashboard/todos|filtros|rankings|evolucao-mensal|vendas-por-ano|vendas-por-semana`  
`/comercial02/combustiveis|comparativo-filiais|curva-abc|projecao-mes`  
`/comercial03/heatmap|heatmap-hora|pico-detalhe|frota|frota-produtos|rentabilidade-cliente|combustiveis-semana`  
`/comparativo/*` (evolução, grupos, produtos, pista-loja, cross-sell)  
`/compras/painel`, `/compras/painel/resumo`  
`/estoque/tanques|resumo|sugestao-compra`  
`/simulador/combustiveis|capacidade|filtros`  
`/pivot/metadata`, `POST /pivot/query`

**Financeiro / títulos:**  
`/fluxo-caixa/dados`  
`/titulos/kpis|lista|devedores|por-periodo|por-parceiro|por-descricao` (tipo pagar/receber)

**Contábil:**  
`/dre/dados|equilibrio`  
`/balanco/dados|indices|cross-sell|parametros`  
`/disponibilidades/dados`, `/posicaoEmpresa/dados`, `/resumoPatrimonio/dados`

**Operacional:**  
`/bombas/todos` (KPIs + porDia + porBomba + porBico + tanques)

### 10.3 Formatos de payload (provas)

| Tela / API | Shape observado |
|------------|-----------------|
| Dashboard | `kpis{total_vendas,qtd,custo,lucro,margem_pct,preco_medio}` + `vendasMes[]` + `vendasGrupo[]` |
| Projeção mês | `realizado`, `projecao`, `media_diaria`, `dias_com_venda`, `pct_realizado` |
| Curva ABC | `produtos[{produto,faturamento,margem_pct,classe}]` + `resumo{A,B,C}` |
| Heatmap | `matriz[dow][hora]`, `max`, totais |
| Estoque resumo | `capacidade_total`, `litros_estoque`, `pct_estoque`, `custo_total_tanque` |
| Tanques | `capacidade`, `estoque`, `pct_ocupado`, `media_diaria`, **`dias_cobertura`** |
| Sugestão compra | `dias_alvo`, `itens[{estoque,media_diaria,necessidade,comprar}]` |
| Fluxo de caixa | séries diárias saldo/entrada/saída + categorias + previsto receber/pagar |
| DRE | lista plana `{conta,descricao,saldo,nivel,ano,mes}` (muito granular) |
| Bombas | KPIs + `porBomba` + `porBico` + volume aferido |
| Pivot | dims Tempo/Produto/Cliente; metrics venda/qtd/custo/margem/preço_médio |
| Notificações | `{tipo,severidade,titulo,mensagem,valor,link}` — tipos: estoque_critico, tanque_baixo, inadimplencia, margem_caindo, queda_vendas, limite_cliente |

### 10.4 Relações lógicas (inferidas)

```text
Filial
  ├─ Tanque ── Produto combustível (estoque litros, capacidade, cobertura)
  ├─ Bomba ── Bico ── Tanque / Produto
  ├─ Vendas ── Produto / Grupo / Cliente / Tempo
  ├─ Títulos (CR/CP) ── Parceiro / vencimento
  └─ Contas contábeis (DRE/balanço por nível)
```

Grão comercial típico: **mês + filial** (filtros `ano`/`mes`/`filial`).  
Grão operacional bombas: **intervalo de datas**.  
TorqMind deve preferir **dia/hora realtime** onde já somos fortes, e usar mês só onde fizer sentido (DRE, projeção).

### 10.5 O que “copiar no nosso estilo” (além do P0 estoque)

Organização que sentimos falta no TorqMind (UX/IA, não só feature):

1. **IA de menus por domínio** — Comercial / Financeiro / Contábil / Operacional (nós misturamos muito em poucas rotas densas).  
2. **Projeção do mês** (`realizado` vs `projecao`) — encaixa em Dashboard/Vendas.  
3. **Curva ABC** com classe A/B/C — reforça `/sales`.  
4. **Heatmap** — reforça vendas por hora.  
5. **Bombas/bicos** como visão operacional (se houver fonte Xpert confiável).  
6. **Fluxo de caixa dia a dia** com previsto — reforça `/finance`.  
7. **Inbox de alertas** com deep link.  
8. **Pivot** self-serve (P2).  
9. **Simulador** (P1).  
10. **Toggle tema no topo** — já em implementação nesta wave.

Diferenciais TorqMind a **não diluir**: antifraude, caixa/turno realtime, agent Xpert, gestão de lucro/solvência.

### 10.6 Limitações desta vasculha

- Sem browser pixel-perfect (sem screenshots de UI).  
- Sem DDL/Postgres deles — só contratos HTTP.  
- Demo = 1 filial `DEMO` / `99999`, perfil `visualizador`.  
- Alguns endpoints contábeis são pesados (DRE com milhares de linhas) — no TorqMind preferir mart agregada.
