# TorqMind — Padrões Canônicos de UI (Auditoria Frontend)

**Escopo:** `apps/web/**` (somente leitura; sem implementação nesta auditoria).  
**Data:** 2026-07-17  
**Fonte de verdade:** código em `apps/web/app` + `globals.css`.  
**Mapa de produto:** `CODEX_TORQMIND_MAP.md` (páginas BI via `/bi/*` + `lib/api.ts`).

---

## 1. Veredito — padrão oficial encontrado

O frontend de produto (BI) já tem um **núcleo consolidado**:

| Camada | Padrão oficial (evidência) |
|--------|----------------------------|
| Shell / escopo | `AppNav` + URL (`dt_ini`/`dt_fim`/filiais) + `useBiScopeData` / `scope-runtime` |
| Loading de troca de escopo | `ScopeTransitionState` + `Skeleton` |
| Empty de painel | `EmptyState` (title + detail) |
| Card base | `.card` em `globals.css` |
| KPI produto (maioria) | `.card.kpi` (label/value) |
| Tabela | `.tableScroll` > `table.table` [`.compact`] |
| Gráfico | `recharts` dentro de `.chartCard` / `.chartWrap` |
| Dropdown flutuante | `PortalDropdown` (portal + fixed) |
| Filtro local de painel | `.profitFilterBar` (input/select) |
| Copy de cobertura | `lib/reading-copy.mjs` (traduz status técnicos → português operacional) |
| Gate de copy | `lib/ui-copy-quality.test.mjs` (bloqueia `mart`, `snapshot`, `recorte`, etc.) |

Há um **segundo dialecto** para Gestão de Lucro / ANP (`.profitKpi*`, `.anp*`) e um **terceiro** para Plataforma (`.platformInlineFilters`, `.platformStat`). Não são bugs; são famílias paralelas.

---

## 2. Inventário — componentes compartilhados

### 2.1 `apps/web/app/components/` (shell)

| Componente | Local | Telas que usam | Status |
|------------|-------|----------------|--------|
| `AppNav` | `components/AppNav.tsx` | dashboard, sales, cash, customers, finance, fraud, goals, pricing, profit-management, security, settings | **Canônico** — filtro global empresa/filial/período |
| `PlatformShell` | `components/PlatformShell.tsx` | todas `platform/**` | **Canônico** backoffice |
| `EnvBanner` | `components/EnvBanner.tsx` | `layout.tsx` | **Canônico** (ambiente não-prod) |
| `BrandingApplier` | `components/BrandingApplier.tsx` | `layout.tsx` | Infra visual |
| `BrandingEditor` | `components/BrandingEditor.tsx` | `platform/companies/[tenantId]` | Isolado admin |

### 2.2 `apps/web/app/components/ui/`

| Componente | Local | Telas | Status |
|------------|-------|-------|--------|
| `EmptyState` | `ui/EmptyState.tsx` | dashboard, sales (+ABC), cash, customers, finance (+budget/cheques), fraud, goals (+tabs), profit-management | **Canônico** produto |
| `Skeleton` | `ui/Skeleton.tsx` | dashboard; via `ScopeTransitionState` | **Canônico** loading |
| `ScopeTransitionState` | `ui/ScopeTransitionState.tsx` | dashboard, sales, cash, customers, finance, fraud, goals, profit-management | **Canônico** transição de escopo |
| `PortalDropdown` | `ui/PortalDropdown.tsx` | `SalesAbcSection`, `AnpCompliancePanel`, `SolvenciaDetalhada` | **Canônico** de posicionamento; conteúdo **não** unificado |
| `HeroMoneyCard` | `ui/HeroMoneyCard.tsx` | só `dashboard/page.tsx` | Semi-canônico (só home) |
| `RiskBadge` | `ui/RiskBadge.tsx` | dashboard; interno a `ActionCard` | Canônico no dashboard |
| `ActionCard` | `ui/ActionCard.tsx` | **nenhum import externo** | Órfão / preparado, não consolidado |
| `EvidenceChips` | `ui/EvidenceChips.tsx` | só via `ActionCard` | Órfão em cascata |
| `RadarPanel` | `ui/RadarPanel.tsx` | **nenhum uso** | Órfão |
| `ReadingStatusBanner` | `ui/ReadingStatusBanner.tsx` | **nenhum uso** | Órfão (CSS existe) |
| `ErrorBoundary` | `ui/ErrorBoundary.tsx` | **nenhum uso** | Órfão |

### 2.3 Classes CSS canônicas (`globals.css`)

| Classe | Uso observado | Status |
|--------|---------------|--------|
| `.card` | Quase todas as páginas produto/plataforma | Canônico |
| `.kpi` / `.card.kpi` | sales, finance, customers, goals, cash, dashboard | **Canônico KPI produto** |
| `.kpiStrip` / `.heroCard*` | dashboard home | Específico home |
| `.chartCard` / `.chartWrap` | páginas com recharts | Canônico gráfico |
| `.table` / `.tableScroll` / `.table.compact` | sales, cash, customers, finance, fraud, goals… | **Canônico tabela** |
| `.skeleton` | shimmer global | Canônico |
| `.profitFilterBar` | profit-management, fraud | **Canônico filtro local de painel** |
| `.profitKpiStrip` / `.profitKpiCard*` | profit-management, ANP | Família Lucro (paralela a `.kpi`) |
| `.anpFilterRow` / `.anpProductMenu*` / `.anpTable*` | `AnpCompliancePanel` | Isolado ANP |
| `.platformInlineFilters` / `.platformStat*` | platform companies/users/home | Família Plataforma |
| `.fuelCardMobile` / `.compCardMobile` | pricing | Isolado pricing mobile |
| `.actionCard` / `.radarPanel` / `.evidenceChip` | CSS presente; componentes pouco/não usados | Latente |

### 2.4 Por categoria

#### Filtros

| Padrão | Onde | Consolidado? |
|--------|------|--------------|
| Escopo global (empresa/filiais/datas URL) | `AppNav` | Sim — produto BI |
| `.profitFilterBar` (select/input locais) | `profit-management/page.tsx`, `fraud/page.tsx` (créditos + troca pgto) | Sim entre Lucro e Antifraude |
| `.anpFilterRow` + datas locais | `AnpCompliancePanel` (sobrescreve período do panel) | Isolado |
| `.platformInlineFilters` | `platform/companies`, `platform/users` | Canônico plataforma |
| Datas/tabs locais | `pricing/page.tsx`, forms platform/audit | Isolado por tela |
| Chips/botões de filial | `customers/page.tsx` (inadimplência) | Isolado (não usa PortalDropdown) |

#### Dropdowns / multiselect

| Padrão | Onde | Consolidado? |
|--------|------|--------------|
| Checklist filiais no sidebar | `AppNav` | Canônico escopo |
| `PortalDropdown` + checkboxes + “Todos…” (estilos **inline**) | `sales/SalesAbcSection.tsx` | Referência comportamental ABC |
| `PortalDropdown` + classes `.anpProductMenu*` | `AnpCompliancePanel.tsx` | Mesmo mecanismo, skin diferente |
| `PortalDropdown` hint/editor | `SolvenciaDetalhada.tsx` (estilos inline) | Uso pontual, não filtro |

#### Tabelas

| Padrão | Onde | Comportamento |
|--------|------|---------------|
| `.tableScroll` > `.table` | Maioria BI | Scroll X; empty via `EmptyState` **antes** da tabela |
| `.anpTableScroll` > `.anpTable` | ANP | `min-width` alto; scroll dedicado |
| `<table style={{…}}>` inline | trechos de `profit-management/page.tsx` (produtos/repricing) | Fora do `.table`; scroll via card |
| Plataforma | `platform/**` | `.table` + empty em `<td colSpan>` frequentemente |
| Sort | customers (botões), SalesAbc (botões) | Não há sort por clique em `<th>` |

#### KPI cards

| Padrão | Onde |
|--------|------|
| `.card.kpi` | Padrão majoritário BI |
| `HeroMoneyCard` / `.heroCard` | Só dashboard |
| `.profitKpiCard` | Lucro + ANP |
| `.platformStat` | Platform home |
| Cards mobile pricing | `fuelCardMobile` etc. |

#### Gráficos

- Lib: **recharts** (`BarChart`, `AreaChart`, `PieChart`, `ComposedChart`).
- Telas: `sales`, `SalesAbcSection`, `customers`, `cash`, `finance`, `fraud`, `profit-management`.
- Wrapper CSS: `.chartCard` / `.chartWrap`.

#### Empty / skeleton / modal / banner

| Tipo | Padrão | Exceções |
|------|--------|----------|
| Empty | `EmptyState` | pricing/TV/security: `<p className="muted">…</p>` |
| Skeleton | `Skeleton` + `ScopeTransitionState` | loading textual (“Carregando…”) em várias tabs |
| Modal | **Não há** modal/`role="dialog"` consolidado | Popovers = `PortalDropdown` |
| Banner ambiente | `EnvBanner` | — |
| Banner leitura | `ReadingStatusBanner` **não usado**; freshness via `reading-copy` em trechos (finance/customers) | — |

---

## 3. Inconsistências (evidência)

1. **Multiselect duplicado:** ABC (`SalesAbcSection`, estilos inline) vs ANP (`.anpProductMenu*`) — mesmo `PortalDropdown`, skins diferentes.
2. **Filtro local:** `.profitFilterBar` (fraud/lucro) vs `.anpFilterRow` vs `.platformInlineFilters` vs chips em customers — mesmo papel (“refinar painel”), quatro skins.
3. **KPI:** `.card.kpi` vs `.profitKpiCard` vs `.platformStat` vs `HeroMoneyCard` — quatro famílias visuais.
4. **Tabela:** canônica `.tableScroll/.table` vs ANP `.anpTable*` vs tabelas inline em lucro.
5. **Empty:** `EmptyState` vs `muted` inline (pricing, TV, platform colSpan, vários “Carregando…”).
6. **Componentes órfãos:** `ActionCard`, `RadarPanel`, `ReadingStatusBanner`, `ErrorBoundary` (+ CSS associado) sem uso de página.
7. **Copy proibida ainda em UI:** `fraud/page.tsx` — EmptyState `"Sem injeções de crédito no recorte."` (gate `ui-copy-quality` proíbe `recorte`).
8. **Sort:** só botões externos (customers/ABC); sem padrão de header clicável.

---

## 4. Linguagem técnica visível ao usuário

### 4.1 Achados em strings de UI (evidência)

| Termo | Onde | String / contexto |
|-------|------|-------------------|
| **ETL** | `platform/companies/[tenantId]/page.tsx` | “…sem o **ETL** reverter essas alterações.” (admin plataforma) |
| **pipeline** | `finance/page.tsx` | EmptyState detail: “…em correção no **pipeline**.” |
| **recorte** | `fraud/page.tsx` | “Sem injeções de crédito no **recorte**.” (proibido pelo gate) |
| **publicada** | `lib/reading-copy.mjs` → `describeLastSync` | `"Base publicada em …"` — helper; teste `sync-status-ui-source.test.mjs` garante que `describeLastSync(` **não** está plugado nas páginas hoje |
| **postos** (legado lexical) | `customers/page.tsx` | label UI “Todos os **postos**” (enquanto produto padronizou “Filial” em antifraude) |

### 4.2 Ausentes em UI (bom)

Não encontrados em strings visíveis de páginas: `mart`, `DW`, `CDC`, `Debezium`, `ClickHouse`, `STG`, `watermark`, `fact_`, `ingest`.  
`snapshot` aparece em código/API (`churn_snapshot`, `_snapshot_cache`) e é traduzido por `reading-copy` / bloqueado pelo gate de copy.

### 4.3 Política já codificada

`lib/ui-copy-quality.test.mjs` proíbe (entre outros): `mart`, `snapshot`, `recorte`, `Frescor operacional`, `publicação analítica`, `trilho operacional`, `FORMA_*`, `1970`, label visual `Platform` (deve ser “Plataforma”).

`lib/reading-copy.mjs` é o **padrão oficial de copy de frescor/cobertura** (“Base pronta…”, “Atualizado em…”, “Em atualização…”).

---

## 5. Componentes canônicos (lista oficial encontrada)

Para novas telas BI, o padrão **já existente** a seguir é:

1. `AppNav` + escopo URL + `useBiScopeData` / `ScopeTransitionState`
2. Painéis em `.card`
3. KPIs em `.card.kpi` (exceto família Lucro, que já usa `.profitKpi*`)
4. Tabelas em `.tableScroll` > `table.table`
5. Gráficos recharts em `.chartCard`/`.chartWrap`
6. Empty via `EmptyState`
7. Filtros locais de painel via `.profitFilterBar` quando forem select/input simples
8. Menus flutuantes via `PortalDropdown`
9. Copy de status via `reading-copy.mjs` (nunca expor mart/snapshot/CDC)

**Exceções legítimas já no código:**

- Lucro/ANP: `.profitKpi*`, `.anp*`
- Plataforma: `PlatformShell` + `.platform*`
- Pricing mobile: cards próprios
- TV/kiosk: layout próprio, empty muted

---

## 6. Recomendações de unificação (só com base no que existe)

Ordenadas por impacto / evidência; **não** inventar design system novo.

1. **Multiselect:** extrair o conteúdo de ABC (`SalesAbcSection`) como referência de comportamento e fazer ANP reutilizar a mesma estrutura de menu (manter só classes se tipografia ANP exigir) — hoje são clones comportamentais.
2. **Filtro local:** preferir `.profitFilterBar` para novos filtros de painel BI; não criar quarta família CSS.
3. **Tabela:** migrar trechos inline de `profit-management` para `.table`/`.tableScroll`; manter `.anpTable*` só se `min-width`/print ANP continuar necessário.
4. **KPI:** novos KPIs BI → `.card.kpi`; Lucro permanece em `.profitKpi*` até decisão explícita de merge visual.
5. **Empty/loading:** trocar `<p className="muted">` de empty em pricing (e similares) por `EmptyState`; loading de escopo → `ScopeTransitionState`/`Skeleton`.
6. **Órfãos:** decidir destino de `ActionCard`/`RadarPanel`/`ReadingStatusBanner`/`ErrorBoundary` (adotar no dashboard ou remover em chore futuro) — hoje geram falsa sensação de padrão.
7. **Copy:** corrigir `recorte` em fraud; trocar `pipeline` em finance por linguagem operacional (“atualização dos valores”); avaliar se “ETL” na plataforma fica só para `platform_master` ou vira “sincronização automática”.
8. **Lexical filial:** alinhar “postos” em customers com “Filial” (já usado em fraud).

---

## 7. Mapa rápido de rotas auditadas

Produto: `/`, `/dashboard`, `/sales`, `/cash`, `/customers`, `/finance`, `/fraud`, `/goals`, `/pricing`, `/profit-management`, `/settings`, `/security`, `/scope`, `/tv`, `/tv/sales-hourly`, `/tv/sales-ranking`, auth pages.  
Plataforma: `/platform` + companies/users/notifications/channels/contracts/receivables/channel-payables/audit.

---

## 8. Riscos remanescentes (auditoria)

- Documento descreve o **estado atual** dos fontes; não valida runtime/homolog.
- `describeLastSync` / “Base publicada” existem no helper mas não estão ligados às páginas (teste explícito); risco futuro se alguém plugá-los sem revisar copy.
- Gate `ui-copy-quality` e a string `recorte` em fraud estão em tensão — gate pode falhar em CI se a regra estiver ativa no pipeline de web.
)
