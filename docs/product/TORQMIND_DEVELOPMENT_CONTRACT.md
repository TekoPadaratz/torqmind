# TorqMind — Contrato Oficial de Desenvolvimento

**Status:** normativo e permanente  
**Público:** agentes (Cursor/Codex), developers, revisores  
**Fontes (somente estas):**  
- `docs/product/TORQMIND_UI_CANONICAL_PATTERNS.md`  
- `AGENTS.md`  
- `.cursor/rules/*`  
- código em `apps/web` (evidência da auditoria)  

**Não é:** redesign, wishlist, design system novo, guia acadêmico.  
**É:** o que já existe e deve ser reutilizado. Criar fora daqui é exceção justificada.

Documentos irmãos: `TORQMIND_UI_CANONICAL_PATTERNS.md` (inventário), `CODEX_TORQMIND_MAP.md` (mapa técnico), `AGENTS.md` (operação/segurança/dados).

---

# 1. Princípios Fundamentais

1. O TorqMind **já possui** um design system consolidado no código (`apps/web/app/components`, `globals.css`, hooks de escopo). Não inventar outro.
2. Toda implementação UI **segue** os padrões canônicos deste contrato e da auditoria.
3. **Reutilizar é obrigatório.** Abrir arquivo novo de componente/CSS só quando não existir equivalente comprovado.
4. Criação de componente/classe/família visual nova é **exceção** e exige justificativa no PR/comentário técnico.
5. Diffs cirúrgicos. Sem big-bang. Sem placeholders. Production-ready (`00-torqmind-global`).
6. Frontend **nunca** é fronteira de segurança; escopo/permissão na API (`AGENTS.md`).
7. Economia de tokens: procurar no código antes de reinventar (`.cursor/rules/05-economia-tokens.mdc`).

---

# 2. Processo Obrigatório Antes de Implementar

Antes de escrever UI:

| # | Ação | Critério de passagem |
|---|------|----------------------|
| 1 | Procurar equivalente | `rg`/leitura em `apps/web/app/components`, `globals.css`, tela irmã |
| 2 | Validar padrão da família | BI → núcleo canônico; Lucro/ANP → `.profit*`/`.anp*`; Plataforma → `PlatformShell`/`.platform*` |
| 3 | Reutilizar | Importar componente ou classe existente; copiar estrutura da tela referência |
| 4 | Criar só se inexistente | Nenhum equivalente no inventário da auditoria |
| 5 | Justificar | PR ou comentário: o que buscou, por que não reaproveitou |

Proibido pular para “vou criar um `FilterBarV2` / `DataTable` / modal novo” sem os passos 1–3.

---

# 3. Componentes Canônicos Oficiais

Paths relativos a `apps/web/app/`.

| Canônico | Path / definição | Quando usar |
|----------|------------------|-------------|
| **AppNav** | `components/AppNav.tsx` | Toda tela produto BI: shell + escopo global (empresa/filiais/período na URL) |
| **ScopeTransitionState** | `components/ui/ScopeTransitionState.tsx` | Troca de escopo / leitura indisponível enquanto atualiza |
| **Skeleton** | `components/ui/Skeleton.tsx` | Blocos de loading (também via ScopeTransitionState); dashboard |
| **EmptyState** | `components/ui/EmptyState.tsx` | Painel/tabela/gráfico sem dados (`title` + `detail?`) |
| **PortalDropdown** | `components/ui/PortalDropdown.tsx` | Menu flutuante (multiselect, hint, popover). Âncora + portal |
| **`.card`** | `globals.css` | Container de painel / bloco |
| **`.card.kpi`** | `globals.css` | KPI padrão BI (label + value) |
| **`.table`** | `globals.css` | Tabela de dados |
| **`.tableScroll`** | `globals.css` | Wrapper com scroll horizontal da tabela |
| **`.chartCard`** | `globals.css` | Card que contém gráfico |
| **`.chartWrap`** | `globals.css` | Área de altura do chart |
| **`.profitFilterBar`** | `globals.css` | Filtro local de painel BI (input/select) |

### Famílias paralelas (não misturar nem inventar quarta)

| Família | Usar quando | Não usar em |
|---------|-------------|-------------|
| Lucro/ANP: `.profitKpi*`, `.anp*` | `profit-management`, ANP | sales/cash/finance genérico |
| Plataforma: `PlatformShell`, `.platformInlineFilters`, `.platformStat` | `platform/**` | telas tenant BI |
| Home: `HeroMoneyCard`, `.kpiStrip` | só dashboard hero | novos módulos |
| Pricing mobile: `.fuelCardMobile` etc. | pricing | resto do produto |

### Semi-canônicos (só no contexto já existente)

- `HeroMoneyCard` — só home (`dashboard/page.tsx`)
- `RiskBadge` — dashboard (e interno a ActionCard órfão)
- `EnvBanner` — layout (ambiente não-prod)
- Copy de frescor: `lib/reading-copy.mjs` — status de base/atualização para o usuário

### Referência de multiselect (comportamento)

- `sales/SalesAbcSection.tsx` — `PortalDropdown` + checkboxes + “Todos…”
- `profit-management/AnpCompliancePanel.tsx` — mesmo mecanismo; **não** criar terceiro skin

---

# 4. Padrões de Filtros

| Tipo | Padrão oficial | Quando |
|------|----------------|--------|
| **Escopo global** | `AppNav` + params URL (`dt_ini`/`dt_fim`/filiais) + `useBiScopeData` / `scope-runtime` | Empresa, filiais, período da tela BI |
| **Filtro local (select/input)** | `.profitFilterBar` | Refinar um painel sem alterar escopo global (ex.: fraud, lucro) |
| **Multiselect em menu** | `PortalDropdown` + checklist (modelo ABC; ANP se já no domínio) | Produtos/grupos com “todos” |
| **Seleção única** | `<select>` dentro de `.profitFilterBar` (ou equivalente já na tela) | Status, risco, forma, ordenação simples |
| **Datas globais** | AppNav / URL | Período da página |
| **Datas locais de painel** | Só onde já existe (ex.: ANP sobrescreve período do panel; pricing/tabs) | Não criar datas locais se o global basta |

**Plataforma:** `.platformInlineFilters` — não usar `.profitFilterBar` no backoffice.

**Proibido:** nova família CSS de filtro; chips/botões ad hoc se `.profitFilterBar` ou PortalDropdown resolvem.

---

# 5. Padrões de KPIs / Totalizadores

| Usar | Quando |
|------|--------|
| **`.card.kpi`** | Default BI (sales, cash, finance, customers, goals, dashboard secundário) |
| **`.profitKpi*`** | Somente Gestão de Lucro / ANP (já no dialecto) |
| **`HeroMoneyCard`** | Somente destaque monetário da home |
| **`.platformStat`** | Somente home Plataforma |

**Não criar** novos tipos de card KPI (sem nova classe, sem lib de “StatCard”).  
Se a tela é Lucro → `.profitKpi*`. Senão BI → `.card.kpi`.

### Tipografia responsiva (contrato de resolução)

- **Floor:** 720p (viewport ≈ 1280×720). Abaixo disso ainda funciona, mas não é o alvo.
- **Preferência:** notebook HD (1366–1920).
- Tokens em `:root` (`globals.css`): `--font-kpi-value`, `--font-kpi-label`, `--font-hero-value` via `clamp(...)`.
- Totalizadores **nunca** usam `font-size` fixo que estoure com valor alto (R$ 1.234.567,89).
- Em resoluções maiores o `clamp` sobe até o teto; em menores encolhe sem encavalar cards.
- Coluna de valor em DRE (`.dreTable td:last-child`) também escala com o mesmo princípio.
- Proibido overrides de media query que **aumentem** a fonte do KPI em breakpoints intermediários (isso reintroduz o encavalamento).

---

# 5b. Controle de acesso por menu e painel (ACL)

Fonte canônica: `apps/api/app/permissions.py` → `SCREEN_REGISTRY`.

| Conceito | Chave | Onde aparece |
|----------|-------|----------------|
| **Menu** | `profit_management` | AppNav / `PRODUCT_LINKS` |
| **Painel/aba** | `profit_management.overview` | Abas dentro da tela + `require_screen` no endpoint |

Regras:

1. Toda **nova aba ou painel** deve ser registrada no `SCREEN_REGISTRY` com `parent` = menu.
2. Todo endpoint do painel usa `require_screen("menu.painel")` (não só o menu).
3. Cadastro de usuário (`/platform/users`): árvore menu → painéis; desmarcar o menu **remove todos** os painéis; marcar o menu marca todos; painéis individuais podem ser desmarcados.
4. **Default:** `platform_master` / `platform_admin` / `product_global` / `channel_admin` / `tenant_admin` → acesso total via `ROLE_DEFAULT_SCREENS` (sem checkbox). `tenant_manager` / `tenant_viewer` novos ou com lista vazia na criação → **todo o produto** (menus + painéis). `tenant_kiosk` → telas TV.
5. Legado: se o usuário só tem o menu no DB, o runtime **expande** para todos os painéis (`expand_screen_permissions`).
6. Nav continua olhando só a chave do menu.
7. Solvência/ANP: além do painel, mantém `can_view_sensitive_financials` (roles financeiras).
8. Endpoint de árvore: `GET /platform/screen-registry`.
9. FE: `canAccessScreenKey` / `lib/screen-permissions.ts`.

**Obrigatório em PRs de feature com aba nova:** registry + require_screen + checkbox na árvore (automático se registry OK) + filtro de aba no FE.

---

# 6. Padrões de Tabelas

Estrutura canônica:

```html
<div class="tableScroll">
  <table class="table"> … </table>
</div>
```

Opcional: `.table.compact` quando a tela já usa.

| Aspecto | Norma |
|---------|--------|
| **Empty** | `EmptyState` **antes**/no lugar da tabela — não inventar empty só com CSS |
| **Loading (escopo)** | `ScopeTransitionState` / `Skeleton` |
| **Responsivo** | confiar em `.tableScroll` (overflow-x); não esmagar colunas |
| **Sort** | botões externos (padrão customers / Curva ABC); não inventar sort por `<th>` sem precedente |
| **ANP** | `.anpTableScroll` / `.anpTable` só no painel ANP (print/min-width) |
| **Proibido** | `<table style={{ width:"100%", …}}>` novo fora do padrão; copiar só se mantendo dívida existente |

Plataforma pode usar `td colSpan` para empty — não importar esse hábito para BI produto.

---

# 7. Padrões de Gráficos

| Norma | Detalhe |
|-------|---------|
| Lib | **recharts** apenas (já no produto) |
| Container | `.chartCard` + `.chartWrap` |
| Tipos já usados | `BarChart`, `AreaChart`, `PieChart`, `ComposedChart` |
| Proibido | nova lib de chart; wrapper CSS novo; chart sem card |

---

# 8. Padrões de Loading

| Situação | Usar |
|----------|------|
| Troca de empresa/filial/período / base indisponível | `ScopeTransitionState` |
| Placeholder de bloco/métrica | `Skeleton` |
| Tabs/forms pontuais | texto “Carregando…” só onde a tela irmã já faz — preferir Skeleton quando possível |

**Proibido:** spinners improvisados, `Loading…` em HTML cru sem padrão, loaders de lib nova, inventar `SkeletonCard` paralelo ao `Skeleton` existente.

---

# 9. Padrões de Empty State

**Obrigatório em painéis BI:** `EmptyState` com título claro + detail opcional em linguagem de negócio.

**Incorreto (evidência na auditoria — não copiar):**

- `<p className="muted">Nenhum…</p>` (pricing, TV, trechos avulsos)
- Empty só em `<td colSpan>` em telas produto BI
- Título com jargão (`recorte`, `pipeline`, códigos técnicos)

Referência correta: sales, cash, customers, finance, fraud, goals, profit-management.

---

# 10. Linguagem Oficial do Produto

**Público:** dono de posto, gerente, supervisor, financeiro (e `platform_master` no backoffice — ainda assim evitar jargão de engenharia quando possível).

**Copy de status/frescor:** `lib/reading-copy.mjs` (“Base pronta…”, “Atualizado em…”, “Em atualização…”).  
**Gate:** `lib/ui-copy-quality.test.mjs` — deve continuar passando.

### Termos proibidos na UI (lista normativa)

| Proibido | Substituir por (exemplos já no produto/gate) |
|----------|-----------------------------------------------|
| ETL | sincronização / atualização automática |
| Pipeline | atualização dos valores / ainda estamos atualizando |
| Snapshot | base / leitura / dados do período |
| Mart | (não expor; falar do dado: vendas, inadimplência…) |
| DW | — |
| CDC | — |
| Debezium | — |
| STG | — |
| Fact / `fact_` | — |
| Watermark | — |
| ClickHouse | — |
| `recorte` | período / período selecionado |
| Frescor operacional | Atualizado em… / Em atualização |
| Publicação analítica | Base pronta / Dados disponíveis até… |
| Trilho operacional | — |
| `FORMA_*` | nome da forma de pagamento |
| `01/01/1970` / ano 1970 | nunca exibir |
| Label visual `Platform` | **Plataforma** |
| “postos” como label de filial | **Filial** (padrão antifraude) |

Interno (código, logs, SQL, comentários de engenharia): termos técnicos OK.  
UI e mensagens ao usuário: **nunca**.

Margem/lucro/CMV/custo: nunca para gerente/vendedor (`AGENTS.md`).

---

# 11. Regras de Reutilização

Checklist **obrigatório** antes de criar qualquer um destes:

| Artefato | Procurar primeiro em |
|----------|----------------------|
| Dropdown / popover | `PortalDropdown`; ABC / ANP / Solvência |
| Multiselect | `SalesAbcSection` → depois ANP |
| Tabela | `.tableScroll` + `.table` |
| Card / painel | `.card` |
| KPI | `.card.kpi` ou `.profitKpi*` se Lucro/ANP |
| Filtro local | `.profitFilterBar` |
| Filtro global | `AppNav` (não duplicar seletor de período/filial na página) |
| Gráfico | recharts + `.chartCard`/`.chartWrap` |
| Empty | `EmptyState` |
| Loading escopo | `ScopeTransitionState` + `Skeleton` |
| Modal | **Não há modal canônico.** Preferir `PortalDropdown` / fluxo inline. Criar `role="dialog"` só com justificativa forte |
| Banner de leitura | `reading-copy.mjs` (não inventar banner; `ReadingStatusBanner` é órfão) |

Se não achou: documentar busca + justificativa. Só então criar o mínimo.

---

# 12. Componentes Órfãos

Existentes, **sem uso de página** na auditoria. Registrar apenas — **não** remover neste contrato:

| Componente | Path |
|------------|------|
| ActionCard | `components/ui/ActionCard.tsx` (+ EvidenceChips) |
| RadarPanel | `components/ui/RadarPanel.tsx` |
| ReadingStatusBanner | `components/ui/ReadingStatusBanner.tsx` |
| ErrorBoundary | `components/ui/ErrorBoundary.tsx` |

Não tratar como padrão canônico para features novas até haver adoção explícita em tela.

---

# 13. Checklist Obrigatório para Novas Features

Antes de declarar pronto / PASS de UI:

- [ ] Reutilizei canônicos (seção 3) — sem componente/CSS novo sem justificativa
- [ ] Escopo: `AppNav` + hooks; loading: `ScopeTransitionState`/`Skeleton`
- [ ] Filtros: global vs `.profitFilterBar` / PortalDropdown corretos (seção 4)
- [ ] KPI: família certa (seção 5) — sem quarto visual
- [ ] Tabelas: `.tableScroll` > `.table`; empty com `EmptyState`
- [ ] Gráficos: só recharts + `.chartCard`/`.chartWrap`
- [ ] Sem empty/loading improvisado (`muted` solto, spinner novo)
- [ ] Linguagem: zero termos da lista proibida; “Filial”; copy via `reading-copy` se frescor
- [ ] Roles: sem margem/lucro/custo para gerente/vendedor
- [ ] Permissão real na API (frontend não é segurança)
- [ ] Consistência visual com tela irmã do mesmo módulo
- [ ] Responsivo: scroll horizontal de tabela, não colunas esmagadas
- [ ] Acessibilidade mínima: botões/labels; PortalDropdown fecha Esc/fora; empty legível
- [ ] Testes: `apps/web` `npm test` (inclui gate de copy quando aplicável)
- [ ] Entrega: arquivos alterados + riscos (`00-torqmind-global`)

**Violação deste contrato = débito técnico consciente.** Só aceitável com justificativa explícita no PR.

---

## Hierarquia de verdade (conflito)

1. Este contrato + `TORQMIND_UI_CANONICAL_PATTERNS.md` (UI)  
2. `AGENTS.md` + rules de segurança/tenancy/dados  
3. Código em produção nas telas canônicas  

Se rule antiga (ex. nomes genéricos em `.cursor/rules/04-frontend-nextjs.mdc`) divergir da auditoria (`Skeleton` vs “SkeletonCard”), **prevalece a auditoria + este contrato**.
)
