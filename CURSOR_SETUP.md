# ⚙️ Configuração do Cursor para o TorqMind

## Estrutura real dos arquivos

```
torqmind/
├── .cursor/
│   └── rules/
│       ├── 00-torqmind-global.mdc              → alwaysApply — identidade, entrega, proteções
│       ├── 01-security-tenancy.mdc             → alwaysApply — multi-tenant / dados / ACL
│       ├── 02-etl-database.mdc                 → glob (ETL/SQL/repos) — PG + pipeline + CH cutover
│       ├── 03-backend-fastapi.mdc              → glob apps/api — FastAPI
│       ├── 04-frontend-nextjs.mdc              → glob apps/web — Next.js
│       ├── 05-economia-tokens.mdc              → alwaysApply — resposta enxuta
│       ├── 06-clickhouse-bi-reads.mdc          → alwaysApply — hot path BI = ClickHouse
│       ├── 06-acl-painel-tipografia.mdc        → glob ACL/painéis — menu + aba
│       ├── 07-documento-nota-fiscal.mdc        → glob web/api/sql — DOCUMENTO = NF
│       ├── 08-grids-colunas-ordenacao.mdc      → alwaysApply — contrato de grids
│       ├── 09-agent-version.mdc                → glob agent/build — bump __version__
│       ├── 10-docker-isolation.mdc             → alwaysApply — compose seguro
│       ├── 11-migrations-safety.mdc            → glob migrations — sem reset destrutivo
│       ├── torqmind-principal-architect.agent.mdc
│       ├── torqmind-codigo.agent.mdc
│       ├── torqmind-ssh-producao.agent.mdc
│       ├── torqmind-homologacao.agent.mdc
│       └── torqmind-git-release.agent.mdc
├── .cursorignore
├── AGENTS.md                                   → autoridade operacional #1
├── CODEX_TORQMIND_MAP.md                       → mapa técnico vivo #2
├── PROJECT_CONTEXT.md                          → contexto resumido do Agent
└── docs/product/TORQMIND_DEVELOPMENT_CONTRACT.md
```

**Não existe** `.cursor/agents/` nem `.github/agents/` neste repositório.
As personas atuais são os arquivos `*.agent.mdc` em `.cursor/rules/`.

## Tipos de regra

| Tipo | Comportamento |
|---|---|
| `alwaysApply: true` | Injeta em toda sessão Cursor |
| `alwaysApply: false` + `globs` | Aplica quando arquivos casam o glob |
| `*.agent.mdc` | Persona (papel, escopo, limites de decisão) — não duplicar a arquitetura inteira |

alwaysApply atuais (revisar periodicamente): `00`, `01`, `05`, `06-clickhouse`, `08`, `10`.
Regras sensíveis a escopo: `02`, `03`, `04`, `06-acl`, `07`, `09`, `11`, personas.

## Ambiente / fontes de verdade

1. `AGENTS.md` — segurança, dados, deploy, PASS  
2. `CODEX_TORQMIND_MAP.md` — mapa técnico  
3. `docs/product/TORQMIND_DEVELOPMENT_CONTRACT.md` — contrato UI/produto  
4. `.cursor/rules/*.mdc` — execução focada  
5. Docs de domínio / ops / intelligence  
6. ADRs  
7. `PHASE3_*` / `COMPLETE_DELIVERY_*` — evidência histórica, não lei atual  

Não criar outro “master context” paralelo a esses.

## Instalação

1. Checkout canônico: `/home/tm/torqmind` (ou clone com `.cursor/` na raiz)
2. Confirme `.cursorignore` na raiz
3. Confirme `PROJECT_CONTEXT.md`, `AGENTS.md`, `CODEX_TORQMIND_MAP.md`
4. Opcional — User Rules (Settings → Rules) para preferências pessoais, sem contradizer `AGENTS.md`

## User Rules sugeridas (pessoais)

```
Sempre responda em português brasileiro.
Antes de implementar qualquer coisa, confirme que entendeu a tarefa.
Nunca faça suposições sobre nomes de colunas ou schemas — pergunte ou inspecione.
Prefira diffs cirúrgicos a rewrites completos.
Ao terminar uma tarefa, liste: arquivos alterados, o que mudou, riscos remanescentes.
```

## Personas (Cursor)

| Persona | Arquivo | Uso |
|---|---|---|
| Principal Architect | `torqmind-principal-architect.agent.mdc` | Orquestração, plano, handoff |
| Código | `torqmind-codigo.agent.mdc` | Implementação API/Web/SQL/testes |
| SSH Produção | `torqmind-ssh-producao.agent.mdc` | Deploy/diagnóstico produção |
| Homologação | `torqmind-homologacao.agent.mdc` | Validar em Hom antes de Prod |
| Git/Release | `torqmind-git-release.agent.mdc` | Branch, commit, push |

Não criar personas novas sem decisão explícita. Não migrar para `.cursor/agents/` nesta fase.

## Como usar

### Agent Mode
Tarefas multi-arquivo; as rules alwaysApply + globs relevantes entram automaticamente.

### Composer / edição inline
Arquivo único; globs do path aberto tendem a aplicar.

### @-references úteis
```
@AGENTS.md
@CODEX_TORQMIND_MAP.md
@PROJECT_CONTEXT.md
@sql/migrations/
@apps/api/app/repos_analytics.py
```

## Prompt de início de sessão

```
@AGENTS.md @CODEX_TORQMIND_MAP.md @PROJECT_CONTEXT.md

Confirme em poucas linhas:
1. Hot path BI = ClickHouse (exceções PG só se registradas)
2. Isolamento multi-tenant (id_empresa / id_filial)
3. Homolog e Prod compartilham analytics em 172.30.0.9
4. Quais arquivos você vai tocar

Quando estiver pronto, diga o contexto ativo.
```

## MCP

Não há `.cursor/mcp.json` neste repositório. MCP pode ser introduzido depois, só com ferramentas de domínio controladas — nunca acesso irrestrito a banco.

## Copilot (VS Code)

Instruções: `.github/copilot-instructions.md`.  
Prompts: `.github/prompts/*.prompt.md`.  
**Não** há pack `.github/agents/*.agent.md` — personas vivem no Cursor (`.cursor/rules/*.agent.mdc`).
