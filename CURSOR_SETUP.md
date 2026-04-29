# ⚙️ Configuração do Cursor para o TorqMind

## Estrutura dos Arquivos

```
torqmind/
├── .cursor/
│   └── rules/
│       ├── 00-torqmind-global.mdc     → Contexto global, identidade e fluxo de entrega
│       ├── 01-security-tenancy.mdc    → Regras de ouro: multi-tenant e segurança
│       ├── 02-etl-database.mdc        → Pipeline ETL e PostgreSQL
│       ├── 03-backend-fastapi.mdc     → Backend FastAPI
│       └── 04-frontend-nextjs.mdc     → Frontend Next.js
├── .cursorignore                      → Exclui node_modules, builds, dados, segredos
└── PROJECT_CONTEXT.md                 → Contexto vivo do projeto (lido pelo Agent)
```

## Instalação

1. Copie a pasta `.cursor/` para a raiz do seu monorepo TorqMind
2. Copie `.cursorignore` para a raiz do monorepo
3. Copie `PROJECT_CONTEXT.md` para a raiz do monorepo
4. Abra o Cursor e vá em **Settings → Rules** (User Rules)

---

## Configuração de User Rules (Settings → Rules)

Cole isso no campo de User Rules do Cursor (aplica a TODOS os projetos seus):

```
Sempre responda em português brasileiro.
Antes de implementar qualquer coisa, confirme que entendeu a tarefa.
Nunca faça suposições sobre nomes de colunas ou schemas — pergunte ou inspecione.
Prefira diffs cirúrgicos a rewrites completos.
Ao terminar uma tarefa, liste: arquivos alterados, o que mudou, riscos remanescentes.
```

---

## Como Usar o Cursor no TorqMind

### Agent Mode (⌘. ou Ctrl+.)
Use para tarefas que envolvem múltiplos arquivos:
- "Adiciona endpoint de overview de vendas respeitando multi-tenant"
- "Cria migration para nova coluna em dw.fact_venda"
- "Refatora o ETL da track operational para ser mais eficiente"

O Agent vai automaticamente ler as regras e o PROJECT_CONTEXT.md.

### Composer (⌘K ou Ctrl+K)
Use para edições inline em arquivo único:
- Refatorar uma função específica
- Corrigir um bug pontual
- Adicionar tipagem Pydantic

### @-references úteis
```
@PROJECT_CONTEXT.md        → força o Agent a ler o contexto do projeto
@sql/migrations/           → referencia as migrations para checar schema
@apps/api/etl_orchestrator.py  → referencia o orquestrador ETL
```

---

## Modelo Recomendado

No Cursor, selecione **Claude Sonnet 4.6** para tarefas do dia a dia.
Para decisões arquiteturais complexas ou refatorações grandes, use **Claude Opus 4.6**.

---

## Dica: Prompt de Inicialização de Sessão

Ao começar uma nova sessão de trabalho no Agent, use este prompt:

```
@PROJECT_CONTEXT.md

Leia o contexto do projeto e confirme que entendeu:
1. A arquitetura de schemas (stg → dw → mart)
2. A regra de nunca ler dw.fact_* nos dashboards
3. O isolamento multi-tenant obrigatório

Quando estiver pronto, me diga o que está no seu contexto ativo.
```

Isso garante que o Agent não vai inventar nomes de colunas ou quebrar as regras de ouro.
