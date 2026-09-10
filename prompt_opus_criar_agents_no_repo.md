# Prompt histórico — criar Agents TorqMind no repositório

> **Status (2026-09-10):** documento **histórico / obsoleto como instrução de execução**.
> Não criar `.github/agents/` nem duplicar personas Cursor só para satisfazer este prompt.

## O que existe hoje

- Operação: `AGENTS.md`
- Copilot instructions: `.github/copilot-instructions.md` (existe)
- Prompts: `.github/prompts/*.prompt.md` (existem)
- **Personas ativas:** `.cursor/rules/*.agent.mdc`
  - `torqmind-principal-architect.agent.mdc`
  - `torqmind-codigo.agent.mdc`
  - `torqmind-ssh-producao.agent.mdc`
  - `torqmind-homologacao.agent.mdc`
  - `torqmind-git-release.agent.mdc`
- Setup Cursor: `CURSOR_SETUP.md`

## O que NÃO existe (e não deve ser inventado nesta fase)

```text
.github/agents/*.agent.md
.cursor/agents/
```

## Intenção original deste arquivo (arquivo morto)

O texto abaixo era um prompt pedindo a criação de custom agents Copilot.
Mantido só como evidência histórica — **não executar**.

```text
AGENTS.md
.github/copilot-instructions.md
.github/agents/torqmind-codigo.agent.md          ← NÃO criar
.github/agents/torqmind-ssh-producao.agent.md    ← NÃO criar
.github/agents/torqmind-git-release.agent.md     ← NÃO criar
.github/prompts/...
```
