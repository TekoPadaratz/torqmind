# Prompt para Opus criar Agents TorqMind no repositório

Você está conectado no repositório TorqMind.

Objetivo: criar custom agents e prompt files do VS Code/Copilot para acelerar manutenção, produção, Git, qualidade e performance do TorqMind.

Criar exatamente estes arquivos:

```text
AGENTS.md
.github/copilot-instructions.md
.github/agents/torqmind-codigo.agent.md
.github/agents/torqmind-ssh-producao.agent.md
.github/agents/torqmind-git-release.agent.md
.github/prompts/debug-producao.prompt.md
.github/prompts/deploy-producao.prompt.md
.github/prompts/auditar-zip.prompt.md
.github/prompts/validar-marts-clickhouse.prompt.md
.github/prompts/validar-controle-acesso.prompt.md
README_AGENTS_TORQMIND.md
```

Regras:
- Não alterar código do produto.
- Não mexer em deploy.
- Não rodar migrations.
- Não alterar package.json.
- Apenas criar arquivos de instrução/agents/prompts.
- Rodar validação simples:

```bash
find .github/agents -maxdepth 1 -type f -name "*.agent.md" -print
find .github/prompts -maxdepth 1 -type f -name "*.prompt.md" -print
test -f AGENTS.md
test -f .github/copilot-instructions.md
```

Commitar e pushar apenas se o usuário pedir.

Relatório: arquivos criados, como usar no VS Code, git status, commit hash se comitado.
