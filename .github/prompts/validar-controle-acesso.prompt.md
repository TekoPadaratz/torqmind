---
name: validar-controle-acesso
description: "Validar controle de acesso por role, tela, filial e dado sensível."
agent: "TorqMind Código"
---

# Validar Controle de Acesso TorqMind

Validar Fase 3.

Matriz mínima:
1. platform_master: tudo, Plataforma, margem/lucro/custo.
2. owner: telas normais, sem Plataforma, margem/lucro/custo.
3. manager com `customers`, `competitor_pricing`: vê só essas telas, APIs de outras telas 403, sem sensíveis.
4. manager com `sales`: vê Vendas, não recebe margem/lucro/custo.
5. tenant_kiosk: entra em `/tv`, só tela permitida, sem menu normal, só logout.
6. force password: bloqueia até trocar, troca limpa flag e redireciona para rota permitida.

Campos sensíveis a bloquear: margem, margin, lucro, profit, cmv, custo, cost, markup, rentab, rentabilidade, custo_total, custo_unitario, margin_10d, margem_score.

Relatório PASS/FAIL com endpoints testados, menus vistos, payload sensível, usuários temporários criados/removidos e bloqueadores.
