# TorqMind — Próximos Passos Revisado

Data da revisão: 2026-06-04  
Base analisada: pacote `TorqMind(2).zip`  
Documento de origem encontrado: `docs/product/ROADMAP_DORES_POSTO.md`

## 1. Situação atual

O TorqMind está em fase de produto operacional, com módulos principais funcionando e com governança técnica mais madura após as últimas rodadas:

- Dashboard Geral, Vendas, Caixa, Antifraude, Clientes, Financeiro, Gestão de Lucro, Preço Concorrente e Metas & Equipe estão estruturados.
- Prioridades de Cobrança foi corrigido para evitar duplicidade por grão errado e passou a usar `mart.customer_delinquency_summary`.
- Comissão foi ajustada para suportar os três modos: `team_total`, `equal_split` e `individual_sales`.
- Pagamentos agora expõem a diferença de conciliação como linha explícita “Não conciliado (operacional)”.
- 2FA por TOTP foi implementado no backend e no fluxo de login/reset, mas ainda falta uma tela de auto-cadastro com QR Code para o usuário final.
- Agents/regras foram endurecidos com ritual fonte → tela e uso obrigatório dos mapas.

## 2. Pendências antes de novas features grandes

Estas pendências não impedem o uso atual, mas devem ser tratadas antes de vender escala maior:

### 2.1 2FA — tela de auto-cadastro

**Status:** backend pronto; login e reset já suportam 2FA quando habilitado.  
**Gap:** o usuário final ainda não tem uma página de Perfil/Segurança para escanear QR Code e ativar o autenticador por conta própria.

**Implementar:**

- Página ou seção “Minha segurança”.
- Botão “Ativar autenticação em dois fatores”.
- Exibição de QR Code do `otpauth_uri`.
- Campo para confirmar o código de 6 dígitos.
- Exibição de recovery codes uma única vez.
- Botão “Desativar 2FA” exigindo código.
- Status visual: ativo/inativo/configuração indisponível.

**Prioridade:** alta, porque completa a segurança sem depender do admin.

### 2.2 SMTP e domínio

**Status:** serviço SMTP existe e é env-driven.  
**Gap:** domínio `torqmind.com` ainda não deve ser usado se não estiver comprado/controlado.

**Ajustar:**

- Atualizar exemplos de `.env` com SMTP/TOTP.
- Corrigir documentação interna que ainda sugere `SMTP_FROM=master@torqmind.com`.
- Usar remetente de domínio controlado até comprar/configurar o domínio oficial.
- Antes de ativar remetente `@torqmind.com`, configurar SPF, DKIM e DMARC.

**Prioridade:** alta se o fluxo “esqueci minha senha” for usado em produção real.

### 2.3 Pagamentos e conciliação

**Status:** payment mix agora concilia visualmente com vendas usando linha explícita de diferença.  
**Gap:** ainda precisa virar auditoria recorrente, porque diferença operacional pode esconder origem legítima ou problema de ingestão.

**Implementar depois:**

- Painel técnico/operacional de conciliação por filial/período.
- Drill-down dos comprovantes com pagamento maior/menor que a venda.
- Separar causas: fiado/prazo, troca de forma, arredondamento, ausência de forma, erro de origem.
- Alertas quando diferença passar de limite percentual/material.

**Prioridade:** média-alta para confiança financeira.

### 2.4 Testes locais/documentação de ambiente

**Status:** testes passaram no ambiente de produção segundo relatório do agente.  
**Gap:** no sandbox local, alguns testes não rodam sem `psycopg` e `clickhouse_connect`, apesar de `compileall` passar.

**Implementar:**

- Documentar comando oficial de teste dentro do container/venv correto.
- Garantir `requirements.txt`/ambiente dev com dependências mínimas para testes.
- Criar script único `make test-core` ou equivalente.

**Prioridade:** média.

## 3. Próximas features recomendadas

### 3.1 Comissão — acabamento de produto

**Objetivo:** transformar comissão de regra funcional em ferramenta de gestão motivacional.

**Itens:**

- Manual atualizado com comissão.
- Cards por nível Bronze/Prata/Ouro/Diamante.
- Explicação clara dos três modos de pagamento.
- Histórico mensal de campanhas.
- Exportação simples da comissão calculada.
- Validação com regra real do cliente antes de congelar nomenclaturas.

**Prioridade:** alta, porque é o próximo módulo planejado.

### 3.2 Estoque de combustível por tanque/bico/LMC

**Documento base:** seção 1 do `ROADMAP_DORES_POSTO.md`.

**Resumo:** fonte Xpert já mapeada: `TANQUES`, `QTDESTANQUES`, `MOVTANQUES`, `LMC`, `LMCTANQUES`, `LMCBICOS`, `LMCENTRADATANQUES`, `BICOS`, `BOMBAS`, `AFERICAO`.

**Decisão técnica importante:** usar LMC como fonte viva principal de volumetria; não usar `ENCERRANTESTURNOS` como fonte atual, pois está congelada.

**Entregáveis futuros:**

- STG de tanques/bicos/LMC/aferição.
- Mart de estoque combustível por tanque/dia.
- KPI no Dashboard Geral.
- Alerta de quebra/sobra acima da tolerância.
- Comparativo saldo esperado vs medição física.

**Prioridade:** muito alta para dono de posto, mas deve vir depois de estabilizar comissão/segurança.

### 3.3 Conciliação de cartão/TEF

**Documento base:** seção 2 do `ROADMAP_DORES_POSTO.md`.

**Objetivo:** comparar venda por cartão, taxa esperada, taxa efetiva, recebível e divergência.

**Entregáveis:**

- Mapear/validar tabelas Xpert de cartão, bandeiras, taxas e EDI.
- Mart de conciliação cartão por filial/bandeira/dia.
- Tela no Financeiro.
- Alertas de taxa divergente e valores não conciliados.

**Prioridade:** alta após confiança nos totalizadores de pagamento.

### 3.4 Saúde dos dados / painel técnico interno

**Objetivo:** permitir que o operador do TorqMind saiba se o dado está fresco e confiável.

**Entregáveis:**

- Última carga Agent por dataset.
- Última publicação CDC/ClickHouse.
- Último refresh de mart.
- Status de divergências fonte → tela.
- Smoke por tela.
- Alertas técnicos.

**Prioridade:** alta antes de escalar para mais clientes.

### 3.5 Manual executivo/comercial final

**Objetivo:** usar como peça comercial na apresentação da Verenka.

**Atualizar com:**

- Comissão.
- 2FA/segurança.
- Gestão de Lucro com fórmulas completas.
- Sem chamar o produto de “BI” como definição principal.
- Posicionamento: “plataforma de inteligência operacional e gerencial”.

**Prioridade:** alta antes da apresentação.

## 4. Regras permanentes para próximas rodadas

1. Nunca corrigir bug de dado apenas no frontend.
2. Para qualquer divergência, validar fonte → STG → DW/mart → ClickHouse → API → tela.
3. Usar `CODEX_TORQMIND_MAP.md` antes de abrir arquivos soltos.
4. Evitar refatoração ampla em produção sem causa raiz.
5. Sempre preservar responsividade e design system.
6. Não expor custo, margem, lucro, CMV ou markup para perfis não autorizados.
7. Não usar `ID_DB` como chave gerencial; preservar apenas como rastreabilidade técnica quando necessário.
8. Não usar domínio/e-mail não controlado como remetente oficial.
9. Não declarar PASS sem testes, health check e validação real.
10. Não aceitar número financeiro “aproximadamente certo” sem explicar diferença.

## 5. Ordem sugerida para as próximas rodadas

1. Completar tela de auto-cadastro 2FA + QR Code.
2. Atualizar env examples e documentação SMTP/TOTP.
3. Finalizar acabamento/manual do módulo Comissão.
4. Criar painel de Saúde dos Dados.
5. Implementar Estoque de Combustível por tanque/LMC.
6. Implementar Conciliação de Cartão/TEF.
7. Evoluir Gestão de Lucro com histórico, exportação e simulação por grupo.

## 6. Decisão atual

O projeto está bom para seguir, mas antes de novas features grandes vale fechar as duas lacunas pequenas de segurança/documentação:

- UI de 2FA para o usuário final.
- exemplos/documentação de SMTP/TOTP.

Depois disso, a próxima feature estratégica é Comissão com acabamento e manual atualizado.
