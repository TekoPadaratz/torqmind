# Final Review Round

## Pontos corrigidos

- Português da interface revisado nas telas principais, com acentuação e textos mais executivos.
- `Dashboard Geral` ajustado para leitura mais orientada à ação, com blocos de pagamentos e turnos menos destrutivos quando a fonte ainda não está pronta.
- `Sistema Anti-Fraude` limpo de termos mais técnicos na apresentação, com foco em investigação operacional.
- `Análise de Clientes` com leitura de churn mais próxima da operação de posto, enfatizando frequência, ausência e retomada comercial.
- `Financeiro` com labels mais claros para pagamentos, turnos e formas ainda em validação.
- `Preço da Concorrência` restringido a combustíveis, mantendo a lógica por filial e removendo itens sem aderência ao cenário competitivo.
- `Metas & Equipe` preservado como tela forte de motivação, com margem oculta por padrão e sinalização operacional menos acusatória.

## Pontos que dependem de dados adicionais da Xpert

- Turnos e caixa em aberto ainda dependem de carga confiável em `stg.turnos`. Enquanto isso, o produto trata o bloco como integração operacional em andamento, sem aparência de erro.
- Radar de recorrência anônima ainda depende de maior volume em `mart.anonymous_retention_daily` para entregar leitura mais rica por coorte.
- Parte do domínio de pagamentos ainda tem formas em validação. O produto já trata isso visualmente, mas o ganho final depende de mapeamento adicional da fonte.

## Módulos 100% prontos para demo

- Dashboard Geral
  - pronto para demonstração, com prioridades, alertas e leituras executivas
- Vendas
  - forte visualmente e semanticamente consistente para combustíveis, conveniência, serviços e outros da operação
- Sistema Anti-Fraude
  - pronto para demonstração com leitura de risco, eventos recentes e agrupamentos operacionais
- Metas & Equipe
  - pronto para TV/sala de reunião, com Top 5, ranking ampliado e camada motivacional

## Módulos parcialmente prontos

- Análise de Clientes
  - churn já está útil para demo, mas a recorrência anônima ainda depende de mais base para ganhar profundidade
- Financeiro
  - sólido para leitura de aging, pagamentos e anomalias, mas ainda depende de melhor cobertura da fonte de turnos e de mapeamento adicional de formas
- Preço da Concorrência
  - pronto para uso por filial e combustíveis, dependendo apenas de preenchimento dos preços concorrentes para a simulação completa

## Observações finais

- O branch atual está em estágio de apresentação comercial premium.
- Os pontos ainda pendentes foram tratados com states honestos e elegantes, evitando qualquer impressão de tela quebrada.
