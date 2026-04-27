# TorqMind — Documento de Contexto do Projeto

> Este arquivo é lido automaticamente pelo Cursor Agent para manter contexto do projeto.
> Mantenha-o atualizado conforme o sistema evolui.

## O que é o TorqMind

Micro SaaS Multi-Tenant de BI e Gestão Operacional para redes de postos de combustíveis.
Software de produção. Impacto real. Clientes reais com múltiplas filiais.

**Módulos de negócio:**
- Dashboard Geral (KPIs executivos)
- Vendas (análise por produto, turno, filial)
- Antifraude / Risco (motor risk_v2)
- Financeiro (DRE, margem, custo)
- Clientes / Churn (RFM, retenção)
- Caixa (fluxo, conciliação)
- Backoffice Platform (empresas, filiais, usuários, contratos, recebíveis, auditoria)

## Decisões Arquiteturais Críticas

### Por que mart.* e não dw.fact_*?
Consultar `dw.fact_venda` em tempo real causa 3 minutos de lentidão.
A solução implementada: Materialized Views em `mart.*` com REFRESH automático ao fim de cada ciclo ETL.
Esta decisão está consolidada em `repos_mart.py`. Não reverta.

### Por que ETL incremental?
Bases grandes travam quando o ETL reprocessa demais.
O `etl_orchestrator.py` controla o que foi processado via tabelas `etl.*`.
Cargas novas chegam continuamente — o pipeline precisa ser robusto a isso.

### Por que ID sintético negativo no risk_v2?
O grão de `dw.fact_risco_evento` é por `id_comprovante`.
Usuários outliers não têm comprovante válido → constraint `uq_fact_risco_evento_nk` quebra.
Solução: ID sintético negativo para esses casos.

## Pontos de Atenção Ativos

- ETL incremental deve terminar rápido — cron de produção não pode travar
- Filiais são administráveis via backoffice — ETL não pode reverter essas mudanças
- Migrations estão na `061+` — sempre verifique a migration mais recente antes de assumir schema

## Ambiente de Desenvolvimento

```bash
# Setup inicial
docker compose up -d

# Operação diária
make migrate          # aplica migrations pendentes
make test             # testes
make lint             # linting
make etl-incremental  # ciclo ETL manual
```

## Arquivos-Chave

| Arquivo | Responsabilidade |
|---|---|
| `apps/api/etl_orchestrator.py` | Orquestrador central do ETL |
| `apps/api/repos_mart.py` | Repositório das mart views — NÃO lê dw.fact_* |
| `sql/migrations/` | Fonte da verdade do schema |
| `deploy/docker-compose.yml` | Infraestrutura completa |
| `.cursor/rules/` | Regras do projeto para o Cursor |
