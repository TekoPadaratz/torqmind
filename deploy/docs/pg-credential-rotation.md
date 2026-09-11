# Rotação coordenada — credencial PG compartilhada (`torqmind`)

**Não executar nesta etapa.** Este documento só orienta a ordem segura.

## Contexto

Hom e Prod (e env analytics / Debezium, quando o fingerprint coincidir) podem
compartilhar a mesma senha do role de aplicação. Rotação com credencial única
implica **janela de interrupção**: conexões novas falham até todos os
consumidores efetivos receberem a senha nova.

Não registrar senhas nem DSNs neste repositório nem em tickets.

## Ordem correta (obrigatória)

1. **Inventariar consumidores efetivos** (env files, containers, connectors,
   scripts que abrem conexão com a mesma credencial). Não presumir lista.
2. **Preparar** os novos valores em arquivos/staging **sem ativar** (cópia
   paralela, diff revisado, sem recreate ainda).
3. **Na mesma janela coordenada:**
   - `ALTER ROLE … PASSWORD` (banco passa a aceitar só a senha nova);
   - em seguida atualizar/ativar configs e recriar **somente** os consumidores
     inventariados para carregarem a senha nova.
4. **Validar conexões novas** de cada consumidor (login/`SELECT 1`/health que
   abra socket novo). Health de processo com pool antigo **não basta**.
5. Plano de recuperação: reverter role + configs na mesma janela se algum
   consumidor crítico falhar.

## Proibido

- Recriar API/ETL/Debezium com senha nova **antes** do banco aceitá-la.
- `ALTER ROLE` isolado sem atualizar consumidores na mesma janela.
- Declarar sucesso só com container “healthy” sem prova de conexão nova.
