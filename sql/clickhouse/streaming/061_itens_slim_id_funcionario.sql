-- 061_itens_slim_id_funcionario.sql
-- Vendedor canônico está no item (ID_FUNCIONARIOS), não no usuário de caixa do comprovante.
-- Propaga id_funcionario para a slim usada nas leituras BI.

ALTER TABLE torqmind_current.stg_itenscomprovantes_slim
    ADD COLUMN IF NOT EXISTS id_funcionario Int32 DEFAULT 0 AFTER id_grupo_produto;
