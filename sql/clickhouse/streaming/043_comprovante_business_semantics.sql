ALTER TABLE torqmind_current.fact_venda
    ADD COLUMN IF NOT EXISTS commercial_eligible UInt8 NOT NULL DEFAULT 1 AFTER cancelado;

ALTER TABLE torqmind_current.fact_comprovante
    ADD COLUMN IF NOT EXISTS ignored_business UInt8 NOT NULL DEFAULT 0 AFTER cancelado;

ALTER TABLE torqmind_current.fact_comprovante
    ADD COLUMN IF NOT EXISTS commercial_eligible UInt8 NOT NULL DEFAULT 1 AFTER ignored_business;

ALTER TABLE torqmind_current.fact_pagamento_comprovante
    ADD COLUMN IF NOT EXISTS cash_eligible UInt8 NOT NULL DEFAULT 0 AFTER data_key;

ALTER TABLE torqmind_current.stg_comprovantes_slim
    ADD COLUMN IF NOT EXISTS ignored_business UInt8 NOT NULL DEFAULT 0 AFTER cancelado;

ALTER TABLE torqmind_current.stg_comprovantes_slim
    ADD COLUMN IF NOT EXISTS commercial_eligible UInt8 NOT NULL DEFAULT 1 AFTER ignored_business;