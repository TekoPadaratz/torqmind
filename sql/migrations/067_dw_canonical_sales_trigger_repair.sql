-- ============================================================================
-- Migration 067: DW canonical sales trigger repair
-- ============================================================================
-- PT-BR:
-- Recria triggers de compatibilidade que preenchem chaves canônicas de venda
-- antes do INSERT/UPDATE. Em bancos com drift, as colunas canônicas existem
-- como NOT NULL, mas o trigger histórico pode não existir, quebrando inserts
-- legados que ainda informam apenas id_movprodutos/id_itensmovprodutos.
-- ============================================================================

BEGIN;

CREATE OR REPLACE FUNCTION etl.fact_venda_sync_canonical_keys()
RETURNS trigger AS $$
BEGIN
  IF NEW.id_comprovante IS NULL AND NEW.id_movprodutos IS NOT NULL THEN
    NEW.id_comprovante := NEW.id_movprodutos;
  END IF;
  IF NEW.id_movprodutos IS NULL AND NEW.id_comprovante IS NOT NULL THEN
    NEW.id_movprodutos := NEW.id_comprovante;
  END IF;
  RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION etl.fact_venda_item_sync_canonical_keys()
RETURNS trigger AS $$
DECLARE
  v_doc_key integer;
BEGIN
  IF NEW.id_comprovante IS NULL AND NEW.id_movprodutos IS NOT NULL THEN
    SELECT v.id_comprovante
    INTO v_doc_key
    FROM dw.fact_venda v
    WHERE v.id_empresa = NEW.id_empresa
      AND v.id_filial = NEW.id_filial
      AND v.id_db = NEW.id_db
      AND v.id_movprodutos = NEW.id_movprodutos
    ORDER BY v.updated_at DESC NULLS LAST, v.created_at DESC NULLS LAST
    LIMIT 1;

    NEW.id_comprovante := COALESCE(v_doc_key, NEW.id_movprodutos);
  END IF;

  IF NEW.id_movprodutos IS NULL AND NEW.id_comprovante IS NOT NULL THEN
    NEW.id_movprodutos := NEW.id_comprovante;
  END IF;

  IF NEW.id_itemcomprovante IS NULL AND NEW.id_itensmovprodutos IS NOT NULL THEN
    NEW.id_itemcomprovante := NEW.id_itensmovprodutos;
  END IF;

  IF NEW.id_itensmovprodutos IS NULL AND NEW.id_itemcomprovante IS NOT NULL THEN
    NEW.id_itensmovprodutos := NEW.id_itemcomprovante;
  END IF;

  RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_dw_fact_venda_sync_canonical_keys ON dw.fact_venda;
CREATE TRIGGER trg_dw_fact_venda_sync_canonical_keys
BEFORE INSERT OR UPDATE ON dw.fact_venda
FOR EACH ROW EXECUTE FUNCTION etl.fact_venda_sync_canonical_keys();

DROP TRIGGER IF EXISTS trg_dw_fact_venda_item_sync_canonical_keys ON dw.fact_venda_item;
CREATE TRIGGER trg_dw_fact_venda_item_sync_canonical_keys
BEFORE INSERT OR UPDATE ON dw.fact_venda_item
FOR EACH ROW EXECUTE FUNCTION etl.fact_venda_item_sync_canonical_keys();

COMMIT;
