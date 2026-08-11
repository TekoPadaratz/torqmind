-- 135: etl.safe_int must accept JSON numbers serialized as "123.0"
--
-- Bug: pymssql/heal podem gravar IDs inteiros como number JSON com casa decimal
-- (ex.: 282384.0). O fallback antigo fazia regexp_replace('[^0-9-]', '') →
-- '2823840', quebrando join de baixas (contaspagar/contasreceber) e inflando
-- contas a pagar na tela vs Xpert.
--
-- ClickHouse: toInt32OrZero('282384.0') = 0 — por isso também normalizamos
-- payloads STG para inteiros JSON limpos (CDC propaga).

CREATE OR REPLACE FUNCTION etl.safe_int(p_text text)
RETURNS integer
LANGUAGE plpgsql
IMMUTABLE
AS $function$
DECLARE v integer;
BEGIN
  IF p_text IS NULL OR btrim(p_text) = '' THEN
    RETURN NULL;
  END IF;
  BEGIN
    v := p_text::integer;
    RETURN v;
  EXCEPTION WHEN others THEN
    BEGIN
      -- "282384.0" / "1.5e2" → trunc numeric (não concatenar dígitos do '.')
      v := trunc(p_text::numeric)::integer;
      RETURN v;
    EXCEPTION WHEN others THEN
      BEGIN
        v := regexp_replace(p_text, '[^0-9-]', '', 'g')::integer;
        RETURN v;
      EXCEPTION WHEN others THEN
        RETURN NULL;
      END;
    END;
  END;
END;
$function$;

-- Normaliza IDs floatish já persistidos em baixas (payload JSONB).
UPDATE stg.contaspagarbaixa b
SET payload = b.payload
  || jsonb_build_object(
       'ID_CONTASPAGAR', to_jsonb(etl.safe_int(b.payload->>'ID_CONTASPAGAR')),
       'ID_CONTASPAGARBAIXA', to_jsonb(
         COALESCE(
           etl.safe_int(b.payload->>'ID_CONTASPAGARBAIXA'),
           b.id_contaspagarbaixa
         )
       ),
       'ID_FILIAL', to_jsonb(COALESCE(etl.safe_int(b.payload->>'ID_FILIAL'), b.id_filial)),
       'ID_DB', to_jsonb(COALESCE(etl.safe_int(b.payload->>'ID_DB'), b.id_db))
     ),
    ingested_at = now(),
    received_at = now()
WHERE (b.payload->>'ID_CONTASPAGAR') LIKE '%.%'
   OR (b.payload->>'ID_CONTASPAGARBAIXA') LIKE '%.%'
   OR (b.payload->>'ID_FILIAL') LIKE '%.%'
   OR (b.payload->>'ID_DB') LIKE '%.%';

UPDATE stg.contasreceberbaixa b
SET payload = b.payload
  || jsonb_build_object(
       'ID_CONTASRECEBER', to_jsonb(etl.safe_int(b.payload->>'ID_CONTASRECEBER')),
       'ID_CONTASRECEBERBAIXA', to_jsonb(
         COALESCE(
           etl.safe_int(b.payload->>'ID_CONTASRECEBERBAIXA'),
           b.id_contasreceberbaixa
         )
       ),
       'ID_FILIAL', to_jsonb(COALESCE(etl.safe_int(b.payload->>'ID_FILIAL'), b.id_filial)),
       'ID_DB', to_jsonb(COALESCE(etl.safe_int(b.payload->>'ID_DB'), b.id_db))
     ),
    ingested_at = now(),
    received_at = now()
WHERE (b.payload->>'ID_CONTASRECEBER') LIKE '%.%'
   OR (b.payload->>'ID_CONTASRECEBERBAIXA') LIKE '%.%'
   OR (b.payload->>'ID_FILIAL') LIKE '%.%'
   OR (b.payload->>'ID_DB') LIKE '%.%';
