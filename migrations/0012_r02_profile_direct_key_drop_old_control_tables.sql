SET LOCAL lock_timeout = '5s';
SET LOCAL statement_timeout = '20s';

DO $migration_preflight$
DECLARE
    dev_namespace_id uuid;
    dev_namespace_count integer;
BEGIN
    SELECT count(*)::integer, min(namespace_id::text)::uuid
      INTO dev_namespace_count, dev_namespace_id
      FROM stock.namespaces
     WHERE environment = 'DEV';
    IF dev_namespace_count <> 1 OR dev_namespace_id IS NULL THEN
        RAISE EXCEPTION '0012 requires exactly one DEV namespace';
    END IF;

    IF (SELECT count(*) FROM stock.source_set_candidate_inputs) > 14
       OR EXISTS (
           SELECT 1 FROM stock.source_set_candidate_inputs
            WHERE namespace_id IS DISTINCT FROM dev_namespace_id
       ) THEN
        RAISE EXCEPTION '0012 candidate-input rows exceed the approved DEV-only boundary';
    END IF;
    IF (SELECT count(*) FROM stock.source_set_capabilities) > 4
       OR EXISTS (
           SELECT 1 FROM stock.source_set_capabilities
            WHERE namespace_id IS DISTINCT FROM dev_namespace_id
       ) THEN
        RAISE EXCEPTION '0012 capability rows exceed the approved DEV-only boundary';
    END IF;
    IF (SELECT count(*) FROM stock.source_sets) > 2
       OR EXISTS (
           SELECT 1 FROM stock.source_sets
            WHERE namespace_id IS DISTINCT FROM dev_namespace_id
       ) THEN
        RAISE EXCEPTION '0012 source-set rows exceed the approved DEV-only boundary';
    END IF;
    IF (SELECT count(*) FROM stock.source_set_candidates) > 2
       OR EXISTS (
           SELECT 1 FROM stock.source_set_candidates
            WHERE namespace_id IS DISTINCT FROM dev_namespace_id
       ) THEN
        RAISE EXCEPTION '0012 candidate rows exceed the approved DEV-only boundary';
    END IF;

    IF EXISTS (
        SELECT 1
          FROM stock.source_symbol_price_profiles
         WHERE namespace_id IS NULL
            OR target_session IS NULL
            OR source_symbol IS NULL
            OR package_sha256 IS NULL
            OR profile_revision IS NULL
    ) THEN
        RAISE EXCEPTION '0012 Profile direct identity contains NULL';
    END IF;
    IF EXISTS (
        SELECT 1
          FROM stock.source_symbol_price_profiles
         GROUP BY namespace_id, target_session, source_symbol,
                  package_sha256, profile_revision
        HAVING count(*) > 1
    ) THEN
        RAISE EXCEPTION '0012 Profile direct identity is not unique';
    END IF;
    IF EXISTS (
        SELECT 1
          FROM stock.source_symbol_price_profiles p
          LEFT JOIN stock.source_raw_daily r
            ON r.namespace_id = p.namespace_id
           AND r.source_capture_id = p.source_capture_id
           AND r.session_date = p.target_session
           AND r.package_sha256 = p.package_sha256
           AND r.source_symbol = p.source_symbol
         WHERE r.source_symbol IS NULL
    ) THEN
        RAISE EXCEPTION '0012 Profile row has no exact Raw capture row';
    END IF;
    IF EXISTS (
        SELECT 1
          FROM stock.source_symbol_price_profiles p
          LEFT JOIN stock.source_captures c
            ON c.namespace_id = p.namespace_id
           AND c.capture_id = p.source_capture_id
         WHERE c.capture_id IS NULL
    ) THEN
        RAISE EXCEPTION '0012 Profile row has no source capture';
    END IF;
    IF EXISTS (
        SELECT p.namespace_id, p.target_session, p.source_capture_id,
               p.package_sha256, p.profile_revision
          FROM stock.source_symbol_price_profiles p
         GROUP BY p.namespace_id, p.target_session, p.source_capture_id,
                  p.package_sha256, p.profile_revision
        HAVING count(*) <> (
            SELECT count(*)
              FROM stock.source_raw_daily r
             WHERE r.namespace_id = p.namespace_id
               AND r.source_capture_id = p.source_capture_id
               AND r.session_date = p.target_session
               AND r.package_sha256 = p.package_sha256
        )
    ) THEN
        RAISE EXCEPTION '0012 Profile revision does not cover its exact Raw capture';
    END IF;
END
$migration_preflight$;

ALTER TABLE stock.source_symbol_price_profiles
    DROP CONSTRAINT source_symbol_price_profiles_namespace_id_source_set_id_fkey,
    DROP CONSTRAINT source_symbol_price_profiles_namespace_id_target_session_p_fkey,
    DROP CONSTRAINT source_symbol_price_profiles_pkey,
    DROP CONSTRAINT source_symbol_price_profiles_namespace_id_source_set_id_pac_key;

ALTER TABLE stock.source_symbol_price_profiles
    DROP COLUMN source_set_id,
    DROP COLUMN source_set_revision;

ALTER TABLE stock.source_symbol_price_profiles
    ADD CONSTRAINT source_symbol_price_profiles_pkey
    PRIMARY KEY (
        namespace_id,
        target_session,
        source_symbol,
        package_sha256,
        profile_revision
    );

DROP TABLE stock.source_set_candidate_inputs;
DROP TABLE stock.source_set_capabilities;
DROP TABLE stock.source_sets;
DROP TABLE stock.source_set_candidates;

