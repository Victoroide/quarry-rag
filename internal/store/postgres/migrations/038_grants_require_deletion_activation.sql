-- Keep the existing grants cutover entry point, permissions and lock order.
-- Checkpointed deletion remains a separate, monotone, schema-owner operation.
CREATE OR REPLACE FUNCTION grant_catalog_activate_grants_impl(p_dry_run BOOLEAN)
RETURNS TABLE (resulting_revision BIGINT, already_active BOOLEAN)
LANGUAGE plpgsql AS $$
DECLARE
    current_mode TEXT;
    current_revision BIGINT;
BEGIN
    IF p_dry_run IS NULL THEN
        RAISE SQLSTATE 'QG001' USING MESSAGE = 'invalid preview flag';
    END IF;
    IF p_dry_run THEN
        SELECT m.authz_mode INTO STRICT current_mode FROM grant_catalog_corpus_mode m WHERE m.singleton;
        SELECT r.revision INTO STRICT current_revision FROM grant_catalog_policy_revision r WHERE r.singleton;
        IF document_deletion_active() IS DISTINCT FROM TRUE THEN
            RAISE SQLSTATE 'QG006' USING MESSAGE = 'checkpointed document deletion must be activated first';
        END IF;
        RETURN QUERY SELECT current_revision, current_mode = 'grants';
        RETURN;
    END IF;
    SELECT m.authz_mode INTO STRICT current_mode FROM grant_catalog_corpus_mode m WHERE m.singleton FOR UPDATE;
    SELECT r.revision INTO STRICT current_revision FROM grant_catalog_policy_revision r WHERE r.singleton FOR UPDATE;
    IF document_deletion_active() IS DISTINCT FROM TRUE THEN
        RAISE SQLSTATE 'QG006' USING MESSAGE = 'checkpointed document deletion must be activated first';
    END IF;
    IF current_mode = 'grants' THEN
        RETURN QUERY SELECT current_revision, TRUE;
        RETURN;
    END IF;
    PERFORM grant_catalog_check_revision(current_revision, current_revision);
    UPDATE grant_catalog_corpus_mode
    SET authz_mode = 'grants', activated_at = clock_timestamp(), activated_by = session_user
    WHERE singleton;
    UPDATE grant_catalog_policy_revision
    SET revision = revision + 1
    WHERE singleton
    RETURNING revision INTO STRICT current_revision;
    INSERT INTO grant_catalog_security_audit (operator, policy_revision, action, outcome, reason)
    VALUES (session_user, current_revision, 'corpus_cutover', 'allow', 'grants_activated');
    RETURN QUERY SELECT current_revision, FALSE;
END;
$$;
