DO $$
BEGIN
    LOCK TABLE ingest_jobs IN ACCESS EXCLUSIVE MODE;
    IF EXISTS (
        SELECT 1
        FROM ingest_jobs j
        JOIN document_pdf_extractions p ON p.document_id = j.document_id
        WHERE j.state = 'running'
          AND j.lease_until > clock_timestamp()
          AND p.profile = 'quarry.pdf.layout.v1'
    ) THEN
        RAISE EXCEPTION 'active layout PDF leases must drain before applying migration 040; pause layout admissions and drain or release running layout jobs'
            USING ERRCODE = 'object_not_in_prerequisite_state';
    END IF;
END
$$;

ALTER TABLE ingest_jobs
    ADD COLUMN layout_claim_attempt INTEGER,
    ADD CONSTRAINT ingest_jobs_layout_lease CHECK (
        layout_claim_attempt IS NULL
        OR (state = 'running' AND (layout_claim_attempt = attempts) IS TRUE AND lease_token IS NOT NULL)
    );

CREATE FUNCTION require_layout_capable_claim() RETURNS TRIGGER
LANGUAGE plpgsql SET search_path = pg_catalog, pg_temp AS $$
DECLARE
    doc_profile TEXT;
    doc_coverage TEXT;
BEGIN
    IF NEW.state <> 'running' THEN
        NEW.layout_claim_attempt = NULL;
    ELSIF OLD.state <> 'running' OR NEW.attempts <> OLD.attempts THEN
        -- Old replicas could requeue a layout checkpoint with pdf_required = FALSE.
        EXECUTE format('SELECT profile, coverage FROM %I.document_pdf_extractions WHERE document_id = $1', TG_TABLE_SCHEMA)
            INTO doc_profile, doc_coverage
            USING OLD.document_id;
        IF doc_profile = 'quarry.pdf.layout.v1' THEN
            IF (NEW.layout_claim_attempt = NEW.attempts) IS NOT TRUE
               OR NEW.lease_token IS NULL
               OR NEW.lease_token IS NOT DISTINCT FROM OLD.lease_token THEN
                RETURN NULL;
            END IF;
            IF doc_coverage = 'pending' THEN
                NEW.pdf_required = TRUE;
            END IF;
            RETURN NEW;
        END IF;
        NEW.layout_claim_attempt = NULL;
    END IF;
    RETURN NEW;
END;
$$;

CREATE TRIGGER ingest_jobs_layout_capable_claim
    BEFORE UPDATE OF state, attempts ON ingest_jobs
    FOR EACH ROW EXECUTE FUNCTION require_layout_capable_claim();
