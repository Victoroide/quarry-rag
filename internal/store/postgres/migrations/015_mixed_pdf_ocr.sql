CREATE TABLE document_pdf_extractions (
    document_id UUID PRIMARY KEY REFERENCES documents(id) ON DELETE CASCADE,
    profile TEXT NOT NULL,
    manifest JSONB NOT NULL,
    coverage TEXT NOT NULL CHECK (coverage IN ('pending', 'complete', 'partial', 'failed')),
    page_results JSONB,
    window_id UUID NOT NULL DEFAULT gen_random_uuid(),
    started_at TIMESTAMPTZ,
    start_bound_at TIMESTAMPTZ,
    accepted_at TIMESTAMPTZ
);

ALTER TABLE ingest_jobs
    ADD COLUMN pdf_required BOOLEAN NOT NULL DEFAULT FALSE,
    ADD COLUMN pdf_claim_attempt INTEGER,
    ADD CONSTRAINT ingest_jobs_pdf_lease CHECK (
        (state = 'running' OR pdf_claim_attempt IS NULL)
        AND (NOT pdf_required OR state <> 'running' OR ((pdf_claim_attempt = attempts) IS TRUE AND lease_token IS NOT NULL))
    );

CREATE INDEX idx_ingest_jobs_pdf_required ON ingest_jobs (document_id) WHERE pdf_required;

CREATE FUNCTION require_pdf_capable_claim() RETURNS TRIGGER LANGUAGE plpgsql AS $$
BEGIN
    IF NEW.state <> 'running' THEN
        NEW.pdf_claim_attempt = NULL;
        NEW.lease_token = NULL;
    ELSIF OLD.state <> 'running' OR NEW.attempts <> OLD.attempts THEN
        IF (NEW.pdf_claim_attempt = NEW.attempts) IS NOT TRUE
           AND EXISTS (SELECT 1 FROM ingest_jobs WHERE pdf_required) THEN
            RAISE check_violation USING MESSAGE = 'mixed PDF work requires a compatible queue consumer',
                CONSTRAINT = 'ingest_jobs_pdf_lease';
        END IF;
        IF NEW.lease_token IS NOT DISTINCT FROM OLD.lease_token THEN
            NEW.lease_token = NULL;
        END IF;
    END IF;
    RETURN NEW;
END;
$$;

CREATE TRIGGER ingest_jobs_pdf_capable_claim
    BEFORE UPDATE OF state, attempts ON ingest_jobs
    FOR EACH ROW EXECUTE FUNCTION require_pdf_capable_claim();
