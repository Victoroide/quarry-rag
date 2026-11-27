-- Keep the source version independent of document schema and bookkeeping changes.
CREATE OR REPLACE VIEW grant_catalog_source_documents AS
SELECT d.id AS document_id, COALESCE(d.incarnation, d.id) AS incarnation,
       d.collection,
       pg_catalog.encode(pg_catalog.sha256(pg_catalog.convert_to(
         pg_catalog.jsonb_build_array(
           'quarry.grant-source.v2'::text,
           d.id, COALESCE(d.incarnation, d.id), d.collection, d.doc_type,
           d.fingerprint, d.content_dedup,
           pg_catalog.encode(pg_catalog.sha256(pg_catalog.convert_to(d.content, 'UTF8')), 'hex'),
           d.metadata, d.content_preview, d.text_source, d.file_name, d.file_type, d.s3_key,
           d.status, d.chunk_count, EXTRACT(EPOCH FROM d.created_at)::text,
           EXTRACT(EPOCH FROM d.completed_at)::text, d.scoped_vectors, d.vector_attempt,
           d.vector_incarnation_required
         )::text, 'UTF8')), 'hex') AS source_version
FROM documents d;
