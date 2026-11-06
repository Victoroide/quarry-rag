ALTER TABLE documents ADD COLUMN IF NOT EXISTS collection TEXT NOT NULL DEFAULT '';
CREATE INDEX IF NOT EXISTS idx_documents_collection_id ON documents (collection, id);
