-- MIGRATION: 005_create_drafts_table
-- Date: Jan 11, 2026
-- Description: Create dedicated drafts table to solve PGRST116 and separate concerns.
CREATE TABLE IF NOT EXISTS drafts (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    dossier_id UUID NOT NULL,
    -- Content
    email_subject TEXT,
    email_body TEXT,
    -- Clean body (no signature)
    -- Versioning & Audit
    version INTEGER DEFAULT 1,
    status TEXT DEFAULT 'ready',
    -- 'generating', 'ready', 'error'
    last_error TEXT,
    -- Timestamps
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    -- Constraints
    CONSTRAINT drafts_dossier_id_key UNIQUE (dossier_id)
);
-- Index for fast lookups
CREATE INDEX IF NOT EXISTS idx_drafts_dossier_id ON drafts(dossier_id);