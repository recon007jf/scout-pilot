-- MIGRATION: 004_scout_draft_engine
-- Date: Jan 10, 2026
-- Description: Single-Brain Draft Engine (Status, Body, Subject) & Atomic Locking Support
-- 1. ADD COLUMNS TO DOSSIERS
-- We attach the draft state directly to the dossier (the "Row").
ALTER TABLE dossiers
ADD COLUMN IF NOT EXISTS llm_draft_status TEXT DEFAULT 'idle',
    -- 'idle' | 'generating' | 'ready' | 'error'
ADD COLUMN IF NOT EXISTS llm_email_subject TEXT,
    ADD COLUMN IF NOT EXISTS llm_email_body TEXT,
    -- WITHOUT signature
ADD COLUMN IF NOT EXISTS llm_draft_version INTEGER DEFAULT 0,
    ADD COLUMN IF NOT EXISTS llm_last_error TEXT,
    ADD COLUMN IF NOT EXISTS llm_draft_generated_at TIMESTAMP WITH TIME ZONE;
-- 2. INDEX FOR PERFORMANCE
CREATE INDEX IF NOT EXISTS idx_dossier_draft_status ON dossiers(llm_draft_status);