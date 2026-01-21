-- Migration: Add draft_subject column to candidates table
-- Date: 2026-01-21
-- Purpose: Enable persistence of edited subject lines (currently only draft_body exists)
ALTER TABLE candidates
ADD COLUMN IF NOT EXISTS draft_subject TEXT;
-- Add comment for documentation
COMMENT ON COLUMN candidates.draft_subject IS 'Editable email subject line, separate from body';