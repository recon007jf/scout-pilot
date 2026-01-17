-- Migration 011: Phase 2 Data Enrichment (Images)
-- Adds storage for LinkedIn Profile Images to satisfy UI Requirement.
-- 1. Add Column to Candidates (The active entity)
ALTER TABLE candidates
ADD COLUMN IF NOT EXISTS linkedin_image_url TEXT;
-- 2. Comment
COMMENT ON COLUMN candidates.linkedin_image_url IS 'URL of profile image fetched via Serper/Proxy (Jan 17 UI Req)';