-- Migration 008: Create Candidates Table for Phase 1
-- DATE: 2026-01-14
-- 1. Create 'candidates' table
-- We assume this is a new table to decouple from legacy 'target_brokers'.
-- We will seed it later from target_brokers or leads.
CREATE TABLE IF NOT EXISTS candidates (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    -- Link to source data. Assuming we link to 'leads' or 'target_brokers'.
    -- The user didn't specify the FK, but we need context. 
    -- We'll add a JSONB 'context' column or link to leads if possible.
    -- For now, we'll include basic fields needed for drafting.
    lead_id UUID,
    -- Optional link to leads table
    full_name TEXT,
    firm TEXT,
    role TEXT,
    email TEXT,
    linkedin_url TEXT,
    -- Phase 1 State Machine Columns
    status TEXT NOT NULL DEFAULT 'POOL' CHECK (status IN ('POOL', 'QUEUED', 'SENT', 'FAILED')),
    draft_ready BOOLEAN NOT NULL DEFAULT FALSE,
    sent_at TIMESTAMP WITH TIME ZONE,
    draft_body TEXT,
    -- Metadata
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);
-- 2. Index for Queue Performance
CREATE INDEX IF NOT EXISTS idx_candidates_status_draft_ready ON candidates(status, draft_ready);
CREATE INDEX IF NOT EXISTS idx_candidates_sent_at ON candidates(sent_at);
-- 3. Velocity Check Function
CREATE OR REPLACE FUNCTION check_rolling_velocity() RETURNS BIGINT LANGUAGE sql STABLE AS $$
SELECT count(*)
FROM candidates
WHERE sent_at > (NOW() - INTERVAL '24 HOURS');
$$;