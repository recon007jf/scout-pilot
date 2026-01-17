-- Migration 010: Fix Morning Briefing Queue (Revised)
-- Creates the Queue table if it doesn't exist, linking to Candidates.
-- 1. Create Table (Safe Mode)
CREATE TABLE IF NOT EXISTS morning_briefing_queue (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    -- Link to Candidate (The Modern Entity)
    candidate_id UUID REFERENCES candidates(id) ON DELETE CASCADE,
    -- Status & Metadata
    status TEXT DEFAULT 'pending',
    selected_for_date DATE DEFAULT CURRENT_DATE,
    priority_score INTEGER DEFAULT 0,
    -- Backend Audit Fields (Required by Jan 17 Audit)
    ranking_reason TEXT,
    -- "Why this candidate?"
    draft_preview TEXT,
    -- Snapshot of draft at selection time
    -- Legacy/Optional Compatibility (Nullable)
    dossier_id UUID,
    -- Kept for 001 compatibility but optional
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    -- Constraint: One entry per candidate per day
    UNIQUE(candidate_id, selected_for_date)
);
-- 2. Indexes for Performance
CREATE INDEX IF NOT EXISTS idx_mbq_status ON morning_briefing_queue(status);
CREATE INDEX IF NOT EXISTS idx_mbq_date_candidate ON morning_briefing_queue(selected_for_date, candidate_id);
-- 3. Comments
COMMENT ON TABLE morning_briefing_queue IS 'Staging area for Morning Briefing generation (Jan 17 Fixed Schema)';