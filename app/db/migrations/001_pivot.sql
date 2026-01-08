-- MIGRATION: 001_pivot
-- Date: Jan 6, 2026
-- Description: V0 Support + Refinery Queue + Audit Compliance
-- 1. MORNING BRIEFING QUEUE (Refinery Output)
-- Acts as the staging area for email generation.
CREATE TABLE IF NOT EXISTS morning_briefing_queue (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    dossier_id UUID REFERENCES dossiers(id) ON DELETE CASCADE,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    priority_score INTEGER DEFAULT 0,
    status TEXT DEFAULT 'pending',
    -- 'pending', 'sent', 'failed'
    -- Processing Metadata
    selected_for_date DATE DEFAULT CURRENT_DATE,
    UNIQUE(dossier_id) -- Avoid queuing same person twice
);
-- 2. DOSSIER NOTES (V0 Feature)
CREATE TABLE IF NOT EXISTS dossier_notes (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    dossier_id UUID REFERENCES dossiers(id) ON DELETE CASCADE,
    content TEXT NOT NULL,
    author TEXT DEFAULT 'User',
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);
-- 3. USER PREFERENCES (V0 Feature)
CREATE TABLE IF NOT EXISTS user_preferences (
    user_email TEXT PRIMARY KEY,
    preferences JSONB DEFAULT '{}'::jsonb,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);
-- 4. DRAFT DECISIONS (Audit Log)
CREATE TABLE IF NOT EXISTS draft_decisions (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    dossier_id UUID REFERENCES dossiers(id) ON DELETE CASCADE,
    decision TEXT NOT NULL,
    -- 'approved', 'dismissed', 'paused', 'contacted'
    draft_content TEXT,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);
-- 5. INDEXES (Performance)
CREATE INDEX IF NOT EXISTS idx_mbq_status ON morning_briefing_queue(status);
CREATE INDEX IF NOT EXISTS idx_notes_dossier ON dossier_notes(dossier_id);