-- SUPPORT FOR V0 FRONTEND
-- Applied on top of Iron Clad Schema
-- 1. NOTES
CREATE TABLE IF NOT EXISTS dossier_notes (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    dossier_id UUID REFERENCES dossiers(id) ON DELETE CASCADE,
    content TEXT NOT NULL,
    author TEXT DEFAULT 'User',
    -- 'User' or 'System'
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);
-- 2. USER SETTINGS
CREATE TABLE IF NOT EXISTS user_preferences (
    user_email TEXT PRIMARY KEY,
    -- Single user mode, use email as key
    preferences JSONB DEFAULT '{}'::jsonb,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);
-- 3. DRAFT ACTIONS / APPROVALS
-- Tracks the decision made on a Briefing Target
CREATE TABLE IF NOT EXISTS draft_decisions (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    dossier_id UUID REFERENCES dossiers(id) ON DELETE CASCADE,
    decision TEXT NOT NULL,
    -- 'approved', 'dismissed', 'paused'
    draft_content TEXT,
    -- Snapshot of what was approved
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);
-- 4. Enable RLS or Policies if needed (Skipping for MVP/Iron Clad)