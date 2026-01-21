-- SIGNALS INTELLIGENCE SCHEMA
-- Stores reactive news/events found by the Analyst
CREATE TABLE IF NOT EXISTS candidate_signals (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    candidate_id UUID REFERENCES candidates(id) ON DELETE CASCADE,
    -- Stage 1: Capture (Source)
    title TEXT NOT NULL,
    url TEXT NOT NULL,
    source TEXT,
    -- 'Google News', 'Bloomberg', 'Tavily'
    published_at TIMESTAMP WITH TIME ZONE,
    -- Stage 2: Interpretation (Analyst)
    signal_type TEXT,
    -- 'M&A', 'GROWTH', 'REGULATORY', 'NOISE', 'PERSONNEL'
    relevance_score INTEGER DEFAULT 0,
    -- 0-100
    impact_rating TEXT,
    -- 'HIGH', 'MEDIUM', 'LOW', 'IGNORE'
    analysis TEXT,
    -- "This acquisition implies..."
    action_suggested TEXT,
    -- "Mention integration challenges..."
    -- Meta
    raw_content TEXT,
    -- Stored text from Tavily (optional, for debugging/re-analysis)
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);
-- Index for fast lookup by candidate
CREATE INDEX IF NOT EXISTS idx_candidate_signals_candidate_id ON candidate_signals(candidate_id);