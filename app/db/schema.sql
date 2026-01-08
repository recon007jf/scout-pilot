-- Scout Backend MVP Schema (Iron Clad)
-- Definition of Truth for "Operation Iron Clad"
-- Jan 5, 2026
-- 1. EXTENSIONS
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
-- 2. ENUMS
DO $$ BEGIN CREATE TYPE identity_type_enum AS ENUM ('email', 'linkedin', 'hash');
EXCEPTION
WHEN duplicate_object THEN null;
END $$;
DO $$ BEGIN CREATE TYPE outreach_status_enum AS ENUM ('active', 'paused');
EXCEPTION
WHEN duplicate_object THEN null;
END $$;
-- 3. TABLES
-- DOSSIERS (Master Record)
-- Replaces rigid CSV structure with a flexible, identifiable record.
CREATE TABLE IF NOT EXISTS dossiers (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    -- Identity (The Diamond Standard)
    identity_key TEXT NOT NULL UNIQUE,
    -- The normalized unique key
    identity_type identity_type_enum NOT NULL,
    -- Source of the key
    -- Data Points
    full_name TEXT NOT NULL,
    firm TEXT,
    role TEXT,
    work_email TEXT,
    linkedin_url TEXT,
    tier TEXT,
    -- 'Tier 1', 'Tier 2'
    -- Status
    is_active BOOLEAN DEFAULT TRUE,
    -- Meta
    raw_data JSONB DEFAULT '{}'::jsonb -- Store original CSV row here
);
-- PSYCHE PR0FILES (The Mind)
-- Stores AI-inferred attributes.
CREATE TABLE IF NOT EXISTS psyche_profiles (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    dossier_id UUID REFERENCES dossiers(id) ON DELETE CASCADE,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    -- Baseline (Deterministic)
    base_archetype TEXT,
    -- 'Analyst', 'Social Climber', etc.
    risk_profile TEXT,
    -- 'Warm', 'Cold-Safe', 'High-Risk'
    -- Adaptive (AI)
    adaptive_modifiers JSONB DEFAULT '{}'::jsonb,
    -- e.g. {"directness": 0.8}
    last_analysis_hash TEXT,
    -- To avoid re-analyzing same data
    UNIQUE(dossier_id)
);
-- PSYCHE HISTORY (Audit Log)
-- Why did the profile change?
CREATE TABLE IF NOT EXISTS psyche_history (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    dossier_id UUID REFERENCES dossiers(id) ON DELETE CASCADE,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    source_type TEXT NOT NULL,
    -- 'ingest', 'note', 'signal'
    reasoning TEXT,
    diff_summary JSONB
);
-- OUTREACH BATCHES (Cadence)
CREATE TABLE IF NOT EXISTS outreach_batches (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    status TEXT DEFAULT 'pending',
    -- 'pending', 'processing', 'completed'
    scheduled_for TIMESTAMP WITH TIME ZONE,
    target_ids JSONB -- List of dossier UUIDs
);
-- GLOBAL OUTREACH STATUS (Safety Brake)
-- Single row table to control the master switch.
CREATE TABLE IF NOT EXISTS global_outreach_status (
    id INTEGER PRIMARY KEY CHECK (id = 1),
    -- Singleton
    status outreach_status_enum DEFAULT 'active',
    paused_at TIMESTAMP WITH TIME ZONE,
    resume_at TIMESTAMP WITH TIME ZONE,
    pause_reason TEXT,
    updated_by TEXT -- User or System
);
-- Ensure singleton exists
INSERT INTO global_outreach_status (id, status)
VALUES (1, 'active') ON CONFLICT (id) DO NOTHING;
-- INTEGRATION TOKENS (OAuth2)
-- Stores refreshed credentials for third-party services.
CREATE TABLE IF NOT EXISTS integration_tokens (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    user_email TEXT NOT NULL,
    provider TEXT NOT NULL,
    -- 'outlook', 'gmail'
    access_token TEXT,
    refresh_token TEXT,
    expires_at TIMESTAMP WITH TIME ZONE,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    UNIQUE(user_email, provider)
);