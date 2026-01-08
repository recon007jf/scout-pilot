-- INSTRUCTION: FINAL REMEDIATION (Strict Schema Patch)
-- STATUS: Backend Acceptance: CONDITIONALLY APPROVED.
-- Requirement: Columns MUST match exactly.
-- 0. EXTENSION (required for uuid_generate_v4)
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
-- 1. PSYCHE HISTORY (Audit Log - Canonical)
CREATE TABLE IF NOT EXISTS psyche_history (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    dossier_id UUID REFERENCES dossiers(id) ON DELETE CASCADE,
    timestamp TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    source_type TEXT NOT NULL,
    delta_applied JSONB NOT NULL,
    source_content TEXT
);
-- 2. OUTREACH BATCHES (Queue - Canonical)
CREATE TABLE IF NOT EXISTS outreach_batches (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    status TEXT NOT NULL DEFAULT 'pending',
    scheduled_for TIMESTAMPTZ,
    target_ids JSONB
);
-- 3. HELIX USAGE STATS (Meter - Canonical)
CREATE TABLE IF NOT EXISTS helix_usage_stats (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    endpoint TEXT,
    status_code INTEGER,
    latency_ms INTEGER
);