-- MIGRATION: 009_update_tokens_schema
-- Date: Jan 17, 2026
-- Description: Add metadata columns for Outlook OAuth Verification (Scopes, Expiry)
-- Ensure table exists (Recovery from manual creation)
CREATE TABLE IF NOT EXISTS integration_tokens (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    user_email TEXT NOT NULL,
    provider TEXT NOT NULL,
    access_token TEXT NOT NULL,
    refresh_token TEXT,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    UNIQUE(user_email, provider)
);
-- Add verification columns
ALTER TABLE integration_tokens
ADD COLUMN IF NOT EXISTS scopes TEXT;
ALTER TABLE integration_tokens
ADD COLUMN IF NOT EXISTS expires_at TIMESTAMP WITH TIME ZONE;