-- OUTLOOK PROBE PATCH
-- Adds table for storing Azure AD Tokens.
-- Run this in Production Supabase SQL Editor.
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