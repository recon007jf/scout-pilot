-- 006_user_identities.sql
-- Map Clerk Identities to Internal Users
-- This table allows us to link a Clerk User ID (string) to our internal UUIDs if needed,
-- or simply serve as a cache of known users.
CREATE TABLE IF NOT EXISTS user_identities (
    clerk_user_id TEXT PRIMARY KEY,
    -- The 'sub' claim from Clerk
    email TEXT,
    internal_user_id UUID DEFAULT gen_random_uuid(),
    -- Our internal stable ID
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    last_seen_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    metadata JSONB DEFAULT '{}'::jsonb
);
-- Index for email lookups
CREATE INDEX IF NOT EXISTS idx_user_identities_email ON user_identities(email);
-- Enable RLS (Service Role Only by default)
ALTER TABLE user_identities ENABLE ROW LEVEL SECURITY;
-- Policy: Service Role has full access (Implicit in Supabase, but good practice to be explicit if using standard Postgres)
-- Note: We are accessing this via Service Role Key in Backend, so RLS policies don't strictly apply to the backend,
-- but prevent Client Access via Supabase Client.