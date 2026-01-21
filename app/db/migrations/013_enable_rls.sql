-- Enable RLS on key tables (good practice)
ALTER TABLE morning_briefing_queue ENABLE ROW LEVEL SECURITY;
ALTER TABLE candidates ENABLE ROW LEVEL SECURITY;
ALTER TABLE target_brokers ENABLE ROW LEVEL SECURITY;
-- Drop existing policies to avoid conflicts
DROP POLICY IF EXISTS "Enable read access for all users" ON morning_briefing_queue;
DROP POLICY IF EXISTS "Enable read access for all users" ON candidates;
DROP POLICY IF EXISTS "Enable read access for all users" ON target_brokers;
-- Create permissive policies for Monitor Mode (allows Anon/Authenticated to READ)
CREATE POLICY "Enable read access for all users" ON morning_briefing_queue FOR
SELECT USING (true);
CREATE POLICY "Enable read access for all users" ON candidates FOR
SELECT USING (true);
CREATE POLICY "Enable read access for all users" ON target_brokers FOR
SELECT USING (true);
-- Allow updates for authenticated users (monitor mode)
CREATE POLICY "Enable update for auth users" ON morning_briefing_queue FOR
UPDATE USING (true);
CREATE POLICY "Enable update for auth users" ON candidates FOR
UPDATE USING (true);
CREATE POLICY "Enable update for auth users" ON target_brokers FOR
UPDATE USING (true);