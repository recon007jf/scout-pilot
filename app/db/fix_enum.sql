ALTER TABLE candidates DROP CONSTRAINT IF EXISTS candidates_status_check;
ALTER TABLE candidates ADD CONSTRAINT candidates_status_check CHECK (status IN ('POOL', 'QUEUED', 'DRAFTED', 'SENT', 'FAILED', 'BLOCKED_BOUNCE_RISK', 'IGNORED'));
