-- Migration: Add Batch Protocol columns to target_brokers
-- Date: 2026-01-19
-- Purpose: Enable "Max 20" Batch Protocol via direct querying.
ALTER TABLE target_brokers
ADD COLUMN IF NOT EXISTS selected_for_date DATE;
ALTER TABLE target_brokers
ADD COLUMN IF NOT EXISTS batch_number INTEGER;
-- Index for performance (Frontend queries by date + order by batch)
CREATE INDEX IF NOT EXISTS idx_tb_date_batch ON target_brokers(selected_for_date, batch_number);
-- Comment
COMMENT ON COLUMN target_brokers.batch_number IS 'Batch 1-5 for Morning Briefing (10 items per batch)';