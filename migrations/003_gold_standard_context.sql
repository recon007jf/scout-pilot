-- Migration: 003_gold_standard_context
-- Purpose: Add 'Sponsor Context' (Linkage) and 'Contact Reachability' (Mobile) fields to the targets table.
-- 1. Add 'sponsor_linkage' column (JSONB)
-- Stores the original plan/sponsor details (e.g., {"sponsor_name": "STARBUCKS", "lives": 228674, "ein": "..."})
-- This allows the UI to display "The Hook" directly from the contact record.
ALTER TABLE targets
ADD COLUMN IF NOT EXISTS sponsor_linkage JSONB;
-- 2. Add 'mobile_phone' column (Text)
-- Specific field for high-value mobile numbers enriched via Clay/Datagma.
ALTER TABLE targets
ADD COLUMN IF NOT EXISTS mobile_phone TEXT;
-- 3. Add 'validation_flags' column (JSONB) - Optional but good for cleanliness protocols
-- To store flags like {"email_valid": true, "mobile_valid": false}
ALTER TABLE targets
ADD COLUMN IF NOT EXISTS validation_flags JSONB DEFAULT '{}'::jsonb;