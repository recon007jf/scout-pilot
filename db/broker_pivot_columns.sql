ALTER TABLE public.target_brokers
ADD COLUMN IF NOT EXISTS work_email text,
    ADD COLUMN IF NOT EXISTS linkedin_url text;
-- Reload cache just in case
NOTIFY pgrst,
'reload schema';