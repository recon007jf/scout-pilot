-- FORCE VISIBILITY
-- 1. Ensure table is in public schema
ALTER TABLE public.target_brokers
SET SCHEMA public;
-- 2. Grant everything to service_role (and authenticated for debugging if needed, but mainly service)
GRANT ALL ON TABLE public.target_brokers TO service_role;
GRANT ALL ON TABLE public.target_brokers TO postgres;
GRANT ALL ON TABLE public.target_brokers TO anon;
GRANT ALL ON TABLE public.target_brokers TO authenticated;
-- 3. Grant RPC execute explicitly again
GRANT EXECUTE ON FUNCTION public.claim_broker_batch(text, text, integer) TO service_role;
GRANT EXECUTE ON FUNCTION public.claim_broker_batch(text, text, integer) TO anon;
GRANT EXECUTE ON FUNCTION public.claim_broker_batch(text, text, integer) TO authenticated;
-- 4. Disable RLS temporarily to rule it out (service_role bypasses it, but let's be sure)
ALTER TABLE public.target_brokers DISABLE ROW LEVEL SECURITY;
-- 5. Force cache reload
NOTIFY pgrst,
'reload schema';