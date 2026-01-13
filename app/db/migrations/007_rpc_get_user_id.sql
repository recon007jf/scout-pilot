-- 007_rpc_get_user_id.sql
-- Secure RPC to lookup User ID by Email
-- Only callable by Service Role (if we restrict access)
CREATE OR REPLACE FUNCTION get_user_id_by_email(email_input TEXT) RETURNS UUID LANGUAGE plpgsql SECURITY DEFINER -- Runs with privileges of creator (postgres/admin)
SET search_path = public,
    auth -- Secure search path
    AS $$
DECLARE found_id UUID;
BEGIN
SELECT id INTO found_id
FROM auth.users
WHERE email = email_input
LIMIT 1;
RETURN found_id;
END;
$$;
-- Grant execution to service_role only (optional refinement)
-- REVOKE EXECUTE ON FUNCTION get_user_id_by_email(TEXT) FROM PUBLIC;
-- GRANT EXECUTE ON FUNCTION get_user_id_by_email(TEXT) FROM service_role;