-- 007_rpc_get_user_id.sql
-- 1. Diagnostic: Get Current Context (Role & UID)
-- Callable by anyone to check their own context
CREATE OR REPLACE FUNCTION get_my_claims() RETURNS json LANGUAGE plpgsql SECURITY DEFINER
SET search_path = public,
    auth AS $$ BEGIN RETURN json_build_object(
        'role',
        auth.role(),
        'uid',
        auth.uid(),
        'jwt',
        current_setting('request.jwt.claims', true)
    );
END;
$$;
GRANT EXECUTE ON FUNCTION get_my_claims() TO anon,
    authenticated,
    service_role;
-- 2. Secure RPC to lookup User ID by Email
CREATE OR REPLACE FUNCTION get_user_id_by_email(email_input TEXT) RETURNS UUID LANGUAGE plpgsql SECURITY DEFINER
SET search_path = public,
    auth AS $$
DECLARE found_id UUID;
BEGIN
SELECT id INTO found_id
FROM auth.users
WHERE email = email_input
LIMIT 1;
RETURN found_id;
END;
$$;
GRANT EXECUTE ON FUNCTION get_user_id_by_email(TEXT) TO service_role;