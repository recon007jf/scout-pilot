-- 1. Drop existing trigger to ensure clean slate (optional but safe)
-- DROP TRIGGER IF EXISTS on_auth_user_created ON auth.users;
-- 2. Redefine the Function to match Backend Metadata
CREATE OR REPLACE FUNCTION public.handle_new_user() RETURNS TRIGGER AS $$
DECLARE metadata_org_id UUID;
metadata_role TEXT;
BEGIN -- Extract values from the 'data' object passed in admin.invite_user_by_email
-- Backend sends: data: { "org_id": "...", "role": "..." }
metadata_org_id := (new.raw_user_meta_data->>'org_id')::UUID;
metadata_role := new.raw_user_meta_data->>'role';
-- Default fallback (Safety)
IF metadata_role IS NULL THEN metadata_role := 'member';
END IF;
-- Insert into public.profiles
-- Note: We rely on the fact that 'organizations' table uses 'id' (Verified by script)
-- If profiles.org_id is NOT NULL, this INSERT works only if metadata_org_id is valid.
INSERT INTO public.profiles (id, email, org_id, role, full_name)
VALUES (
        new.id,
        new.email,
        metadata_org_id,
        metadata_role,
        new.raw_user_meta_data->>'full_name'
    );
RETURN new;
END;
$$ LANGUAGE plpgsql SECURITY DEFINER;
-- 3. Ensure Trigger is Bound (Idempotent)
DROP TRIGGER IF EXISTS on_auth_user_created ON auth.users;
CREATE TRIGGER on_auth_user_created
AFTER
INSERT ON auth.users FOR EACH ROW EXECUTE FUNCTION public.handle_new_user();