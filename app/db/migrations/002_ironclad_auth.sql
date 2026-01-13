-- 1. Organizations (Correct Name: Pacific AI Systems)
create table if not exists public.organizations (
    id uuid primary key default gen_random_uuid(),
    name text not null,
    created_at timestamptz default now()
);
-- 2. Insert Bootstrap Organization (Deterministic)
insert into public.organizations (name)
values ('Pacific AI Systems') on conflict do nothing;
-- 3. Profiles (Linked to Auth)
create table if not exists public.profiles (
    id uuid primary key references auth.users(id) on delete cascade,
    org_id uuid references public.organizations(id) not null,
    role text check (role in ('admin', 'member')) not null default 'member',
    full_name text,
    email text,
    created_at timestamptz default now()
);
-- 4. Invites (Audit Trail)
create table if not exists public.invites (
    email text primary key,
    org_id uuid references public.organizations(id) not null,
    role text not null default 'member',
    status text check (status in ('pending', 'accepted')) default 'pending',
    supabase_invite_id uuid,
    -- Optional/Nullable (Store if available)
    created_by uuid references auth.users(id),
    created_at timestamptz default now()
);
-- 5. RLS Policies
alter table public.profiles enable row level security;
drop policy if exists "Users can view own profile" on public.profiles;
create policy "Users can view own profile" on public.profiles for
select using (auth.uid() = id);
alter table public.invites enable row level security;
-- (Write access handled via Service Role in API)
-- 6. Dossier Backfill (Deterministic)
alter table dossiers
add column if not exists org_id uuid references public.organizations(id);
update dossiers
set org_id = (
        select id
        from public.organizations
        where name = 'Pacific AI Systems'
        limit 1
    )
where org_id is null;