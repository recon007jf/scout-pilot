-- Enable UUID extension for unique IDs
create extension if not exists "uuid-ossp";
-- 1. Create Targets Table (The People)
create table if not exists targets (
    id uuid default gen_random_uuid() primary key,
    company_name text,
    person_name text,
    email text,
    title text,
    linkedin_url text,
    pdl_confidence float,
    last_contacted_at timestamp with time zone,
    do_not_contact boolean default false,
    raw_data jsonb
);
-- 2. Create Outreach Queue Table (The Drafts)
create table if not exists outreach_queue (
    id uuid default gen_random_uuid() primary key,
    target_id uuid references targets(id),
    status text check (
        status in (
            'draft',
            'approved',
            'queued',
            'sent',
            'rejected'
        )
    ),
    priority_score integer,
    system_confidence float,
    selection_reason text,
    email_subject text,
    email_body text,
    scheduled_at timestamp with time zone,
    created_at timestamp with time zone default now(),
    updated_at timestamp with time zone default now()
);
-- 3. Create Inbound Replies Table (The Signals)
create table if not exists inbound_replies (
    id uuid default gen_random_uuid() primary key,
    target_id uuid references targets(id),
    email_body text,
    response_type text,
    next_action text
);