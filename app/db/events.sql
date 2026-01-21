-- EVENT SCOUT SCHEMA
-- Stores the "North Star" industry events for proactive triangulation
CREATE TABLE IF NOT EXISTS industry_events (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    name TEXT NOT NULL,
    start_date DATE NOT NULL,
    end_date DATE,
    city TEXT NOT NULL,
    state TEXT NOT NULL,
    -- 'TN', 'CO', etc.
    venue TEXT,
    match_tags TEXT [],
    -- ['ROSETTA', 'TECH', 'EXECUTIVE', 'BENEFITS']
    description TEXT,
    website_url TEXT,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);
-- SEED DATA (The "Must-Watch" 5)
INSERT INTO industry_events (
        name,
        start_date,
        end_date,
        city,
        state,
        venue,
        match_tags,
        description
    )
VALUES (
        'RosettaFest 2026',
        '2026-07-29',
        '2026-07-31',
        'Nashville',
        'TN',
        'The Grand Hyatt',
        ARRAY ['ROSETTA', 'BENEFITS'],
        'The premier event for Health Rosetta advisors.'
    ),
    (
        'CIAB Employee Benefits Leadership Forum',
        '2026-05-26',
        '2026-05-29',
        'Colorado Springs',
        'CO',
        'The Broadmoor',
        ARRAY ['EXECUTIVE', 'BENEFITS'],
        'Strategic gathering for C-Suite Execs in Benefits.'
    ),
    (
        'InsurTech Insights',
        '2026-06-03',
        '2026-06-04',
        'New York',
        'NY',
        'Javits Center',
        ARRAY ['TECH', 'INNOVATION'],
        'Large scale InsurTech conference.'
    ),
    (
        'CIAB Insurance Leadership Forum',
        '2026-10-02',
        '2026-10-06',
        'Colorado Springs',
        'CO',
        'The Broadmoor',
        ARRAY ['EXECUTIVE', 'BROKERAGE'],
        'The "Big Dog" event for top-tier brokerage leadership.'
    ),
    (
        'Benefits at Work (EBN)',
        '2026-09-15',
        '2026-09-16',
        'Las Vegas',
        'NV',
        'Aria Resort',
        ARRAY ['BENEFITS', 'HR'],
        'General benefits conference for HR and Brokers.'
    ) ON CONFLICT DO NOTHING;
-- (No unique constraint defined on name yet, but good practice if we add one)