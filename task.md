# Tasks

- [x] Create project files
  - [x] Create leads.csv
  - [x] Create agent.py
  - [x] Create requirements.txt
- [x] Install dependencies
- [x] Verify credentials and run script
  - [x] Credentials format check passed
  - [x] Deploy application (Success: <https://scout-dashboard-283427197752.us-central1.run.app>)
  - [x] Fix Access Issues (Made Public + Password Protected)
  - [x] Deploy application (Success: <https://scout-dashboard-283427197752.us-central1.run.app>)
  - [x] Fix Access Issues (Made Public + Password Protected)
  - [x] Verify Password Protection on Cloud Run (Confirmed Live)
- [x] Final Polish of UI
  - [x] Implement "Retry" button for failed leads
  - [x] Verify mobile responsiveness (Streamlit default)
- [x] Implement Bernays Protocol (Deep Intelligence)
  - [x] Expand `recon_agent.py` gathering (Podcasts, News, Speaking)
  - [x] Update `analyze_lead` with Archetype Prompt (Controller, Social Climber, Guardian, Analyst)
  - [x] Update `app.py` to display Archetype Badges & Legend
  - [x] Verify on test lead
- [x] Add Podcast Details
  - [x] Update `recon_agent.py` to extract & save Podcast Name/URL
  - [x] Update `app.py` to display "🎙️ Podcast Host" section
- [x] Personalize Email Salutations
  - [x] Update `write_email` to accept `first_name`
  - [x] Update prompt to enforce "Hi {first_name}"
  - [x] Update `process_single_lead` to pass name
- [x] Improve Login UX
  - [x] Wrap login inputs in `st.form` to enable "Enter" key submission
- [x] Add Archetype Evidence
  - [x] Update `analyze_lead` to extract evidence/quotes
  - [x] Update `process_single_lead` to save evidence to dossier
  - [x] Regenerate all leads with new evidence
- [x] Beautify Dossier Display
  - [x] Parse dossier text into structured data
  - [x] Render with custom HTML/CSS card design
- [x] Implement Smart Retry Logic
  - [x] Update `recon_agent.py` with exponential backoff for API calls
  - [x] Handle 429 errors gracefully
- [x] Implement Budget Safety Guard
  - [x] Add `SAFETY_LIMIT` constant to `recon_agent.py`
  - [x] Enforce max leads per run (default 50)
- [x] Add Bulk Regeneration UI
  - [x] Add "Bulk Actions" section to Sidebar
  - [x] Implement "Regenerate Incomplete Leads" button (Batch process)
- [x] Implement Email Guessing & Explanation
  - [x] Add `guess_email_logic` to `recon_agent.py`
  - [x] Update `process_single_lead` to populate missing emails with Guess + Reason
  - [x] Beautify display in `app.py` (Bold Email, Italic Reason)
- [x] Implement Manual Email Override
  - [x] Add "Edit Email" input field to `app.py` lead details
  - [x] Implement update logic to save manual entry to Google Sheet
- [x] Implement Manual LinkedIn Override
  - [x] Update `app.py` to include "Edit LinkedIn" field
  - [x] Combine with Email Edit for a unified "Manual Updates" form
  - [x] Create .gitignore
  - [x] Initialize repo and commit

- [ ] Integrate Serper API
  - [x] Install python-dotenv
  - [x] Create .env for API key
  - [x] Install python-dotenv
  - [x] Create .env for API key
  - [x] Update agent.py with search logic (Query, Extraction, Fallback)
  - [x] Run script (Limit 5 leads)
- [x] Process all leads
  - [x] Remove limit in agent.py
  - [x] Run script for full list
- [ ] Add LinkedIn Search
  - [x] Update process_lead with LinkedIn query
  - [x] Update write_to_google_sheet with new column
  - [x] Run script to overwrite data
- [ ] Refine LinkedIn Search
  - [x] Update query to be broader (remove strict quotes)
  - [x] Re-run script (Success: Improved results)
- [x] Save progress to GitHub
  - [x] Commit and push new features
- [ ] Implement Email Validation
  - [x] Add validation logic (length check, name matching)
  - [x] Filter generic/junk emails
  - [x] Re-run script to clean up data
- [ ] Refine Email Search
  - [x] Remove "Guessing" logic (Strict "Not Found")
  - [x] Add LinkedIn X-Ray search (site:linkedin.com/in/... "@")
  - [x] Re-run script (Success: X-Ray active)
- [ ] Build Deep Recon Module
  - [x] Update requirements (google-generativeai)
  - [x] Update .env with Gemini Key
  - [x] Create recon_agent.py (Serper + Gemini Analyst + Gemini Copywriter)
  - [x] Run on first 3 leads (Success: Dossiers & Drafts generated)
- [ ] Refine Deep Recon Output
  - [x] Format Dossier Summary as readable text (not JSON)
  - [x] Run on next batch of leads (Success: Readable dossiers)
- [ ] Build Scout Dashboard
  - [x] Update requirements (streamlit)
  - [x] Create app.py (Streamlit UI)
  - [x] Implement Google Sheets connection
  - [x] Add "Export to CSV" features
  - [x] Style with custom CSS
  - [x] Launch Dashboard (<http://localhost:8502>)
- [ ] Redesign Scout Dashboard UI
  - [x] Analyze example URLs (Crypto Table aesthetic)
  - [x] Create "Sisyphus" CSS (Deep Navy, Rating Badges, Pill Labels)
  - [x] Update app.py to use "Sisyphus" layout
  - [x] Add "Quick Actions" (Export/View)
- [ ] Add LinkedIn Profile Photos
  - [x] Update recon_agent.py to search for profile images
  - [x] Update Google Sheet with "Profile Image" column
  - [x] Update app.py to display photos in the dashboard
  - [x] Update CSS for image avatars
- [ ] Refine Dashboard Layout
  - [x] Merge "Row" and "Details" visually (remove gaps/borders)
  - [x] Update CSS to create a unified "Card" look
  - [x] Move actions inside the details area if needed
- [ ] Clean Up Dossier Formatting
  - [x] Create fix_dossier_format.py (JSON -> Text, Strip Markdown)
  - [x] Run cleanup script on all rows
  - [x] Update recon_agent.py to enforce plain text output
- [ ] Debug & Fix Data Issues
  - [x] Fix agent.py (saving Title instead of URL?)
  - [x] Update recon_agent.py Prompt (Ignore "Welcome user" / login text)
  - [x] Re-run Recon for Kevin Overbey (and full list) - DONE (Some 429 errors on emails)
- [ ] Refine Search Accuracy
  - [ ] Update search queries to include "Present" to target current role
  - [x] Update analysis prompt to explicitly filter for target firm
- [x] Implement Automated Data Integrity Audit
  - [x] Create `integrity_check.py` to re-verify all leads
  - [x] Logic: Compare "Target Firm" vs. "Extracted Current Firm" from LinkedIn
  - [x] Auto-flag mismatches for review or auto-regenerate
  - [x] Add "Run Integrity Check" to Dashboard Sidebar
- [ ] Implement CSV Import Feature
  - [ ] Add "Upload CSV" widget to Sidebar
  - [ ] Logic: Validate columns, clean data, append to Google Sheet
  - [ ] **Smart Enrichment:** Update `recon_agent.py` to auto-find LinkedIn URL if missing
  - [ ] Ensure "Fix Incomplete Leads" triggers full recon (Search -> Intel -> Dossier -> Email)
- [ ] **Discuss: Microsoft Azure Integration**
  - [ ] Analyze pitfalls: OAuth2 complexity, Token management, Permissions (`Mail.ReadWrite`)
  - [ ] Discuss deployment challenges (Redirect URIs, Secrets)
  - [ ] Evaluate UX impact (Login flow in Streamlit)
- [x] **Execute Forensic Probe v3** (Shadow Mode) <!-- id: 11 -->
  - [x] Create `scout_probe_v3.py` (No assumptions, raw logging) <!-- id: 12 -->
  - [x] Run on "Hostile Dataset" (6 rows) <!-- id: 13 -->
  - [x] Analyze Billing Headers & Data resolution <!-- id: 14 -->
- [x] **Launch Scout Production Engine** (Final) <!-- id: 15 -->
  - [x] Implement `scout_production_engine_final.py` (Waterfall + ID Search) <!-- id: 16 -->
  - [x] Verify on Hostile Data (0 errors) <!-- id: 17 -->
    - [x] Run Canary Batch (Real DOL Data) <!-- id: 18 -->
    - [x] Execute Full Production Run (v4 Slow) <!-- id: 19 -->
    - [x] **Final Code Freeze (v7.4 PLATINUM)**
      - [x] Implement `scout_production_engine_v7_final.py` (Waterfall + Exact Match)
      - [x] Verify Output (Rivian/Netflix/Opendoor)
      - [x] Confirm Resilience (Allbirds Backoff)
    - [x] **Deploy Broker Hunter (v8.6 Safe Scrape)**
      - [x] Implement `scout_broker_hunter_v8_safe_scrape.py` (Domain Whitelist)
      - [x] Verify Output (Found USI for Rivian/Chipotle)
    - [x] **Deploy Golden Master (v8.12)**
      - [x] Implement `scout_broker_hunter_v8_12_golden.py` (Header Validation + Anomaly Check)
      - [x] Generate `Morning_Briefing_With_Brokers.csv` (16-Column Spec)
- [x] **Upgrade Target Schema (Gold Standard)**
  - [x] Create `migrations/003_gold_standard_context.sql` (Sponsor Linkage, Mobile)
  - [x] Update `run_handshake_2023.py` (Firm Normalization, Email Validation)
- [x] **Refactor Broker Hunter (v9)**
  - [x] Create `src/scout/broker_hunter.py` (Validator Upgrade)
  - [x] Create `scripts/execute_broker_hunter.py` (Primacy Logic)
  - [x] Verify Output (CSV Alignment + Context Pass-through)
  - [x] **Final Polish (v9.1)**
    - [x] Expand Stopwords ("What", "These", etc.)
    - [x] Enforce 9-Column Gold Schema
    - [x] Validate Output (Clean CSV confirmed)
- [ ] **Inject Domain Knowledge (Andrew's Rules)**
  - [ ] Update `recon_agent.py` prompt with Carrier Rules (Anthem States, Cigna Flexibility, Aetna Caution)
  - [ ] Implement "Cannibalization Rule" (No Anthem FI -> Anthem Self-Funded)
  - [ ] Implement "FairPoint" logic for Controller/Analyst profiles (RBP, Savings, Front Desk objection)
  - [ ] Apply Logic Mapping (State-based & Archetype-based pitching)
- [x] **Clay Integration** (DEPRECATED: Pivoting to PDL)
  - [x] Configure `CLAY_WEBHOOK_URL` in `.env`
  - [x] Configure `CLAY_API_KEY` in `.env`
  - [x] Verify `execute_clay_live_fire.py` configuration loading
  - [x] Run Clay Live Fire test (Success: 10 rows sent)

- [x] **Broker Pivot Workflow (Concurrency Safe)**
  - [x] Create `broker_pivot.sql` (Table, Index, RPC)
  - [x] Create `execute_broker_live_fire.py` (Launcher)
  - [x] Execute SQL Infrastructure (User execution confirmed)
  - [x] Run `execute_broker_live_fire.py` (10-row batch)
  - [x] Verify results (Status check, Zombie check)
  - [x] Ingest Enriched Data (`execute_broker_receiver.py`)
  - [x] Verify Enriched Data (Email/LinkedIn populated)

- [x] **2023 Data Pivot (Sniper Scope v2)**
  - [x] Locate Schedule A Part 1 (`F_SCH_A_PART1_2023_latest.csv`)
  - [x] Plan: Update Handshake to use Part 1 Broker Data
  - [x] Script: `run_handshake_2023.py` (Join Part 1 + Humans)
  - [x] Script: `run_sniper_2023.py` (Rank & Shortlist)
  - [x] Verification: Compare row counts (expecting ~700-1000 rows, got 8,274)
  - [x] Output: `Leads_Shortlist_2023.csv`

- [x] **Pilot Generation**
  - [x] Generate Top 50 Unique Humans (`Pilot_50_For_Clay_v8_1.csv`)
  - [x] Generate Ready-To-Send List (`Pilot_50_Ready_To_Send.csv`)

- [ ] **People Data Labs (PDL) Migration** (REVENUE LOOP PRIORITY)
  - [x] **Research & Planning**
    - [x] Research PDL Python SDK & Enrichment API
    - [x] Create `pdl_migration_plan.md`
  - [x] **Infrastructure**
    - [x] Install `peopledatalabs` python package
    - [x] Configure `PDL_API_KEY` in `.env`
  - [x] **Implementation (Revenue Loop)**
    - [x] Create `app/services/enrichment_service.py` (Requests-based)
    - [x] Implement `find_person` with Title/Company search logic
    - [x] Create `scripts/verify_pdl.py` (Sanity Check - Apple/OpenAI)
  - [ ] **Integration**
    - [ ] Update `recon_agent.py` to use PDL instead of deep recon
    - [ ] Create `execute_pdl_batch.py` (Replacements for Clay scripts)

## Next Steps (Planning Week)

- [ ] **Code Audit & Cleanup (New Year Clean Slate)**
  - [x] Create `code_audit_plan.md`
  - [x] Inventory legacy scripts (`v1`, `v2`, etc.)
  - [x] Identify `debug_*.py` files for deletion/archival
  - [x] Propose new directory structure (Move scripts to `scripts/`, core logic to `app/`)
- [ ] Draft PDL Migration Plan
- [ ] Review Andrew's Rules Logic
- [x] **Territory Map Backend** (UI Handoff)
  - [ ] **Implementation Plan**
    - [x] Create `implementation_plan_territory.md`
    - [x] Schema Update (`leads_pilot.db`: `geo_lat`, `geo_lng`, `geo_precision`, `geo_source`, `geo_last_geocoded_at`, `geo_error`, `geo_address_hash`)
  - [x] **Core Service**
    - [x] Create `app/services/territory_service.py`
    - [x] Implement `get_territory_points(filters)`
    - [x] Implement `geocode_lead(lead_id)` with caching
  - [x] **Infrastructure**
    - [x] Configure `GOOGLE_MAPS_SERVER_KEY` in `.env`
    - [x] Create `scripts/backfill_geocoding.py` (One-time runner)
  - [x] **API Surface**
    - [x] Create `scripts/generate_territory_json.py` to dump JSON for UI
    - [x] Deploy `api_server.py` (FastAPI) for `/api/accounts/territory`
- [ ] Review CSV Import UI Logic

- [x] **Deploy Scout v4 Fusion Engine**
  - [x] Implement `execute_scout_fusion.py` (Atomic Ledger)
  - [x] Verify Input Data (`BF_RAW_*.csv`)
  - [x] Execute Batch (Yield: 48 Targets)

- [x] **Deploy V3 Definitive Funding Engine**
  - [x] Document Logic (`docs/funding_inference.md`)
  - [x] Implement `finalize_batch_v3_hardened.py`
  - [x] Hotfix for 2023 Indicator Columns (`FUNDING_*_IND`)
  - [x] Validate Output (100% Match Rate)

- [x] **Deploy Production Territory Resolution**
  - [x] Implement `resolve_territory_production_v2.py`
  - [x] Execute Waterfall (Internal -> Serper -> PDL)
  - [x] Generate `Target_Hunting_List_Production_v1.csv`

- [x] **Canonical V3 Assembly (Big Bang)**
  - [x] **Inventory**: Scan & Identify Golden Source (`BF_RAW_Leads.csv`)
  - [x] **Assembly**: execute `execute_v3_canonical_assembly.py` (Join Leads + Cache + DOL)
  - [x] **Patch**: Fix Email Fallback & Schema (`execute_v3_export_patch.py`)
  - [x] **Harvest**: Run LinkedIn Safety Harvester (`execute_production_linkedin_harvester.py`)
  - [x] **Deliver**: `artifacts/Master_Hunting_List_Production_v3_ANDREW_ENRICHED.csv` (100% Quality)

- [x] **Forensic Funding Patch**
  - [x] Integrate 2023 Form 5500 Spine (`f_5500_2023_latest.csv`)
  - [x] Deliver 6,955 Verified High-Value Targets (`_FUNDING_PATCHED.csv`)

- [x] **Cloud Run Deployment Prep**
  - [x] Validate `requirements.txt`
  - [x] Create Multi-Stage `Dockerfile`
  - [x] Harden `api_server.py` (Health Check, CORS, Logging)
  - [x] Generate `deployment_readiness.md`

- [x] **Cloud Run Production Launch**
  - [x] Build Artifact (`scout/backend:latest`)
  - [x] Deploy Service (`scout-backend`)
  - [x] Verify Health Probe (`/health` OK)
  - [x] **Status:** LIVE at `https://scout-backend-283427197752.us-central1.run.app`

# Phase 2: Operation IRON CLAD (Backend MVP)

- [ ] **Step 1: Foundation & Schema**
  - [x] **Infrastructure**: `config.py` (Secrets), `logger.py` (JSON)
  - [x] **Domain**: `product_facts.py` (Point C Truth)
  - [x] **DB**: `schema.sql` (Verifiable Truth) with `dossiers`, `psyche_history`, `outreach_batches`.

- [ ] **Step 2: Core Logic Modules**
  - [x] **Identity**: `identity.py` + `seed_targets.py` (Idempotent Ingestion)
  - [x] **Safety**: `safety.py` (State Machine + Time Travel)
  - [x] **Drafting**: `draft_engine.py` (Pydantic + Product Facts)
  - [x] **Briefing**: `briefing.py` (Ranking Logic)
  - [x] **API**: `main.py` (Skeleton)

- [ ] **Step 3: The Smoke Test Gate (Verification)**
  - [x] **Test**: `tests/smoke_test_core.py`
  - [x] **Constraint**: Zero Assertions Failed

# Phase 3: Operation SKY LIFT (Secure Deployment)

- [x] **Step 1: IAM & Secrets (The Keys)**
  - [x] **APIs**: Enable `cloudresourcemanager`, `secretmanager`, `run`.
  - [x] **Auth**: Create `scout-backend-runtime` SA.
  - [x] **Secrets**: Create & Populate `SUPABASE_URL`, `SUPABASE_SERVICE_KEY`, `GEMINI_API_KEY`, `SUPABASE_KEY`.

- [x] **Step 2: Idempotent Schema (The Blueprint)**
  - [x] **SQL**: `backend/app/db/schema.sql`.
  - [x] **Execute**: Run against Production Supabase.

- [x] **Step 3: Containerization (The Vessel)**
  - [x] **Context**: `Dockerfile`, `requirements.txt`.
  - [x] **Build**: Cloud Build / Artifact Registry.

- [x] **Step 4: Secure Deployment (The Lift)**
  - [x] **Command**: `gcloud run deploy`.
  - [x] **Security**: Bind Secrets, No Unauth.

- [x] **Step 5: Controlled Hydration (The Exception)**
  - [x] **Script**: `seed_targets.py` (One-time).
  - [x] **Verification**: Integrity Check (PASSED).

- [x] **Step 6: Authenticated Verification (The Check)**
  - [x] **Health**: `curl` with Identity Token.

**Status**: ✅ **OPERATION COMPLETE**. Backend is Live, Hydrated, and Verified.

# Phase 4: Operation Outlook Probe (Headless Smoke Test)

- [x] **Step 1: Implementation (The Code)**
  - [x] **Dependencies**: `msal` added.
  - [x] **Config**: Azure settings added to `config.py`.
  - [x] **Logic**: `outlook.py` (Auth + Client).
  - [x] **API**: `/auth-url`, `/callback`, `/test-connection`.

- [ ] **Step 2: Deployment & Configuration (The Cloud)**
  - [x] **SQL**: Execute `outlook_probe_patch.sql`.
  - [x] **Secrets**: Bind `AZURE_CLIENT_ID`, `AZURE_CLIENT_SECRET`, `AZURE_TENANT_ID`.
  - [x] **Deploy**: `gcloud run deploy` (Code Update).

- [ ] **Step 3: Verification (The Probe)**
  - [ ] **Auth Flow**: Generate Token.
  - [ ] **Test Connection**: Confirm Draft Creation.
