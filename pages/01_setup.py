import streamlit as st
import pandas as pd
import os
import json
from dotenv import load_dotenv

# Import Logic
from agent_attribution import run_attribution_pipeline, smart_read_csv
from agent_bernays import BernaysAgent
from modules.db_client import DBClient
from modules.ui_components import render_message, render_dossier
from modules.drive_loader import download_data_from_drive
from modules.auto_discovery import auto_discover_drive_files

load_dotenv()
st.set_page_config(page_title="Scout", page_icon="🦅", layout="wide")

# --- 0. AUTO-DISCOVERY & DOWNLOAD ---
try:
    # 1. Search Drive for Real Data
    auto_discover_drive_files()
    # 2. Download if missing (using IDs found in step 1)
    download_data_from_drive()
except Exception as e:
    st.error(f"⚠️ Initialization Error: {e}")

# --- 1. SIDEBAR: FILE I/O ONLY ---
verify_success = False
with st.sidebar:
    st.header("Data Input")
    
    # CACHE PATH
    CACHE_PATH = "data/cached_target_list.csv"
    
    # 1. File Uploader
    uploaded_file = st.file_uploader("📂 Upload Target List / Broker Roster", type=['csv'])
    
    # 2. Persistence Logic
    if uploaded_file is not None:
        # Save to cache
        with open(CACHE_PATH, "wb") as f:
            f.write(uploaded_file.getbuffer())
        st.toast("File Cached for Session Persistence.")
    elif os.path.exists(CACHE_PATH):
        # Load from cache if no new upload
        st.info("📂 Using Cached Target List")
        uploaded_file = open(CACHE_PATH, "rb") # Read as binary stream logic for pandas
        
        if st.button("❌ Clear Cache"):
            os.remove(CACHE_PATH)
            st.experimental_rerun()
            
    st.divider()
    
    # 3. Export
    if st.button("📤 Export Results"):
        st.switch_page("pages/pilot_review.py")

if uploaded_file:
    # Just a quick check (lightweight)
    df_preview = smart_read_csv(uploaded_file)
    if df_preview is not None:
         render_message(f"Verified: {uploaded_file.name}", "success")
         verify_success = True
    else:
         render_message("File Error", "error")

st.sidebar.markdown("---")
# Export logic would act on Supabase data
db = DBClient()
all_leads = db.fetch_pilot_leads()
if all_leads:
    df_export = pd.DataFrame(all_leads)
    csv = df_export.to_csv(index=False).encode('utf-8')
    st.sidebar.download_button("📥 Export Results", data=csv, file_name="scout_results.csv", mime="text/csv")


# --- 2. TOP BAR: CONTROLS ---
col_search, col_filter, col_run = st.columns([3, 2, 1])

with col_search:
    search_query = st.text_input("Search targets...", placeholder="Search employer or broker")

with col_filter:
    status_filter = st.multiselect("Status", ["Platinum", "Verified", "Pending"], default=["Platinum","Verified"])
    auto_emails = st.checkbox("Auto-generate emails", value=True)

with col_run:
    run_clicked = st.button("🚀 RUN SCOUT", type="primary", use_container_width=True)

# --- 3. PIPELINE EXECUTION ---
if run_clicked:
    # Progress Container
    progress_area = st.empty()
    
    with st.spinner("Running Scout Pipeline..."):
        # STEP 1: ATTRIBUTION (Script A)
        dol_path = "data/master_5500.zip"
        sched_a_path = "data/master_sched_a.zip"
        sched_c_path = "data/master_sched_c.zip"
        static_pilot_path = "data/pilot_static_data.csv"
        
        success = False
        msg = "Init"

        # --- STATIC PILOT OVERRIDE (STRICT) ---
        if os.path.exists(static_pilot_path):
             progress_area.info("🔹 PILOT MODE: Loading Pre-Processed Static Data...")
             log_container = st.empty()
             log_event = lambda m: log_container.markdown(f"- {m}")
             log_event(f"Static Pilot Mode: attempting to load {static_pilot_path}")
             
             try:

                 df_static = pd.read_csv(static_pilot_path)
                 log_event(f"Static Pilot Mode: success, {len(df_static)} rows loaded.")
                 
                 # --- RAM PIVOT ---
                 # BYPASS DB. Store directly in Session State.
                 
                 # 1. Convert to Dicts
                 leads = df_static.to_dict('records')
                 
                 # 2. Add ID/Fields if missing (mimic DB schema)
                 for i, l in enumerate(leads):
                     if 'id' not in l: l['id'] = i + 1
                     if 'draft_email_text' not in l: l['draft_email_text'] = None
                     if 'psych_profile_json' not in l: l['psych_profile_json'] = None
                     if 'andrew_feedback_score' not in l: l['andrew_feedback_score'] = None
                     
                 # 3. Store in RAM
                 st.session_state['pilot_data'] = leads
                 st.session_state['data_source'] = 'RAM'
                 
                 log_event(f"✅ Success: Loaded {len(leads)} rows into System Memory.")
                 success = True
                 msg = f"Static Pilot Load Complete ({len(leads)} leads in RAM)"

             except Exception as e:
                 progress_area.error(f"❌ Static Load Failed: {e}")
                 success = False
        else:
             st.error("❌ STATIC PILOT ERROR: pilot_static_data.csv not found in deployment.")
             st.stop()
        
        # --- NO FALLBACK ---
        # User requested Strict Pilot Mode. We do NOT look for Zips.
        if not success:
             st.error("Pilot Failed to Load Data.")
             st.stop()
            
             st.stop()
    
        if not success:
            render_message(f"Pipeline Failed: {msg}", "error")
        else:
            render_message(f"Attribution Complete: {msg}", "success")
            st.session_state.leads_generated = True
            
            # STEP 2: BERNAYS (Script B)
            if auto_emails:
                progress_area.text("🧠 Starting AI Generation (RAM Mode)...")
                agent_b = BernaysAgent()
                # Pass session state list for IN-PLACE modification
                count_b = agent_b.run_bernays_cycle(memory_leads=st.session_state['pilot_data']) 
                render_message(f"AI Complete: Generated {count_b} drafts in RAM.", "success")
            
            st.success("Scout Run Finished. Leads Available in Dashboard.")
            # st.rerun() REMOVED to prevent loop

# --- 4. MAIN WORKSPACE ---
col_list, col_detail = st.columns([1, 2])

# Fetch Data
leads = db.fetch_pilot_leads()
df_leads = pd.DataFrame(leads)

selected_lead = None

if not df_leads.empty:
    # FILTERS
    if search_query:
        df_leads = df_leads[
            df_leads['employer_name'].str.contains(search_query, case=False, na=False) |
            df_leads['broker_firm'].str.contains(search_query, case=False, na=False)
        ]
    
    # Selection
    with col_list:
        st.markdown("### Target List")
        # Use selection API if available, else standard dataframe
        event = st.dataframe(
            df_leads[['employer_name', 'broker_firm', 'verification_status']], 
            use_container_width=True,
            on_select="rerun",
            selection_mode="single-row",
            hide_index=True
        )
        
        selected_rows = event.selection.rows
        selected_lead = df_leads.iloc[selected_rows[0]] if selected_rows else None

    # Detail View
    with col_detail:
        if selected_lead is not None:
             render_dossier(selected_lead)
        else:
            st.info("Select a target from the list to begin analysis.")

# --- 5. SYSTEM STATUS (Collapsed) ---
with st.expander("System Status"):
    st.text(f"DB Connection: {'Supabase' if 'supabase' in str(db) else 'SQLite (Local)'}")
    st.text(f"Total Leads: {len(leads)}")
    if st.button("⚠️ CLEAR ALL LEADS"):
        db.clear_leads()
        st.rerun()
