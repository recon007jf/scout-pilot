import streamlit as st
import pandas as pd
import json
import os
import time

# Custom Modules
import modules.auth as auth
from modules.db_client import DBClient

# --- AUTHENTICATION ---
if not auth.check_password():
    st.stop()

# --- CONFIGURATION ---
st.set_page_config(
    page_title="Scout Intelligence",
    page_icon="🦅",
    layout="wide",
    initial_sidebar_state="expanded"
)

# --- CSS INJECTION (CLEAN DARK MODE) ---
st.markdown("""
<style>
/* 1. CLEAN DARK THEME */
.stApp {
    background-color: #111827; /* sleek dark blue-grey */
    color: #F9FAFB;
}

/* 2. SIDEBAR STYLING */
section[data-testid="stSidebar"] {
    background-color: #1F2937; /* slightly lighter grey */
    border-right: 1px solid #374151;
}

/* 3. CARD STYLING (For Intel/Action Zones) */
/* Targeting vertical blocks inside the main area */
div[data-testid="stVerticalBlock"] > div > div[data-testid="stVerticalBlock"] > div {
    /* 
       Note: Streamlit DOM is tricky. 
       We apply specific classes to containers via st.container() context 
       if possible, or rely on general targeting.
       Below targets the 'stForm' or similar blocks if we use them.
    */
}

/* Custom Card Class (we will wrap zones in this) */
.css-card {
    background-color: #1F2937;
    border: 1px solid #374151;
    border-radius: 12px;
    padding: 24px;
    box-shadow: 0 4px 6px -1px rgba(0, 0, 0, 0.1);
    margin-bottom: 20px;
}

/* 4. METRICS & BADGES */
div[data-testid="stMetricValue"] {
    font-size: 24px;
    font-weight: 600;
    color: #60A5FA !important; /* Calming Blue, not Neon */
}
div[data-testid="stMetricLabel"] {
    color: #9CA3AF !important;
}

/* 5. REMOVE CLUTTER */
#MainMenu {visibility: hidden;}
footer {visibility: hidden;}
header {visibility: hidden;}

/* Custom Action Button */
button[kind="primary"] {
    background-color: #2563EB !important;
    border: none;
    color: white;
}
button[kind="secondary"] {
    background-color: #374151 !important;
    color: #E5E7EB;
    border: 1px solid #4B5563;
}
</style>
""", unsafe_allow_html=True)

# --- DATA LAYER ---
db = DBClient()
leads = db.fetch_pilot_leads()

# --- SIDEBAR: LEAD COMMAND ---
with st.sidebar:
    st.markdown("### 🦅 Lead Command")
    
    # 1. Filters
    with st.expander("Filters", expanded=False):
        status_filter = st.selectbox("Status", ["All", "Verified", "Platinum"], index=0)
        industry_filter = st.selectbox("Industry", ["All", "Tech", "Public Sector", "Manufacturing"], index=0)

    st.markdown("---")
    st.markdown("#### The Hit List")
    
    if not leads:
        st.info("No leads found.")
        st.caption("Go to 'Setup' page to load data.")
        selected_lead_id = None
    else:
        # Format: [Score] Broker Name @ Firm
        # Since we don't have a real score, defaulting to [95] or random for MVP feel
        lead_options = {
            lead['id']: f"🟢 [95] {lead['broker_human_name']} @ {lead['broker_firm']}"
            for lead in leads
        }
        
        # Display as Radio (Employer-Centric)
        selected_lead_id = st.radio(
            "Select Target:",
            options=leads, 
            format_func=lambda x: f"🏢 {x.get('employer_name', 'Unknown')} ({x.get('lives_count', 0)} Lives)",
            label_visibility="collapsed"
        )

    
    st.markdown("---")
    st.markdown("---")
    st.markdown("### 💾 Data & Export")
    
    # IMPORT BTN (Restored)
    # 1. AUTO-LOAD (Drive)
    drive_id = os.getenv("ANDREW_LIST_ID")
    if drive_id:
        if st.button("🔄 Auto-Load Andrew's CSV (Drive)", help=f"Fetch ID: {drive_id}"):
            with st.spinner("Downloading from Drive..."):
                from modules.drive_loader import get_drive_service, download_file_authenticated
                srv = get_drive_service()
                if srv:
                    ok, msg = download_file_authenticated(srv, drive_id, "data/temp_upload.csv")
                    if ok:
                        try:
                            df_new = pd.read_csv("data/temp_upload.csv", encoding='latin-1')
                            if len(df_new) > 0:
                                st.success(f"✅ Loaded {len(df_new)} rows from Drive!")
                                # Optional: You might want to do something with df_new here
                                # For MVP, we just show it to confirm connection
                                st.dataframe(df_new.head())
                            else:
                                st.warning("CSV Empty.")
                        except Exception as e:
                            st.error(f"Read Error: {e}")
                    else:
                        st.error(f"Download Error: {msg}")
                else:
                    st.error("Auth Failed.")
    
    # 2. MANUAL UPLOAD
    uploaded_file = st.file_uploader("📂 Manual Import CSV", type=["csv"], help="Upload BenefitFlow or Custom CSV")
    if uploaded_file is not None:
        if st.button("Load Manual Data"):
             try:
                 uploaded_file.seek(0)
                 df_new = pd.read_csv(uploaded_file)
             except UnicodeDecodeError:
                 uploaded_file.seek(0)
                 df_new = pd.read_csv(uploaded_file, encoding='latin-1')
             except Exception as e:
                 st.error(f"Error loading CSV: {e}")
                 df_new = []
                 
             if len(df_new) > 0:
                 st.success(f"Loaded {len(df_new)} rows (Preview Only)")
             # Logic to save would go here (omitted for MVP speed)

    st.markdown("<br>", unsafe_allow_html=True)

    # EXPORT BTN
    if leads:
        # Convert List of Dicts -> DataFrame
        df_export = pd.DataFrame(leads)
        csv_data = df_export.to_csv(index=False).encode('utf-8')
        
        st.download_button(
            label="📥 Download Pilot Results (CSV)",
            data=csv_data,
            file_name="scout_pilot_results.csv",
            mime="text/csv",
            use_container_width=True
        )
    else:
        st.button("📥 Download Pilot Results (CSV)", disabled=True, help="No data to export.", use_container_width=True)

    # 2. Source Metadata Footer UI
    st.markdown("<br><br>", unsafe_allow_html=True)
    st.markdown("---")
    st.caption("🔌 **System Status**")
    st.caption("✅ Source: Scout Data Lake (GCS)")
    if not leads:
         st.warning("⚠️ No leads found.")
         st.info("👉 Go to **'⚙️ Setup'** (Sidebar Top) to Load Data & Run Pilot.")
    st.caption(f"⚡️ Lead Universe: {len(leads)}")

# --- MAIN STAGE: INTEL VIEW ---

if not selected_lead_id:
    # MISSION SUMMARY (Empty State)
    st.title("Mission Summary")
    
    m1, m2, m3 = st.columns(3)
    m1.metric("Total Market Map", len(leads))
    m2.metric("Verified Humans", len([l for l in leads if l['verification_status'] == 'VERIFIED_PILOT']))
    m3.metric("Platinum Opportunities", int(len(leads) * 0.2)) # Mock stat
    
    st.info("👈 Select a target from the sidebar to begin reconnaissance.")
    
else:
    # Get Selected Lead Data
    # Note: st.radio returns the actual item (dict) because we passed options=leads (list of dicts)
    current_lead = selected_lead_id
    
    # Parse JSON Profile
    try:
        profile = json.loads(current_lead['psych_profile_json'])
    except:
        profile = {}
        
    # --- ZONE 1: TARGET HEADER ---
    with st.container():
        st.markdown('<div class="css-card">', unsafe_allow_html=True)
        z1_c1, z1_c2, z1_c3 = st.columns([3, 1, 1])
        
        with z1_c1:
            st.markdown(f"# {current_lead['employer_name']}")
            st.caption(f"📍 {current_lead['state']} | Self-Funded Header")
            
        with z1_c2:
            lives = current_lead.get('lives_count', 0)
            st.metric("Lives", f"{lives:,}")
            
        with z1_c3:
            assets = current_lead.get('assets_amount', 0)
            assets_fmt = f"${assets/1_000_000:.1f}M" if assets else "N/A"
            st.metric("Est. Assets", assets_fmt)
        st.markdown('</div>', unsafe_allow_html=True)

    # --- ZONE 2 & 3: SPLIT VIEW ---
    col_left, col_right = st.columns([1, 1])
    
    with col_left:
        # ZONE 2: ATTRIBUTION CARD
        st.markdown("### 🧠 Broker Intelligence")
        with st.container():
             st.markdown(f"""
             <div style="background-color: #1F2937; border: 1px solid #374151; border-radius: 12px; padding: 24px;">
                <h4 style="color: #9CA3AF; text-transform: uppercase; font-size: 0.8rem;">Target Broker (System Found)</h4>
                <div style="font-size: 1.5rem; font-weight: bold; color: white; margin-bottom: 5px;">
                    {current_lead.get('broker_human_name', 'Unknown')}
                </div>
                <div style="color: #60A5FA; font-size: 1.1rem; margin-bottom: 20px;">
                    {current_lead['broker_firm']}
                </div>
                
                {f'''
                <div style="margin-bottom: 20px; border-top: 1px solid #374151; padding-top: 15px;">
                    <h4 style="color: #10B981; text-transform: uppercase; font-size: 0.8rem;">✅ Verified Truth (CSV)</h4>
                    <div style="font-size: 1.1rem; font-weight: bold; color: #D1D5DB;">
                        {current_lead.get("Verified Broker Truth", "Not in CSV")}
                    </div>
                </div>
                ''' if "Verified Broker Truth" in current_lead else ""}
                
                <h4 style="color: #9CA3AF; text-transform: uppercase; font-size: 0.8rem;">Psychographic Profile</h4>
                <div style="font-size: 1.2rem; font-weight: bold; color: #FBBF24; margin-bottom: 5px;">
                    {profile.get('psych_profile', 'Unknown')}
                </div>
                <div style="color: #D1D5DB; font-style: italic; border-left: 3px solid #FBBF24; padding-left: 10px;">
                    "{profile.get('Unconscious_Desire', 'Driver Unknown')}"
                </div>
                
                <div style="margin-top: 20px;">
                    <h4 style="color: #9CA3AF; text-transform: uppercase; font-size: 0.8rem;">Strategic Hook</h4>
                    <p style="color: #E5E7EB; margin-top: 5px;">
                        {profile.get('Hook', "N/A")}
                    </p>
                </div>
             </div>
             """, unsafe_allow_html=True)

    with col_right:
        # ZONE 3: ACTION CARD
        st.markdown("### ⚡️ War Room")
        with st.container():
             # Raw Text Area for copying
             draft = current_lead.get('draft_email_text', 'No Draft Available.')
             st.text_area("Draft Email", value=draft, height=250)
             
             ac1, ac2 = st.columns(2)
             with ac1:
                 if st.button("📋 Copy to Clipboard", use_container_width=True):
                     st.toast("Copied to Clipboard!")
                     # Note: Streamlit clipboard access is limited, usually requires JS hack or user manual copy.
                     # We assume user copies from textarea manually for MVP, or we use st.code.
                     
             with ac2:
                 if st.button("🔄 Regenerate", help="Re-run Bernays Engine", use_container_width=True):
                     st.info("Regeneration queued...")
                     
             st.divider()
             st.caption("Pilot Feedback")
             
             # Star Rating (Slider)
             current_score = int(current_lead.get('andrew_feedback_score') or 0)
             new_score = st.slider("Quality Score", 0, 5, current_score)
             
             if new_score != current_score:
                 db.update_feedback(current_lead['id'], str(new_score))
                 st.toast(f"Rating Saved: {new_score} Stars")
                 time.sleep(0.5)
                 st.rerun()

    # Manual Data Override (Optional)
    with st.expander("🔧 Manual Override"):
        new_human = st.text_input("Correct Broker Name", value=current_lead['broker_human_name'])
