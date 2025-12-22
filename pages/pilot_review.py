import streamlit as st
import pandas as pd
from modules.db_client import DBClient
import json
import modules.auth as auth

# --- AUTHENTICATION ---
if not auth.check_password():
    st.stop()

st.set_page_config(page_title="Pilot Review (Andrew Mode)", page_icon="🕵️", layout="wide")

st.title("🦅 Pilot Review: Golden Batch (CA)")
st.markdown("Review the logic. Does the Psychographic Engine sound like a human?")

# RAM MODE
if 'pilot_data' in st.session_state:
    leads = st.session_state['pilot_data']
else:
    leads = []

# db = DBClient() # Disabled for RAM Mode
# leads = db.fetch_pilot_leads()

if not leads:
    st.info("No leads generated yet. Run the Prospector in Protocol D.")
else:
    st.success(f"Data Loaded: {len(leads)} Records.")
    
    # 1. FILTER: Human vs Firm
    humans = [l for l in leads if l.get('broker_human_name') not in [None, 'Unknown', '']]
    backlog = [l for l in leads if l not in humans]
    
    # 2. METRICS
    m1, m2, m3 = st.columns(3)
    m1.metric("Qualified Human Leads", len(humans), help="Ready for Email Gen")
    m2.metric("Firm-Only Backlog", len(backlog), help="Requires Research")
    m3.metric("Total Ingested", len(leads))
    
    # 3. EXPORT LOGIC (Qualified Only)
    df_export = pd.DataFrame(humans)
    if not df_export.empty:
        # User Compliance: Rename 'draft_email_text' -> 'email_draft'
        if 'draft_email_text' in df_export.columns:
            df_export.rename(columns={'draft_email_text': 'email_draft'}, inplace=True)
            
        csv_data = df_export.to_csv(index=False).encode('utf-8')
        st.download_button(
            label=f"📥 Download Qualified Leads ({len(humans)})",
            data=csv_data,
            file_name="scout_qualified_leads.csv",
            mime="text/csv",
            type="primary"
        )
    else:
        st.button("📥 Download Qualified Leads (0)", disabled=True, help="No human-verified leads available.")
    
    # 4. VIEW TABS
    tab_humans, tab_backlog = st.tabs(["🚀 Qualified Humans", "🗄️ Firm Backlog"])
    
    with tab_humans:
        if not humans:
            st.info("No Human-Qualified Leads found. Data refinement required.")
        else:
            for lead in humans:
                with st.expander(f"{lead['employer_name']} | 👤 {lead['broker_human_name']}", expanded=False):
                    c1, c2 = st.columns([2, 1])
                    with c1:
                        st.subheader("Draft Email")
                        st.code(lead.get('draft_email_text', 'No Draft'), language="text")
                    with c2:
                         if st.button("👍 Good", key=f"good_{lead['id']}"):
                             lead['andrew_feedback_score'] = "GOOD"
                             st.rerun()
                         if st.button("👎 Bad", key=f"bad_{lead['id']}"):
                             lead['andrew_feedback_score'] = "BAD"
                             st.rerun()

    with tab_backlog:
        st.dataframe(
            pd.DataFrame(backlog)[['employer_name', 'broker_firm', 'state', 'lives_count']],
            use_container_width=True,
            hide_index=True
        )
