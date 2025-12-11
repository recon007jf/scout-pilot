import streamlit as st
import json

def render_message(text, tier="info"):
    """
    Renders messages according to the 3-Tier Color System.
    """
    if tier == 'info':
        st.info(text, icon="ℹ️")
    elif tier == 'success':
        st.success(text, icon="✅")
    elif tier == 'warning':
        st.warning(text, icon="⚠️")
    elif tier == 'error':
        st.error(text, icon="❌")
    elif tier == 'orange':
        # Custom CSS for Orange Alert
        st.markdown(
            f"""
            <div style="
                padding: 1rem;
                background-color: #4c3218; 
                color: #ffbd45;
                border-left: 5px solid #ffbd45;
                border-radius: 4px;
                margin-bottom: 1rem;
            ">
                <b>🟠 Serious Alert:</b> {text}
            </div>
            """,
            unsafe_allow_html=True
        )
    else:
        st.write(text)

def render_dossier(lead):
    """
    Renders the "Rich Dossier" View for a selected lead.
    Layout: Header (Avatar/Name), Hard Data, Psych, Draft.
    """
    if not lead:
        st.info("Select a lead to view details.")
        return

    # --- 1. HEADER (Visual Identity) ---
    c_avatar, c_info = st.columns([1, 4])
    
    with c_avatar:
        # Avatar Logic: Photo or Monogram
        if lead.get('photo_url'):
            st.image(lead['photo_url'], width=100)
        else:
            # Monogram fallback
            name = lead.get('broker_human_name', 'Unknown')
            initials = "".join([n[0] for n in name.split()[:2]]) if name != 'Unknown' else "??"
            st.markdown(
                f"""
                <div style="
                    width: 100px; height: 100px; 
                    background-color: #4A86E8; 
                    color: white; 
                    font-size: 40px; 
                    font-weight: bold; 
                    border-radius: 50%; 
                    display: flex; 
                    align-items: center; 
                    justify-content: center;
                ">
                    {initials}
                </div>
                """,
                unsafe_allow_html=True
            )

    with c_info:
        st.markdown(f"## {lead.get('broker_human_name', 'Unknown Broker')}")
        st.caption(f"**{lead.get('broker_firm', 'Unknown Firm')}**")
        
        # Badges
        badges = []
        if lead.get('city'): badges.append(f"📍 {lead.get('city')}")
        if lead.get('verification_status') == 'VERIFIED': badges.append("🏛 Verified Self-Funded")
        
        st.markdown(" ".join([f"`{b}`" for b in badges]))

    st.markdown("---")

    # --- 2. HARD DATA (Context) ---
    with st.container():
        st.markdown(f"### 🏢 {lead.get('employer_name', 'Unknown Employer')}")
        
        c1, c2, c3 = st.columns(3)
        c1.metric("Lives", f"{lead.get('lives_count', 0):,}")
        c2.metric("Assets", f"${lead.get('assets_amount', 0):,.0f}")
        c3.metric("State", lead.get('state', 'Unknown'))
        
        # Contact Links
        email = lead.get('email', '')
        linkedin = lead.get('linkedin_url', '')
        
        links = []
        if email: links.append(f"[📧 Email]({email})") # Simple mailto or just display
        if linkedin: links.append(f"[🔗 LinkedIn]({linkedin})")
        
        if links:
            st.markdown(" | ".join(links))

    # --- 3. PSYCH PROFILE ---
    with st.expander("🧠 Psychographic Analysis", expanded=True):
        try:
            profile = json.loads(lead.get('psych_profile_json', '{}'))
            st.markdown(f"**Hook Strategy:** *{profile.get('Hook', 'Standard Approach')}*")
            st.markdown("**Key Insights:**")
            # Assuming 'pain_points' or similar exists, or just dump meaningful keys
            for k, v in profile.items():
                if k not in ['Hook', 'psych_profile', 'archetype']:
                    st.markdown(f"- **{k}:** {v}")
        except:
            st.caption("Detailed psychometrics unavailable.")

    # --- 4. ACTION (Draft) ---
    st.subheader("✉️ Proposed Outreach")
    draft_text = lead.get('draft_email_text', 'No draft generated.')
    st.text_area("Editor", value=draft_text, height=300, key=f"editor_{lead.get('id', 'new')}")
    
    col_copy, col_regen = st.columns(2)
    with col_copy:
        st.button("📋 Copy to Clipboard", key=f"copy_{lead.get('id', 'new')}")
    with col_regen:
        st.button("🔄 Regenerate", key=f"regen_{lead.get('id', 'new')}")
