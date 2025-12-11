import streamlit as st
import pandas as pd
from utils.email_templates import generate_briefing_html

def run_command_center():
    """
    Module 4: THE COMMAND CENTER (UI)
    Premium SaaS Design System.
    """
    
    # --- SIDEBAR (Mission Control) ---
    with st.sidebar:
        st.header("🚁 Mission Control")
        
        # Navigation
        nav_mode = st.radio(
            "Navigation", 
            ["🎯 Hunt", "💼 Review", "⚙️ Settings"],
            label_visibility="collapsed"
        )
        
        st.divider()
        
        # Scout Co-Pilot (Sticky Bottom)
        st.markdown("### 🤖 Scout Co-Pilot")
        messages = st.container(height=300)
        messages.chat_message("assistant").write("Ready. Waiting for orders.")
        if prompt := st.chat_input("Ask Scout..."):
            messages.chat_message("user").write(prompt)
            messages.chat_message("assistant").write(f"Processing '{prompt}'...")

    # --- MAIN CONTENT ---
    
    if nav_mode == "💼 Review":
        st.markdown("## The Daily Stack")
        
        # Mock Data
        mock_leads = [
            {
                "id": 1,
                "name": "Sarah Connor",
                "title": "VP of Operations",
                "company": "Cyberdyne Systems",
                "profile": "Crusader",
                "source": "News Radar",
                "draft": "Hi Sarah, saw the news about the merger...",
                "snippet": "Leading the merger integration team...",
                "initials": "SC"
            },
            {
                "id": 2,
                "name": "Miles Dyson",
                "title": "Director of Engineering",
                "company": "Cyberdyne Systems",
                "profile": "Analyst",
                "source": "Sniper Mode",
                "draft": "Miles, your paper on neural nets was fascinating...",
                "snippet": "Published new research on...",
                "initials": "MD"
            }
        ]

        # Session State for Stack
        if 'stack_index' not in st.session_state:
            st.session_state.stack_index = 0
        
        current_index = st.session_state.stack_index

        if current_index < len(mock_leads):
            lead = mock_leads[current_index]
            
            # Progress
            st.progress((current_index + 1) / len(mock_leads), text=f"Reviewing {current_index + 1} of {len(mock_leads)}")

            # --- CUSTOM HTML CARD ---
            card_html = f"""
            <div class="lead-card">
                <div class="card-header">
                    <div class="avatar">{lead['initials']}</div>
                    <div class="name-block">
                        <h3>{lead['name']}</h3>
                        <p>{lead['title']} @ {lead['company']}</p>
                        <div class="badges">
                            <span class="badge badge-green">Verified</span>
                            <span class="badge badge-purple">{lead['profile']}</span>
                            <span class="badge badge-blue">{lead['source']}</span>
                        </div>
                    </div>
                </div>
                <div class="intel-box">
                    <div class="intel-key">INTEL SNIPPET</div>
                    <div class="intel-value">"{lead['snippet']}"</div>
                </div>
            </div>
            """
            st.markdown(card_html, unsafe_allow_html=True)

            # --- EDITABLE DRAFT ---
            st.markdown("#### ✉️ Draft Preview")
            draft_text = st.text_area("Edit Draft", value=lead['draft'], height=150, label_visibility="collapsed", key=f"draft_{lead['id']}")

            # --- ACTION BAR ---
            c1, c2 = st.columns([1, 2])
            with c1:
                if st.button("REJECT", key=f"reject_{lead['id']}", type="secondary"):
                    st.session_state.stack_index += 1
                    st.rerun()
            with c2:
                if st.button("APPROVE & PUSH", key=f"approve_{lead['id']}", type="primary"):
                    st.toast("🚀 Pushed to Outlook!")
                    st.session_state.stack_index += 1
                    st.rerun()
        
        else:
            st.success("🎉 Stack Cleared!")
            if st.button("Reset Demo"):
                st.session_state.stack_index = 0
                st.rerun()

    elif nav_mode == "🎯 Hunt":
        from modules.hunter import run_hunter
        # Determine sub-mode (could be a radio in the main view or passed from sidebar)
        # For now, let's default to a selector in the main view
        hunt_type = st.radio("Strategy", ["News Radar", "Sniper Mode"], horizontal=True)
        run_hunter(hunt_type)
        
    elif nav_mode == "⚙️ Settings":
        st.text("Settings Placeholder")

    return nav_mode
