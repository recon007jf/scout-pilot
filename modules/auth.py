
import streamlit as st
import os
import time
import extra_streamlit_components as stx
from dotenv import load_dotenv

# Load Environment
load_dotenv()

def check_password():
    """
    Returns `True` if the user had the correct password.
    """
    
    # 1. SETUP COOKIE MANAGER
    # Note: We must initialize usage a specific key to avoid re-renders
    cookie_manager = stx.CookieManager(key="auth_cookie_manager")
    
    # 2. CHECK COOKIE FIRST (Persistence)
    auth_token = cookie_manager.get(cookie="scout_auth_token")
    
    # Hardcoded/Env Password
    CORRECT_PASSWORD = os.getenv("APP_PASSWORD", "scout2025")
    
    if auth_token == "authenticated":
        return True
        
    # 3. SESSION STATE CHECK (Fallback)
    if st.session_state.get("password_correct", False):
        return True

    # 4. SHOW LOGIN FORM
    st.markdown("""
    <style>
    .stApp {
        background-color: #111827;
        color: white;
    }
    input {
        background-color: #374151 !important;
        color: white !important;
        border: 1px solid #4B5563 !important;
    }
    </style>
    """, unsafe_allow_html=True)
    
    col1, col2, col3 = st.columns([1, 2, 1])
    with col2:
        st.markdown("<br><br><br>", unsafe_allow_html=True)
        st.markdown("## 🦅 Scout Intelligence")
        st.caption("Authorized Personnel Only")
        
        # Form ensures "Enter" key works
        with st.form("login_form"):
            password = st.text_input("Access Code", type="password")
            submitted = st.form_submit_button("Authenticate")
            
            if submitted:
                if password == CORRECT_PASSWORD:
                    st.session_state["password_correct"] = True
                    # SET COOKIE (Expires in 7 days)
                    cookie_manager.set("scout_auth_token", "authenticated", expires_at=None, key="set_auth_cookie")
                    st.success("Access Granted.")
                    time.sleep(0.5)
                    st.rerun()
                else:
                    st.error("⛔️ Access Denied.")
                    
    return False

def logout():
    cookie_manager = stx.CookieManager(key="logout_cookie")
    cookie_manager.delete("scout_auth_token")
    st.session_state["password_correct"] = False
    st.rerun()
