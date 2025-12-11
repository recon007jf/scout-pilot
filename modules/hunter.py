import streamlit as st
import pandas as pd
from utils.serper import search_google, search_news

def run_hunter(mode="News Radar"):
    """
    Module 1: THE HUNTER (Discovery)
    Executes search strategies based on the selected mode.
    """
    st.markdown(f"## 🎯 The Hunter: {mode}")

    results = []

    if mode == "News Radar":
        st.info("📡 Scanning for Trigger Events (Mergers, Rate Hikes) in WA, OR, CA...")
        
        # In a real app, we would loop through multiple triggers
        triggers = ["Merger", "Acquisition", "Rate Hike"]
        
        if st.button("Start Scan"):
            with st.spinner("Scanning the horizon..."):
                for trigger in triggers:
                    news_data = search_news(f"{trigger} healthcare California")
                    if "news" in news_data:
                        for item in news_data["news"]:
                            results.append({
                                "Company": "Unknown (Extract from text)", # Placeholder for extraction logic
                                "Title": item['title'],
                                "Source": item['source'],
                                "Snippet": item['snippet'],
                                "Link": item['link'],
                                "Type": "News"
                            })
            
            if results:
                st.success(f"Found {len(results)} signals.")
                st.dataframe(pd.DataFrame(results), use_container_width=True)
            else:
                st.warning("No signals found.")

    elif mode == "Sniper Mode":
        query = st.text_input("Target Query", placeholder="e.g. VP of HR in Seattle")
        if st.button("Fire Shot"):
            with st.spinner("Acquiring targets..."):
                search_data = search_google(f"site:linkedin.com/in/ {query}")
                if "organic" in search_data:
                    for item in search_data["organic"]:
                        results.append({
                            "Title": item['title'],
                            "Snippet": item['snippet'],
                            "Link": item['link'],
                            "Type": "Organic"
                        })
            
            if results:
                st.success(f"Found {len(results)} targets.")
                st.dataframe(pd.DataFrame(results), use_container_width=True)

    return results
