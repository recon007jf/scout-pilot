import pandas as pd
import os

BASE_PATH = "/Users/josephlf/.gemini/antigravity/scratch/backend/Scout_Data_Artifacts"
LEADS_FILE = os.path.join(BASE_PATH, 'Western_Leads_2023_Platinum.parquet')
CONTACTS_FILE = os.path.join(BASE_PATH, 'pilot_inputs/AndrewWestRegion_2025.csv')

def debug_values():
    print(">>> DEBUG BROKER VALUES")
    
    # 1. Inspect Leads
    print(f"Reading {LEADS_FILE}...")
    df_leads = pd.read_parquet(LEADS_FILE)
    print(f"Leads Columns: {list(df_leads.columns)}")
    print("\nTop 20 Raw BROKERS values:")
    print(df_leads['BROKERS'].value_counts().head(20))
    
    # Check if they look like lists
    sample = df_leads['BROKERS'].dropna().iloc[0]
    print(f"\nSample Broker Val (type={type(sample)}): '{sample}'")

    # 2. Inspect Contacts
    print(f"\nReading {CONTACTS_FILE}...")
    df_contacts = pd.read_csv(CONTACTS_FILE)
    print("\nTop 20 Contact Company Names:")
    print(df_contacts['Company Name'].value_counts().head(20))

if __name__ == "__main__":
    debug_values()
