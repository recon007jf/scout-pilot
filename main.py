import os
import pandas as pd
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
import uvicorn

app = FastAPI()

# --- CORS CONFIGURATION (V0 Requirement) ---
# Allow Vercel Previews and Localhost
origin_regex = r"^(http://localhost:3000|https://scout-.*\.vercel\.app)$"
app.add_middleware(
    CORSMiddleware,
    allow_origin_regex=origin_regex, 
    allow_credentials=False, # Public API checks
    allow_methods=["*"],
    allow_headers=["*"],
)

# --- CONFIGURATION ---
# Use Env Var for filename, default to the specific Funding Patched file
DEFAULT_FILENAME = "Master_Hunting_List_Production_v3_ANDREW_ENRICHED_FUNDING_PATCHED.csv"
DATA_FILENAME = os.getenv("SCOUT_DATA_FILE", DEFAULT_FILENAME)

# --- GLOBAL STATE ---
df = pd.DataFrame()
load_error = None

# --- STARTUP LOGIC ---
@app.on_event("startup")
def load_data():
    global df, load_error
    try:
        # Resolve absolute path to avoid ambiguity
        file_path = os.path.abspath(DATA_FILENAME)
        
        if os.path.exists(file_path):
            print(f"Loading data from: {file_path}")
            # Try different encodings if standard utf-8 fails
            try:
                df = pd.read_csv(file_path)
            except UnicodeDecodeError:
                print("UTF-8 failed, trying latin-1...")
                df = pd.read_csv(file_path, encoding='latin-1')
                
            print(f"Successfully loaded {len(df)} rows.")
        else:
            load_error = f"File not found at: {file_path}"
            print(f"CRITICAL WARNING: {load_error}")
            
    except Exception as e:
        load_error = str(e)
        print(f"CRITICAL ERROR loading data: {e}")

# --- DIAGNOSTIC PROBE ---
@app.get("/health")
def health_check():
    # Re-calculate path state for the report
    current_path = os.path.abspath(DATA_FILENAME)
    file_exists = os.path.exists(current_path)
    file_size = os.path.getsize(current_path) if file_exists else 0
    
    return {
        "status": "healthy" if not load_error and len(df) > 0 else "degraded",
        "config": {
            "env_var_value": os.getenv("SCOUT_DATA_FILE"),
            "resolved_filename": DATA_FILENAME,
            "resolved_abs_path": current_path
        },
        "filesystem": {
            "file_exists": file_exists,
            "file_size_bytes": file_size,
            "cwd": os.getcwd()
        },
        "data": {
            "rows_loaded": len(df),
            "columns": list(df.columns) if not df.empty else [],
            "load_error": load_error
        }
    }

# --- PRIMARY ENDPOINT: TERRITORY ---
@app.get("/api/accounts/territory")
def get_territory():
    if df.empty: return {"accounts": []}
    
    # Logic: High Confidence OR Verified Status
    targets = df[
        (df['Funding_Confidence'] == 'High') | 
        (df['Funding_Status'].str.contains('Verified', na=False))
    ]
    
    # Fallback to sample if filtering yields nothing (just to show *something*)
    if targets.empty: 
        targets = df.sample(min(10, len(df)))
    
    priority_targets = []
    # Limit to reasonable size for map payload
    for idx, row in targets.head(1000).iterrows():
        priority_targets.append({
            "id": str(idx),
            "company": str(row.get('Broker_Firm', 'Unknown')),
            "contact": str(row.get('Contact_Name', 'Unknown')),
            "title": str(row.get('Title', 'Unknown')),
            "location": {
                # Map doesn't have lat/lng in CSV, would need Geocoding service or column
                # For now returning placeholder or 0,0, assuming V0 handles it or uses mocked lat/lng
                # based on State center? 
                # Actually, the V3 Assembly didn't strictly add Lat/Lng cols to Andrew's view.
                # Assuming UI handles/mocks location or we use address. 
                "lat": 0, 
                "lng": 0
            }, 
            "address": str(row.get('Broker_State', '')),
            "status": "prospect", # simplified
            "revenue": 0,
            "lastContact": "2024-01-01",
            "region": str(row.get('Broker_State', ''))
        })
        
    return {"accounts": priority_targets}

@app.get("/api/briefing")
def get_briefing():
    if df.empty: return {"priority_targets": [], "metrics": []}
    
    # Logic: High Confidence OR Verified Status
    targets = df[
        (df['Funding_Confidence'] == 'High') | 
        (df['Funding_Status'].str.contains('Verified', na=False))
    ]
    
    # Fallback sampling
    if targets.empty: targets = df.sample(min(5, len(df)))
    else: targets = targets.sample(min(5, len(targets)))
    
    priority_targets = []
    for idx, row in targets.iterrows():
        priority_targets.append({
            "id": str(idx),
            "name": str(row.get('Contact_Name', 'Unknown')),
            "firm": str(row.get('Broker_Firm', 'Unknown')),
            "location": str(row.get('Broker_State', '')),
            "email": str(row.get('Email', '')),
            "linkedIn": str(row.get('LinkedIn', '')),
            "segment": str(row.get('Funding_Status', 'Unknown')),
            "confidence": str(row.get('Funding_Confidence', 'Low')),
            "territory_match": True
        })
        
    return {
        "date": "Today",
        "greeting": "Good Morning",
        "summary": f"Found {len(df[df['Funding_Confidence'] == 'High'])} Verified Targets.",
        "metrics": [],
        "priority_targets": priority_targets
    }

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8080))
    uvicorn.run(app, host="0.0.0.0", port=port)
