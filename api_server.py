from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from app.services.territory_service import TerritoryService
import uvicorn
import os
import json
import logging

# --- Logging (Structured JSON for Cloud Run) ---
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("scout-api")

app = FastAPI(title="Scout Territory API", version="1.0")

# --- CORS (Regex for Vercel Previews) ---
# Matches: http://localhost:3000, https://scout-ui.vercel.app, and any https://scout-*.vercel.app
# This satisfies V0's requirement for dynamic Vercel preview URLs.
origin_regex = r"^(http://localhost:3000|https://scout-.*\.vercel\.app)$"

app.add_middleware(
    CORSMiddleware,
    allow_origin_regex=origin_regex, 
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

service = TerritoryService()

@app.on_event("startup")
async def startup_event():
    logger.info(json.dumps({"event": "startup", "message": "Scout API Starting", "origins": origins}))

@app.get("/health")
def health_check():
    """Cloud Run Health Check"""
    return {"status": "healthy", "service": "Scout API"}

@app.get("/api/accounts/territory")
def get_territory():
    """
    Returns simplified account list with geocoded locations.
    """
    try:
        data = service.get_territory_points()
        return data
    except Exception as e:
        logger.error(json.dumps({"event": "error", "endpoint": "territory", "error": str(e)}))
        raise HTTPException(status_code=500, detail=str(e))

if __name__ == "__main__":
    port = int(os.getenv("PORT", 8001))
    print(f"🚀 Scout API starting on http://0.0.0.0:{port}")
    uvicorn.run(app, host="0.0.0.0", port=port)
