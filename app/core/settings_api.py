from supabase import Client
from typing import Dict, Any
from app.utils.logger import get_logger

logger = get_logger("settings")

class SettingsEngine:
    def __init__(self, db: Client):
        self.db = db

    def get_settings(self, user_email: str) -> Dict[str, Any]:
        try:
            res = self.db.table("user_preferences").select("*").eq("user_email", user_email).execute()
            if res.data:
                return res.data[0].get("preferences", {})
            return {} # Default empty
        except Exception as e:
            logger.error(f"Get Settings Error: {e}")
            return {}

    def update_settings(self, user_email: str, preferences: Dict[str, Any]) -> Dict[str, Any]:
        try:
            # Upsert
            payload = {
                "user_email": user_email,
                "preferences": preferences,
                "updated_at": "now()"
            }
            res = self.db.table("user_preferences").upsert(payload).execute()
            if res.data:
                return res.data[0].get("preferences", {})
            return preferences
        except Exception as e:
            logger.error(f"Update Settings Error: {e}")
            return {"error": str(e)}
