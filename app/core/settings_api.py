from supabase import Client
from typing import Dict, Any
from app.utils.logger import get_logger

logger = get_logger("settings")

class SettingsEngine:
    def __init__(self, db: Client):
        self.db = db

    def get_settings(self, user_email: str) -> Optional[Dict[str, Any]]:
        res = self.db.table("user_preferences").select("*").eq("user_email", user_email).execute()
        if res.data:
            return res.data[0].get("preferences", {})
        return None  # Signal Not Found

    def update_settings(self, user_email: str, preferences: Dict[str, Any]) -> Dict[str, Any]:
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
