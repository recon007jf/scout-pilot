from supabase import Client
from typing import List, Optional, Dict, Any
from pydantic import BaseModel
from datetime import datetime
from app.utils.logger import get_logger

logger = get_logger("notes")

class Note(BaseModel):
    id: str
    dossier_id: str
    content: str
    author: str
    created_at: str

class NotesEngine:
    def __init__(self, db: Client):
        self.db = db

    def get_notes(self, dossier_id: str) -> List[Dict]:
        try:
            res = self.db.table("dossier_notes").select("*").eq("dossier_id", dossier_id).order("created_at", desc=True).execute()
            return res.data
        except Exception as e:
            logger.error(f"Get Notes Error: {e}")
            return []

    def create_note(self, dossier_id: str, content: str, author: str = "User") -> Dict:
        try:
            payload = {
                "dossier_id": dossier_id,
                "content": content,
                "author": author
            }
            res = self.db.table("dossier_notes").insert(payload).execute()
            if res.data:
                return res.data[0]
            return {}
        except Exception as e:
            logger.error(f"Create Note Error: {e}")
            return {"error": str(e)}
