from supabase import Client
from typing import List, Optional, Dict, Any
from pydantic import BaseModel
from datetime import datetime
from app.utils.logger import get_logger

logger = get_logger("network")

# --- Models ---
class Contact(BaseModel):
    id: str
    full_name: str
    firm: str
    role: Optional[str] = ""
    work_email: Optional[str] = ""
    linkedin_url: Optional[str] = ""
    relationship_status: str  # engaged | active | prospect | cold
    engagement_score: int
    last_contact: Optional[str] = None # ISO format
    tags: List[str] = []

class Pagination(BaseModel):
    page: int
    page_size: int
    total: int

class NetworkResponse(BaseModel):
    contacts: List[Contact]
    pagination: Pagination

# --- Engine ---
class NetworkEngine:
    def __init__(self, db: Client):
        self.db = db

    def get_contacts(self, page: int = 1, page_size: int = 50) -> Dict[str, Any]:
        """
        Fetches paginated contacts from Dossiers + Psyche Profiles.
        Maps internal Risk Profile to Relationship Status.
        """
        offset = (page - 1) * page_size
        
        # 1. Get Total Count
        # Using count="exact" with returning minimal data
        count_res = self.db.table("dossiers").select("id", count="exact", head=True).execute()
        total = count_res.count or 0
        
        # 2. Fetch Data
        # Join with psyche_profiles for risk/status context
        # Note: Supabase join syntax: select("*, psyche_profiles(*)") based on FK
        # Assuming FK exists from psyche_profiles -> dossiers or vice versa.
        # Check: psyche_profiles has dossier_id. So we select from dossiers and embed psyche_profiles?
        # Supabase: select *, psyche_profiles(*)
        
        try:
            res = self.db.table("dossiers")\
                .select("*, psyche_profiles(*)")\
                .order("updated_at", desc=True)\
                .range(offset, offset + page_size - 1)\
                .execute()
                
            raw_contacts = res.data
        except Exception as e:
            logger.error(f"Failed to fetch contacts: {e}")
            return {"contacts": [], "pagination": {"page": page, "page_size": page_size, "total": 0}}

        contacts = []
        
        for row in raw_contacts:
            # Safely extract nested psyche profile
            psyche = row.get("psyche_profiles") 
            # psyche might be a list (one-to-many) or dict (one-to-one) or None depending on schema/relation
            # In Supabase JS it returns array or object depending on relation type setup.
            # Assuming one-to-one or one-to-many, we take the first or the object.
            
            p_profile = {}
            if psyche:
                 if isinstance(psyche, list) and len(psyche) > 0:
                     p_profile = psyche[0]
                 elif isinstance(psyche, dict):
                     p_profile = psyche
            
            # Map Fields
            risk = p_profile.get("risk_profile", "Cold-Safe")
            
            # Status Mapping
            rel_status = "cold"
            score = 10
            if risk == "Warm":
                rel_status = "active"
                score = 80
            elif risk == "High-Risk":
                rel_status = "prospect" # or 'cold' but high priority?
                score = 50
            elif risk == "Cold-Safe":
                rel_status = "cold"
                score = 10
                
            # Tags
            base_tags = []
            if p_profile.get("base_archetype"):
                base_tags.append(p_profile.get("base_archetype"))
            if row.get("tier"):
                base_tags.append(row.get("tier"))
            
            # LinkedIn - check raw_data if top-level missing
            li_url = row.get("linkedin_url", "")
            if not li_url and row.get("raw_data"):
                li_url = row["raw_data"].get("LinkedIn URL", "")
            
            c = Contact(
                id=row["id"],
                full_name=row.get("full_name", "Unknown"),
                firm=row.get("firm", "Unknown"),
                role=row.get("role", ""),
                work_email=row.get("work_email", ""),
                linkedin_url=li_url,
                relationship_status=rel_status,
                engagement_score=score,
                last_contact=str(row.get("updated_at")), # using updated_at as proxy for now
                tags=base_tags
            )
            contacts.append(c)

        return {
            "contacts": [c.dict() for c in contacts],
            "pagination": {
                "page": page,
                "page_size": page_size,
                "total": total
            }
        }
