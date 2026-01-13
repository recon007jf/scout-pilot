from supabase import Client
from typing import List, Optional, Dict, Any
from pydantic import BaseModel
from datetime import datetime, timedelta
from app.utils.logger import get_logger

logger = get_logger("signals")

# --- Models ---
# Simplified Contact for Embedding
class SignalContact(BaseModel):
    id: str
    full_name: str
    firm: str
    role: str
    work_email: Optional[str] = ""
    linkedin_url: Optional[str] = ""

class Signal(BaseModel):
    id: str
    type: str  # email_reply | job_change | linkedin_post | company_news
    priority: str # high | medium | low
    priority_score: int
    timestamp: str # ISO
    title: str
    details: str
    actionable: bool
    contact: SignalContact
    metadata: Dict[str, Any] = {}

class SignalsResponse(BaseModel):
    signals: List[Signal]

# --- Engine ---
class SignalsEngine:
    def __init__(self, db: Client):
        self.db = db

    def get_signals(self) -> Dict[str, Any]:
        """
        Synthesizes signals from recent Dossier activity.
        Since we lack a live event bus, we infer signals from state.
        """
        signals = []

        # 1. Fetch "Warm" or "High-Risk" Profiles (High Priority)
        # 1. Fetch "Warm" or "High-Risk" Profiles (High Priority)
        # Get top 5 interesting profiles
        res = self.db.table("psyche_profiles")\
            .select("*, dossiers(*)")\
            .in_("risk_profile", ["Warm", "High-Risk"])\
            .order("updated_at", desc=True)\
            .limit(5)\
            .execute()
            
        for row in res.data:
            dossier = row.get("dossiers")
            if not dossier: continue
            
            # Check formatting of dossier (it might be a list or dict)
            if isinstance(dossier, list) and len(dossier) > 0:
                dossier = dossier[0]
            
            risk = row.get("risk_profile")
            
            # Map to Signal
            sig_type = "company_news"
            title = "High Value Target Detected"
            priority = "high"
            score = 90
            details = f"{dossier.get('full_name')} at {dossier.get('firm')} is marked as {risk}."
            
            if risk == "Warm":
                sig_type = "email_reply" # Proxy for engagement
                title = "Engagement Opportunity"
                score = 95
                details = "Target profile indicates warm reception likely."
            
            # Create Signal Object
            contact = SignalContact(
                id=dossier["id"],
                full_name=dossier.get("full_name", "Unknown"),
                firm=dossier.get("firm", "Unknown"),
                role=dossier.get("role", ""),
                work_email=dossier.get("work_email", ""),
                linkedin_url=dossier.get("linkedin_url", "") or ""
            )
            
            # Use psyche_profile ID as Signal ID base to be consistent but unique per day?
            # Just use random or hash
            sig_id = f"sig_{row['id']}_{str(datetime.now().date())}"
            
            s = Signal(
                id=sig_id,
                type=sig_type,
                priority=priority,
                priority_score=score,
                timestamp=str(row.get("updated_at") or datetime.now().isoformat()),
                title=title,
                details=details,
                actionable=True,
                contact=contact,
                metadata={"risk_profile": risk}
            )
            signals.append(s)
            
        # 2. Fetch Recently Added (New Prospects)
        try:
            # Get 5 most recently created dossiers
            res2 = self.db.table("dossiers")\
                .select("*")\
                .order("created_at", desc=True)\
                .limit(5)\
                .execute()
                
            for doc in res2.data:
                # Dedupe if already added via Warm check
                if any(s.contact.id == doc["id"] for s in signals):
                    continue
                    
                contact = SignalContact(
                    id=doc["id"],
                    full_name=doc.get("full_name", "Unknown"),
                    firm=doc.get("firm", "Unknown"),
                    role=doc.get("role", ""),
                    work_email=doc.get("work_email", ""),
                    linkedin_url=doc.get("linkedin_url", "") or ""
                )
                
                s = Signal(
                    id=f"new_{doc['id']}",
                    type="company_news", # "New Prospect"
                    priority="medium",
                    priority_score=50,
                    timestamp=str(doc.get("created_at")),
                    title="New Prospect Added",
                    details=f"{doc.get('full_name')} joined the dossier list.",
                    actionable=False,
                    contact=contact,
                    metadata={"source": "ingestion"}
                )
                signals.append(s)
                
        except Exception as e:
            logger.error(f"Error fetching new dossiers: {e}")

        # Sort by timestamp desc
        signals.sort(key=lambda x: x.timestamp, reverse=True)

        return {"signals": [s.dict() for s in signals]}
