from fastapi import FastAPI, Depends, HTTPException, Header, Query
from typing import Annotated
from app.config import settings
from app.utils.logger import get_logger
from app.core.safety import SafetyEngine
from supabase import create_client, Client

logger = get_logger("api")
app = FastAPI(title="Scout Backend (Iron Clad)", version="2.0")

# DB Dependency
def get_db():
    return create_client(settings.SUPABASE_URL, settings.SUPABASE_KEY)

@app.get("/health")
def health_check():
    return {"status": "ok", "version": "2.0", "env": settings.ENV}

@app.get("/api/outreach/status")
def get_status(db: Client = Depends(get_db)):
    safety = SafetyEngine(db)
    return safety.get_outreach_status()

@app.get("/api/briefing")
def get_briefing(enable_high_risk: bool = False, db: Client = Depends(get_db)):
    from app.core.briefing import BriefingEngine
    engine = BriefingEngine(db)
    return engine.generate_briefing(enable_high_risk=enable_high_risk)

@app.get("/api/contacts")
def get_contacts(
    page: int = Query(1, ge=1), 
    page_size: int = Query(50, ge=1, le=100), 
    db: Client = Depends(get_db)
):
    from app.core.network import NetworkEngine
    engine = NetworkEngine(db)
    return engine.get_contacts(page=page, page_size=page_size)

@app.get("/api/signals")
def get_signals(db: Client = Depends(get_db)):
    from app.core.signals import SignalsEngine
    engine = SignalsEngine(db)
    return engine.get_signals()

@app.get("/api/profile-image")
def get_profile_image(name: str, company: str):
    from app.core.image_proxy import ImageProxyEngine
    engine = ImageProxyEngine()
    return engine.fetch_image(name=name, company=company)

# --- V0 SUPPORT (Notes, Settings, Drafts) ---

# Request Models
from pydantic import BaseModel
class NoteRequest(BaseModel):
    dossier_id: str
    content: str
    author: str = "User"

class SettingsRequest(BaseModel):
    user_email: str
    preferences: dict

class ActionRequest(BaseModel):
    dossier_id: str
    action: str # approved, dismissed, paused
    user_email: str
    draft_content: str = ""
    draft_subject: str = "Introduction"

@app.get("/api/notes")
def get_notes(dossier_id: str, db: Client = Depends(get_db)):
    from app.core.notes import NotesEngine
    engine = NotesEngine(db)
    return {"notes": engine.get_notes(dossier_id)}

@app.post("/api/notes")
def create_note(req: NoteRequest, db: Client = Depends(get_db)):
    from app.core.notes import NotesEngine
    engine = NotesEngine(db)
    return engine.create_note(req.dossier_id, req.content, req.author)

@app.get("/api/settings")
def get_settings(user_email: str, db: Client = Depends(get_db)):
    from app.core.settings_api import SettingsEngine
    engine = SettingsEngine(db)
    return {"preferences": engine.get_settings(user_email)}

@app.post("/api/settings")
def update_settings(req: SettingsRequest, db: Client = Depends(get_db)):
    from app.core.settings_api import SettingsEngine
    engine = SettingsEngine(db)
    return engine.update_settings(req.user_email, req.preferences)

class SendRequest(BaseModel):
    candidate_id: str
    final_subject: str
    final_body: str
    user_email: str

@app.post("/api/scout/email/send")
def send_email(req: SendRequest, db: Client = Depends(get_db)):
    from app.core.email import EmailEngine
    engine = EmailEngine(db)
    try:
        return engine.send_email(req.user_email, req.candidate_id, req.final_subject, req.final_body)
    except PermissionError as e:
        raise HTTPException(status_code=403, detail=str(e))

@app.post("/api/refinery/run")
def run_refinery(limit: int = 50, db: Client = Depends(get_db)):
    from app.core.refinery import RefineryEngine
    engine = RefineryEngine(db)
    return engine.run_refinery_job(limit)

@app.post("/api/drafts/action")
def handle_draft_action(req: ActionRequest, db: Client = Depends(get_db)):
    from app.core.actions import DraftActionsEngine
    engine = DraftActionsEngine(db)
    return engine.handle_action(
        dossier_id=req.dossier_id,
        action=req.action,
        user_email=req.user_email,
        draft_content=req.draft_content,
        draft_subject=req.draft_subject
    )

# --- OUTLOOK PROBE ENDPOINTS ---
@app.get("/api/outlook/auth-url")
def get_outlook_auth_url():
    from app.core.outlook import OutlookAuth
    auth = OutlookAuth()
    return {"url": auth.get_auth_url()}

@app.get("/api/outlook/callback")
def outlook_callback(code: str, db: Client = Depends(get_db)):
    from app.core.outlook import OutlookAuth
    
    # 1. Exchange Code for Token
    auth = OutlookAuth()
    token = auth.acquire_token_by_code(code)
    
    # 2. Extract User Info
    # Ideally use ID Token, but for probe we'll rely on Access Token logic or graph call
    # MSAL result usually contains "id_token_claims"
    claims = token.get("id_token_claims", {})
    email = claims.get("preferred_username") or claims.get("email")
    
    if not email:
        raise HTTPException(status_code=400, detail="Could not identify user from token")
        
    # 3. Persist (Upsert)
    # Using raw SQL via rpc or just upsert if table allows.
    # We defined UNIQUE(user_email, provider)
    access = token.get("access_token")
    refresh = token.get("refresh_token")
    
    # Calculate expiry? MSAL handles it, but for DB we might want it.
    # Ignoring exact expiry for Probe.
    
    db.table("integration_tokens").upsert({
        "user_email": email,
        "provider": "outlook",
        "access_token": access,
        "refresh_token": refresh
    }, on_conflict="user_email, provider").execute()
    
    return "Connection Established. Implementation: Operational."

@app.get("/api/outlook/test-connection")
def test_outlook_connection(
    email: str, 
    x_scout_internal_probe: Annotated[str | None, Header()] = None,
    db: Client = Depends(get_db)
):
    """
    Manually triggers a draft creation for the given user email.
    Secured by SCOUT_INTERNAL_PROBE_KEY.
    """
    from app.core.outlook import OutlookAuth, OutlookClient
    from app.config import settings
    
    # Security Gate: Internal Header Check
    if not settings.SCOUT_INTERNAL_PROBE_KEY or x_scout_internal_probe != settings.SCOUT_INTERNAL_PROBE_KEY:
         logger.warning(f"Unauthorized Outlook Probe Attempt. User: {email}")
         raise HTTPException(status_code=403, detail="Access Denied: Internal Probe Key Required")
    
    # 1. Fetch Credentials
    res = db.table("integration_tokens").select("*").eq("user_email", email).eq("provider", "outlook").execute()
    if not res.data:
        raise HTTPException(status_code=404, detail="User not authenticated")
        
    record = res.data[0]
    refresh_token = record.get("refresh_token")
    
    # 2. Refresh Token (Ensure validity)
    auth = OutlookAuth()
    new_tokens = auth.refresh_token(refresh_token)
    access_token = new_tokens.get("access_token")
    
    # Update DB with new tokens
    if new_tokens.get("refresh_token"):
         db.table("integration_tokens").update({
             "access_token": access_token,
             "refresh_token": new_tokens.get("refresh_token")
         }).eq("id", record['id']).execute()
    
    # 3. Probe Graph API
    client = OutlookClient(access_token)
    
    # SAFETY: Always allow Read (GET /me) for auth verification
    profile = client.get_me()
    
    # SAFETY: Only allow Write (Create Draft) if Real Send is enabled
    draft_id = "simulated_draft_id_safety_latch"
    draft_created = False
    
    if settings.ALLOW_REAL_SEND:
        draft = client.create_draft(
            subject="Scout Probe: Connection Test",
            body="<h1>Operational</h1><p>The backend has successfully connected to Outlook.</p>",
            to_emails=[email] # Send to self
        )
        draft_id = draft.get("id")
        draft_created = True
    else:
        logger.info("SAFETY LATCH: Skipping Draft Creation for Probe")
    
    return {
        "status": "success",
        "user": profile.get("userPrincipalName"),
        "draft_created": draft_created,
        "draft_id": draft_id,
        "mode": "live" if settings.ALLOW_REAL_SEND else "test_read_only", 
        "scopes_grant_status": "Admin Consent Likely Not Required" 
    }
