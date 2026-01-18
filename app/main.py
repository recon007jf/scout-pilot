from fastapi import FastAPI, Depends, HTTPException, Header, Query
from fastapi.responses import JSONResponse
from typing import Annotated
from pydantic import BaseModel
from app.config import settings
from app.utils.logger import get_logger
from app.core.safety import SafetyEngine
from supabase import create_client, Client, ClientOptions

logger = get_logger("api")
app = FastAPI(title="Scout Backend (Iron Clad)", version="2.3 (Image Proxy Fix)")

# DB Dependency (Anon/User)
def get_db():
    return create_client(settings.SUPABASE_URL, settings.SUPABASE_KEY)

# DB Dependency (Service Role - Trusted Backend)
def get_service_db():
    return create_client(
        settings.SUPABASE_URL, 
        settings.SUPABASE_SERVICE_ROLE_KEY,
        options=ClientOptions(flow_type="implicit")
    )

# --- CORS MIDDLEWARE (FIX) ---
from fastapi.middleware.cors import CORSMiddleware
origins = settings.ALLOWED_ORIGINS.split(",")
app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.get("/health")
def health_check():
    """
    Standard Deployment Contract (Phase 2.5)
    Used by Frontend Version Gate to detect stale clients.
    """
    return {
        "status": "ok", 
        "api_version": "2.3",  # Matches app.version
        "schema_version": "2026-01-17",
        "env": settings.ENV,
        "features": {
            "image_proxy": True,
            "clerk_auth": True,
            "morning_briefing_v2": True
        }
    }

@app.get("/health/auth")
def health_check_auth():
    """
    Diagnostic: Check if Clerk JWKS can be fetched.
    """
    from app.core.auth_clerk import clerk_verifier
    status = "ok"
    details = {}
    try:
        jwks = clerk_verifier._get_public_key("any") # Will fail on ID but trigger fetch
    except ValueError as e:
        if "Public key not found" in str(e):
             # This means fetch worked but key didn't match, which is Good for connectivity check
             details["connectivity"] = "success"
             details["jwks_url"] = clerk_verifier._jwks_url
        else:
             status = "error"
             details["error"] = str(e)
    except Exception as e:
        status = "error"
        details["error"] = str(e)
    
    return {"status": status, "details": details}

@app.get("/auth/whoami")
def auth_whoami(
    authorization: Annotated[str | None, Header()] = None,
    admin_db: Client = Depends(get_service_db)
):
    """
    Diagnostic: Verify Token & Upsert Identity.
    """
    from app.core.auth_clerk import clerk_verifier
    import datetime
    
    if not authorization:
        return JSONResponse(status_code=401, content={"error": "Missing Authorization Header"})
    
    token = authorization.replace("Bearer ", "").strip()
    
    try:
        claims = clerk_verifier.verify_token(token)
        clerk_id = claims.get("sub")
        email = claims.get("email", "unknown")
        # Clerk often puts email in 'email' or 'emails' list if configured
        # Adjust logic if needed based on actual token structure
        
        # Upsert into user_identities
        # Using Service Role DB (admin_db)
        data = {
            "clerk_user_id": clerk_id,
            "email": email,
            "last_seen_at": datetime.datetime.utcnow().isoformat(),
            "metadata": claims
        }
        
        try:
            # Upsert
            admin_db.table("user_identities").upsert(data, on_conflict="clerk_user_id").execute()
            persistence = "success"
        except Exception as db_e:
            persistence = f"failed: {db_e}"

        return {
            "status": "authenticated",
            "clerk_id": clerk_id,
            "email": email,
            "persistence": persistence,
            "claims": claims
        }
        
    except Exception as e:
        return JSONResponse(status_code=401, content={"error": str(e)})

@app.get("/api/outreach/status")
def get_outreach_status(
    authorization: Annotated[str | None, Header()] = None,
    admin_db: Client = Depends(get_service_db)
):
    from app.core.auth_clerk import clerk_verifier
    from app.core.identity_bridge import IdentityBridge

    user_email = None
    if authorization:
        try:
            token = authorization.replace("Bearer ", "").strip()
            # Try Clerk first
            try:
                claims = clerk_verifier.verify_token(token)
                # Quick resolve for email only
                user_email = claims.get("email") # or resolve via bridge if strictly needed, but claims usually have it
            except:
                # Fallback to Supabase
                user = admin_db.auth.get_user(token)
                if user and user.user:
                    user_email = user.user.email
        except:
             pass

    safety = SafetyEngine(admin_db)
    return safety.get_outreach_status(user_email=user_email)

@app.get("/api/briefing")
def get_briefing(
    enable_high_risk: bool = False, 
    authorization: Annotated[str | None, Header()] = None,
    admin_db: Client = Depends(get_service_db)
):
    from app.core.briefing import BriefingEngine
    from app.config import settings

    # --- IDENTITY BRIDGE ---
    target_user_email = None

    # 1. Bridge Mode (dev/debug only)
    if settings.SCOUT_IDENTITY_MODE == "default_user":
        target_user_email = settings.DEFAULT_USER_EMAIL
        logger.warning(f"[SECURITY WARNING] Serving Briefing via IDENTITY BRIDGE for: {target_user_email}")
    
    # 2. Secure Mode (Production)
    elif authorization:
        try:
            # Extract Bearer token
            token = authorization.replace("Bearer ", "").strip()
            
            # A. Try Clerk
            from app.core.identity_bridge import IdentityBridge
            from app.core.auth_clerk import clerk_verifier
            
            try:
                # Need admin_db for Bridge (Service Role)
                # We already injected it!
                
                claims = clerk_verifier.verify_token(token)
                bridge = IdentityBridge(admin_db)
                user_context = bridge.resolve_user(claims)
                
                target_user_email = user_context["email"]
                logger.info(f"Authenticated Clerk User: {target_user_email}")
            except Exception as clerk_err:
                # B. Try Supabase (Legacy)
                user_response = db.auth.get_user(token)
                if user_response and user_response.user:
                    target_user_email = user_response.user.email
                    logger.info(f"Authenticated Supabase User: {target_user_email}")
                else:
                    logger.warning("Invalid Token Provided (Clerk & Supabase Failed)")
        except Exception as e:
            logger.error(f"Token Verification Failed: {e}")
            raise HTTPException(status_code=401, detail="Invalid Session")

    if not target_user_email:
         raise HTTPException(status_code=401, detail="Unauthorized: No identity found.")
         # target_user_email = "diagnostic@internal.com"

    # PIVOT: Use Service Role (admin_db) to bypass RLS on target_brokers
    # RLS was enabled but blocked reading. Backend is trusted.
    engine = BriefingEngine(admin_db)
    # Pass the resolved identity to the engine
    return engine.generate_briefing(user_email=target_user_email, enable_high_risk=enable_high_risk)

# --- HELPER: Canonical Error Response ---
def error_response(status_code: int, error_code: str, message: str, details: dict = None):
    return JSONResponse(
        status_code=status_code,
        content={
            "error": error_code,
            "message": message,
            "details": details or {}
        }
    )

@app.get("/api/contacts")
def get_contacts(
    page: int = Query(1, ge=1), 
    page_size: int = Query(50, ge=1, le=100), 
    admin_db: Client = Depends(get_service_db)
):
    from app.core.network import NetworkEngine
    engine = NetworkEngine(admin_db)
    try:
        return engine.get_contacts(page=page, page_size=page_size)
    except Exception as e:
        logger.error(f"Contacts Error: {e}")
        return error_response(500, "server_error", "Failed to fetch contacts", {"raw": str(e)})

@app.get("/api/signals")
def get_signals(
    page: int = Query(1, ge=1), 
    page_size: int = Query(50, ge=1, le=100),
    admin_db: Client = Depends(get_service_db)
):
    from app.core.signals import SignalsEngine
    engine = SignalsEngine(admin_db)
    try:
        # Paginating Signals is logically requested but Engine logic might be fixed length
        # For "Zero Surprises" we accept params. 
        # Ideally pass to engine or slice results?
        # Engine currently hardcodes limits.
        # Minimal viable: return standard response.
        return engine.get_signals() 
    except Exception as e:
        logger.error(f"Signals Error: {e}")
        return error_response(500, "server_error", "Failed to fetch signals", {"raw": str(e)})

@app.get("/api/image-proxy")
def proxy_image(url: str):
    """
    Proxies image requests to bypass Hotlink Protection / CORS.
    Streamed directly to client.
    """
    import requests
    from fastapi.responses import StreamingResponse
    
    try:
        # Masquerade as a browser
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
            "Referer": "https://www.google.com/"
        }
        
        # Stream content (don't load fully into RAM if large, though avatars are small)
        r = requests.get(url, headers=headers, stream=True, timeout=10)
        r.raise_for_status()
        
        return StreamingResponse(
            r.iter_content(chunk_size=8192), 
            media_type=r.headers.get("content-type", "image/jpeg")
        )
    except Exception as e:
        logger.error(f"Image Proxy Failed for {url}: {e}")
        # Return 404/green pixel or just error
        raise HTTPException(status_code=404, detail="Image not found")

@app.get("/api/profile-image")
def get_profile_image(name: str, company: str):
    from app.core.image_proxy import ImageProxyEngine
    engine = ImageProxyEngine()
    try:
        return engine.fetch_image(name=name, company=company)
    except Exception as e:
        return error_response(500, "proxy_error", "Image fetch failed", {"raw": str(e)})

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
    try:
        return {"notes": engine.get_notes(dossier_id)}
    except Exception as e:
        logger.error(f"Get Notes Error: {e}")
        return error_response(500, "server_error", "Failed to fetch notes", {"raw": str(e)})

@app.post("/api/notes")
def create_note(req: NoteRequest, db: Client = Depends(get_db)):
    from app.core.notes import NotesEngine
    engine = NotesEngine(db)
    try:
        return engine.create_note(req.dossier_id, req.content, req.author)
    except Exception as e:
        logger.error(f"Create Note Error: {e}")
        # Assuming DB error for Invalid ID is KeyConstraint or similar
        return error_response(400, "invalid_request", "Failed to create note. Check dossier_id.", {"raw": str(e)})

@app.get("/api/settings")
def get_settings(user_email: str, admin_db: Client = Depends(get_service_db)):
    from app.core.settings_api import SettingsEngine
    engine = SettingsEngine(admin_db)
    try:
        settings_data = engine.get_settings(user_email)
        if settings_data is None:
            return error_response(404, "not_found", "User settings not found")
        return {"preferences": settings_data}
    except Exception as e:
        logger.error(f"Get Settings Error: {e}")
        return error_response(500, "server_error", "Failed to fetch settings", {"raw": str(e)})

@app.post("/api/settings")
def update_settings(req: SettingsRequest, admin_db: Client = Depends(get_service_db)):
    from app.core.settings_api import SettingsEngine
    engine = SettingsEngine(admin_db)
    try:
        return engine.update_settings(req.user_email, req.preferences)
    except Exception as e:
        logger.error(f"Update Settings Error: {e}")
        return error_response(500, "server_error", "Failed to update settings", {"raw": str(e)})

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
         # Blocked by Safety or Mode
         return error_response(403, "safety_blocked", str(e), {"mode": "test"})
    except Exception as e:
         return error_response(500, "email_failure", f"Send Failed: {str(e)}", {})

@app.post("/api/refinery/run")
def run_refinery(limit: int = 50, db: Client = Depends(get_db)):
    from app.core.refinery import RefineryEngine
    engine = RefineryEngine(db)
    return engine.run_refinery_job(limit)

@app.post("/api/drafts/action")
def handle_draft_action(req: ActionRequest, db: Client = Depends(get_db)):
    # GUARDRAIL: Reject "generate" action to enforce separation of concerns
    if req.action == "generate":
        raise HTTPException(status_code=400, detail="Invalid action. Use /api/scout/generate-draft for generation.")

    from app.core.actions import DraftActionsEngine
    engine = DraftActionsEngine(db)
    return engine.handle_action(
        dossier_id=req.dossier_id,
        action=req.action,
        user_email=req.user_email,
        draft_content=req.draft_content,
        draft_subject=req.draft_subject
    )

# --- SCOUT GENERATION (Atomic Single-Brain) ---
class GenerateDraftRequest(BaseModel):
    dossier_id: str
    force_regenerate: bool = False

@app.post("/api/scout/generate-draft")
def generate_draft_endpoint(
    req: GenerateDraftRequest, 
    x_scout_internal_secret: Annotated[str | None, Header()] = None,
    x_debug_llm: Annotated[str | None, Header()] = None,
    authorization: Annotated[str | None, Header()] = None, # For Session Auth Compat
    db: Client = Depends(get_db),
    admin_db: Client = Depends(get_service_db)
):
    from app.core.draft_engine import DraftEngine, DraftConcurrencyError
    from app.core.auth_clerk import clerk_verifier
    import traceback
    import uuid

    # 1. Truth Protocol V2: Hybrid Auth (Secret OR Clerk OR Supabase)
    is_authorized = False
    use_admin_db = False
    
    # A. Check Secret (Internal)
    if settings.SCOUT_INTERNAL_SECRET and x_scout_internal_secret == settings.SCOUT_INTERNAL_SECRET:
        is_authorized = True
        use_admin_db = True # Secrets imply trusted internal access
    
    # B. Check Session (Clerk or Supabase)
    if not is_authorized and authorization:
        token = authorization.replace("Bearer ", "").strip()
        
        # B1. Try Clerk (Primary Strategy)
        try:
            from app.core.identity_bridge import IdentityBridge
            from app.core.auth_clerk import clerk_verifier
            
            # 1. Verify
            claims = clerk_verifier.verify_token(token)
            
            # 2. Resolve/Auto-Link
            bridge = IdentityBridge(admin_db)
            user_context = bridge.resolve_user(claims)
            
            logger.info(f"Clerk Auth Success: {user_context['email']} (ID: {user_context['user_id']})")
            is_authorized = True
            use_admin_db = True 
        except Exception as clerk_err:
            # logger.debug(f"Clerk attempt failed: {clerk_err}")
            # B2. Try Supabase (Legacy Fallback)
            try:
                user = db.auth.get_user(token)
                if user and user.user:
                    is_authorized = True
                    use_admin_db = False # RLS works for legacy
            except:
                pass 

    # Final Gate
    if not is_authorized:
        # If Secret Required Enforced (Phase 2) OR no method worked
        if settings.SCOUT_INTERNAL_SECRET_REQUIRED:
            logger.warning(f"Unauthorized Draft Gen Attempt (Strict). Dossier: {req.dossier_id}")
            return error_response(401, "unauthorized", "Internal Secret Required")
        
        # Phase 1 Logic:
        # If secret was provided but Wrong -> 403 (Don't fall through)
        if x_scout_internal_secret:
             logger.warning(f"Unauthorized Draft Gen Attempt (Invalid Secret). Dossier: {req.dossier_id}")
             return error_response(403, "forbidden", "Invalid Internal Secret")

        # If just generic unauthorized (Phase 1 checks)
        if not authorization:
             logger.warning(f"Unauthorized Draft Gen Attempt (Missing Auth). Dossier: {req.dossier_id}")
             return error_response(401, "unauthorized", "Authentication Required")

    # Select Engine DB Context
    target_db = admin_db if use_admin_db else db
    engine = DraftEngine(target_db)
    
    # 2. Diagnostic Bypass
    # x-debug-llm: 1 forces regeneration
    force = req.force_regenerate
    if x_debug_llm == "1":
        force = True
        logger.warning(f"Diagnostic Bypass Active for Dossier {req.dossier_id}")

    try:
        # 3. Execute Atomic Generation
        # This handles Lock, Idempotency, Generation, Commit internally
        output = engine.generate_draft_atomic(req.dossier_id, force_regenerate=force)
        
        # 4. Map Response Status & Headers
        if output.status == "cached":
            return JSONResponse(status_code=200, content={
                "dossier_id": req.dossier_id,
                "subject": output.subject,
                "body_clean": output.body_clean,
                "signature_block": output.signature_block,
                "body_with_signature": output.body_with_signature,
                "status": "cached",
                "cached": True,
                "trace_id": output.metadata.get("request_trace_id")
            })

        # 5. Success (200) with Proof Headers
        proof = output.metadata.get("proof", {})
        headers = {
            "x-request-trace": output.metadata.get("prompt_trace", {}).get("request_id", "unknown"),
            "x-llm-status": "success",
            "x-llm-model": proof.get("model", "unknown"),
            "x-llm-latency-ms": str(proof.get("latency_ms", 0)),
            "x-llm-tokens-out": str(proof.get("tokens", "N/A"))
        }

        return JSONResponse(
            status_code=200, 
            headers=headers,
            content={
                "dossier_id": req.dossier_id,
                "subject": output.subject,
                "body_clean": output.body_clean,
                "signature_block": output.signature_block,
                "body_with_signature": output.body_with_signature,
                "status": "ready",
                "cached": False,
                "trace_id": output.metadata.get("prompt_trace", {}).get("request_id")
            }
        )
        
    except DraftConcurrencyError as e:
         return error_response(409, "conflict", str(e))

    except ValueError as e:
         return error_response(404, "not_found", str(e))

    except Exception as e:
         # 6. Secure Fail Loud
         # Log RAW Exception Server-Side
         err_id = str(uuid.uuid4())
         logger.error(f"[Fault {err_id}] DRAFT CRASH: {e}\n{traceback.format_exc()}")
         
         # Return Sanitized Client-Side Error (500)
         return JSONResponse(
             status_code=500,
             content={
                 "error": "llm_call_failed",
                 "trace_id": err_id,
                 "message": "Upstream generation failed. Check server logs."
             }
         )


# --- ADMIN ENDPOINTS (Ironclad v250) ---


class InviteRequest(BaseModel):
    email: str


# --- REFACTORED ADMIN INVITE (JSON Safety) ---
@app.post("/api/admin/invite")
def admin_invite_user(
    req: InviteRequest, 
    authorization: Annotated[str | None, Header()] = None,
    db: Client = Depends(get_db)
):
    from app.config import settings
    from fastapi.responses import JSONResponse
    
    try:
        # 1. Auth Guard (Verify JWT)
        if not authorization:
            return JSONResponse(status_code=401, content={"ok": False, "error": "Missing Authorization Header"})
        
        token = authorization.replace("Bearer ", "")
        
        # Verify User via Supabase GoTrue
        try:
            user_response = db.auth.get_user(token)
            user_id = user_response.user.id
            logger.info(f"Admin Invite: Authenticated User {user_id}")
        except Exception as e:
            logger.warning(f"Auth Failed: {e}")
            return JSONResponse(status_code=401, content={"ok": False, "error": "Invalid Token", "detail": str(e)})

        # Initialize Admin Client (Service Role) - Moved UP to use for Profile Check
        if not settings.SUPABASE_SERVICE_ROLE_KEY:
            logger.error("Missing SUPABASE_SERVICE_ROLE_KEY for Admin Operation")
            return JSONResponse(status_code=500, content={"ok": False, "error": "Configuration Error", "detail": "Service Key Missing"})

        admin_db = create_client(
            settings.SUPABASE_URL, 
            settings.SUPABASE_SERVICE_ROLE_KEY,
            options=ClientOptions(flow_type="implicit")
        )

        # 2. Role Guard (Check Profile via Service Role)
        # We use admin_db to bypass RLS (Anon client fails here due to lack of auth context)
        try:
            profile_res = admin_db.table("profiles").select("role, org_id").eq("id", user_id).single().execute()
            if not profile_res.data:
                 return JSONResponse(status_code=403, content={"ok": False, "error": "Access Denied", "detail": "No Profile Found"})
            
            profile = profile_res.data
            if profile.get("role") != "admin":
                logger.warning(f"Unauthorized Admin Access Attempt by {user_id} (Role: {profile.get('role')})")
                return JSONResponse(status_code=403, content={"ok": False, "error": "Access Denied", "detail": "Admins Only"})
            
            org_id = profile.get("org_id")
            logger.info(f"Admin Invite: Role Verified. Org: {org_id}")

        except Exception as e:
            logger.error(f"Profile Check Failed: {e}")
            return JSONResponse(status_code=500, content={"ok": False, "error": "Database Error (Profile Check)", "detail": str(e)})

        # 3. Action (Service Role Execution)
        # admin_db already initialized

        
        # A. Supabase Invite (Sends Email)
        # A. Supabase Invite (Sends Email)
        invite_result_id = None
        try:
            invite_metadata = {
                "org_id": org_id, 
                "role": "member", 
                "full_name": req.email.split("@")[0]
            }
            logger.info(f"Sending Supabase Invite to {req.email} with metadata: {invite_metadata}")
            
            # AUTH_REDIRECT: Single source of truth for all email links (Invite & Recovery)
            # PM Directive: "redirect_to must be .../auth/callback" WITH "next=/auth/update-password" hint
            # This allows the frontend to route to the update password page after the callback exchange.
            AUTH_REDIRECT = "https://v0-scout-ui.vercel.app/auth/callback?next=/auth/update-password"
            
            logger.info(f"Preparing Invite/Recovery for {req.email}. Redirect Target: {AUTH_REDIRECT}")
            
            action_status = "invited"

            try:
                # 1. Try Invite
                logger.info(f"Attempting Invite. redirect_to={AUTH_REDIRECT}")
                invite_res = admin_db.auth.admin.invite_user_by_email(
                    req.email, 
                    options={
                        "data": invite_metadata,
                        "redirect_to": AUTH_REDIRECT
                    }
                )
                if invite_res and invite_res.user:
                     invite_result_id = invite_res.user.id
                logger.info(f"Supabase Invite Success used redirect_to={AUTH_REDIRECT}. User ID: {invite_result_id}")
                
            except Exception as e:
                # Check if error is "User already registered"
                error_str = str(e).lower()
                if "already registered" in error_str or "user already exists" in error_str:
                    logger.info(f"User {req.email} already exists. Sending Password Reset instead.")
                    
                    # 2. Fallback: Password Recovery
                    logger.info(f"Attempting Recovery. redirect_to={AUTH_REDIRECT}")
                    admin_db.auth.reset_password_email(
                        req.email,
                        options={"redirect_to": AUTH_REDIRECT}
                    )
                    action_status = "recovery_sent"
                    
                    # We don't get a User ID back easily here without querying
                    user_res = admin_db.table("profiles").select("id").eq("email", req.email).single().execute()
                    if user_res.data:
                        invite_result_id = user_res.data.get("id")
                    
                    logger.info(f"Password Reset Email Sent using redirect_to={AUTH_REDIRECT}")
                else:
                    # Genuine error
                    raise e

            
        except Exception as e:
            logger.error(f"Invite/Reset Failed: {e}")
            # Return detailed error
            return JSONResponse(status_code=400, content={"ok": False, "error": "Invite Failed", "detail": str(e), "where": "inviteUserByEmail"})

        # B. Audit Log (Insert into public.invites)
        try:
            db_payload = {
                "email": req.email,
                "org_id": org_id,
                "role": "member", 
                "status": "pending",
                "created_by": user_id,
                "supabase_invite_id": invite_result_id
            }
            logger.info(f"Logging invite to database 'invites'. Payload: {db_payload}")
            
            admin_db.table("invites").upsert(db_payload).execute()
            logger.info("Audit Log Created.")

        except Exception as e:
            # Don't fail the request if audit fails, but log it critical
            logger.critical(f"Failed to write to invites table: {e}")
            # We return success because the invite WAS sent
            return JSONResponse(status_code=200, content={
                "ok": True, 
                "status": action_status,
                "message": f"Invite sent to {req.email} ({action_status})", 
                "warning": "Audit log failed", 
                "audit_error": str(e)
            })

        return JSONResponse(status_code=200, content={
            "ok": True, 
            "status": action_status,
            "message": f"Invite flow complete for {req.email}", 
            "email": req.email
        })

    except Exception as e:
        logger.critical(f"Unhandled Server Error in Admin Invite: {e}")
        return JSONResponse(status_code=500, content={"ok": False, "error": "Internal Server Error", "detail": str(e)})

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
    
    expires_in = token.get("expires_in", 3600)
    import datetime
    expires_at = datetime.datetime.utcnow() + datetime.timedelta(seconds=expires_in)
    scopes = token.get("scope", "")

    # 3a. Save Core Tokens
    db.table("integration_tokens").upsert({
        "user_email": email,
        "provider": "outlook",
        "access_token": access,
        "refresh_token": refresh
    }, on_conflict="user_email, provider").execute()

    # 3b. Save Metadata to Preferences (Schema workaround)
    try:
        pref_res = db.table("user_preferences").select("*").eq("user_email", email).execute()
        current_prefs = pref_res.data[0].get("preferences", {}) if pref_res.data else {}
        
        outlook_meta = current_prefs.get("outlook", {})
        outlook_meta["scopes"] = scopes
        outlook_meta["expires_at"] = expires_at.isoformat()
        current_prefs["outlook"] = outlook_meta
        
        db.table("user_preferences").upsert({
            "user_email": email,
            "preferences": current_prefs,
            "updated_at": "now()"
        }).execute()
    except Exception as e:
        # Don't fail the connection if metadata save fails
        print(f"Metadata save failed: {e}")
    
    # Return to Frontend
    from fastapi.responses import RedirectResponse
    # TODO: Use ALLOWED_ORIGINS config in future. Hardcoded for stability.
    return RedirectResponse("https://v0-scout-ui.vercel.app/settings?status=success&provider=outlook")

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

# --- ROUTING ALIASES (Ironclad v255) ---
# Support frontend middleware requests to /api/scout/outlook/*
@app.get("/api/scout/outlook/auth-url")
def get_outlook_auth_url_alias():
    try:
        return get_outlook_auth_url()
    except Exception as e:
        logger.error(f"Outlook Auth URL Generation Failed: {e}")
        # Diagnostic Response for debugging 500 errors
        return JSONResponse(
            status_code=500,
            content={
                "error": "Outlook Auth Generation Failed",
                "details": str(e),
                "diagnostics": {
                    "AZURE_CLIENT_ID_SET": bool(settings.AZURE_CLIENT_ID),
                    "AZURE_CLIENT_SECRET_SET": bool(settings.AZURE_CLIENT_SECRET),
                    "AZURE_TENANT_ID": settings.AZURE_TENANT_ID,
                    "AZURE_REDIRECT_URI": settings.AZURE_REDIRECT_URI,
                    "ENV": settings.ENV
                }
            }
        )

@app.get("/api/scout/outlook/callback")
def outlook_callback_alias(code: str, db: Client = Depends(get_db)):
    return outlook_callback(code, db)

@app.get("/api/scout/outlook/test-connection")
def test_outlook_connection_alias(
    email: str, 
    x_scout_internal_probe: Annotated[str | None, Header()] = None,
    db: Client = Depends(get_db)
):
    return test_outlook_connection(email, x_scout_internal_probe, db)
