from pydantic import BaseModel
from typing import List, Optional, Any
import json
import datetime
from supabase import Client
import google.generativeai as genai
from app.config import settings
from app.domain.product_facts import PRODUCT_FACTS, get_product_context_str
from app.utils.logger import get_logger

logger = get_logger("draft_engine")

# Configure Gemini
try:
    genai.configure(api_key=settings.GEMINI_API_KEY)
except Exception as e:
    logger.warning(f"AI Config Failed (Check Key): {e}")

# --- SCOUT CONSTITUTION ---
SCOUT_CONSTITUTION = """
IDENTITY:
- You are writing on behalf of Andrew Oram, Regional Sales Director at Point C Health.
- Never write on behalf of "Pacific AI Systems" or "Scout".
- "Scout" is an internal tool name and MUST NEVER appear in the output.
- Do not mention "AI", "automation", "LLM", or "Gemini".

VOICE:
- Clear, grounded, human, intelligent.
- No marketing fluff, hype, or corporate clichés.
- BANNED PHRASES: "I hope this message finds you well", "cutting-edge", "synergy", "circle back", "innovative solution", "fast-paced environment", "streamline", "Scout AI".

VALUE STATEMENT (Truth / use as the core offer):
- Point C Health helps organizations maintain benefits data accuracy, support compliance, and improve employee engagement through benefits administration services and support.
- (Constraint: Do not over-claim. Keep it grounded. No invented metrics.)

STRUCTURE:
- Grounded opener tied to the person/role/company.
- One relevant observation (from dossier/notes if present; if absent, use role-based insight without inventing facts).
- One concrete value statement about Point C Health contextualized to their role.
- Low-pressure, question-based CTA.

HALLUCINATION RULES:
- Do not invent facts, initiatives, or prior interactions.
- If info is missing, be concise and ask a smart question instead of guessing.
- Never output placeholders like [Your Name], [Your Title].
- Never mention the tool name "Scout".

SIGNATURE RULE:
- Do NOT generate a signature block. The backend will append the official signature.
"""

# --- OFFICIAL SIGNATURE ---
OFFICIAL_SIGNATURE = """
Andrew Oram
Regional Sales Director
Point C
San Diego, CA
Mobile: 619-865-8330
andrew.oram@pointchealth.com"""



class DraftConcurrencyError(Exception):
    pass

class DraftOutput(BaseModel):
    subject: str
    body_clean: str # Editable body (NO signature)
    signature_block: str # Static signature
    body_with_signature: str # Convenience (body_clean + sig)
    metadata: dict
    status: str # 'success', 'error', 'cached'

class DraftEngine:
    def __init__(self, db: Client = None):
        self.db = db
        self.db = db
        # 01-11-2026: Updated to Standardized Gemini 2.0 Flash
        # Note: SCOUT_CONSTITUTION is defined globally.
        # We bind it here so the model treats it as authoritative.
        self.model = genai.GenerativeModel(
            settings.GEMINI_MODEL_NAME,
            system_instruction=SCOUT_CONSTITUTION
        )

    def generate_draft_atomic(self, dossier_id: str, force_regenerate: bool = False) -> DraftOutput:
        """
        Single-Brain Entry Point:
        1. Fetch State (Idempotency)
        2. Atomic Lock (Conditional Update)
        3. Generate (Gemini)
        4. Commit (Write)
        """
        import uuid
        import os
        
        request_trace_id = str(uuid.uuid4())
        logger.info(f"[GenerateDraft][trace={request_trace_id}] dossier_id={dossier_id} action=start")

        if not self.db:
            raise ValueError("Database client required for atomic generation")

        # 0. Robustness: Lazy Create (Upsert Dossier)
        # Prevents 404s if the dossier row doesn't exist yet (e.g. race condition)
        try:
            import hashlib
            lazy_hash = hashlib.sha256(str(dossier_id).encode()).hexdigest()
            
            self.db.table("dossiers").upsert({
                "id": dossier_id,
                "identity_key": lazy_hash, # Satisfy Hash Format
                "identity_type": "hash", # Satisfy Enum 'hash'
                "full_name": "Lazy Created Target",
                "org_id": "df966238-4b56-4ed3-886c-157854d8ce90", # Standard Org ID observed in Production
                "updated_at": datetime.datetime.utcnow().isoformat()
            }, on_conflict="id").execute()
            logger.info(f"[GenerateDraft][trace={request_trace_id}] dossier_id={dossier_id} action=upsert_dossier result=success")
        except Exception as e:
            logger.error(f"[GenerateDraft][trace={request_trace_id}] action=upsert_dossier result=error details={e}")
            # We continue, as it might already exist and just failed update, or it will fail later meaningfully.

        # 1. Fetch State & Data
        try:
            # 1. Fetch State (Check drafts table via dossier_id)
            # Since 'dossier_id' is unique in 'drafts', .execute() is safe
            draft_res = self.db.table("drafts").select("*").eq("dossier_id", dossier_id).execute()
            
            status = "idle"
            existing_body = None
            draft_record = {}
            
            if draft_res.data:
                draft_record = draft_res.data[0]
                status = draft_record.get("status", "idle")
                existing_body = draft_record.get("email_body")
            
            # Need dossier data for context!
            dossier_res = self.db.table("dossiers").select("*").eq("id", dossier_id).execute()
            if not dossier_res.data:
                # Should be impossible now due to Step 0, but good to keep safest
                raise ValueError("Dossier not found after upsert")
            dossier = dossier_res.data[0]
            
            # IDEMPOTENCY CHECK
            disable_cache = os.environ.get("SCOUT_DISABLE_CACHE", "false").lower() == "true"
            should_regenerate = force_regenerate or disable_cache

            if status == 'ready' and not should_regenerate and existing_body:
                logger.info(f"[GenerateDraft][trace={request_trace_id}] action=check_cache result=hit")
                return DraftOutput(
                    subject=draft_record.get("email_subject", ""),
                    body_clean=existing_body,
                    signature_block=OFFICIAL_SIGNATURE.strip(),
                    body_with_signature=f"{existing_body}\n\n{OFFICIAL_SIGNATURE.strip()}",
                    metadata={"cached": True, "request_trace_id": request_trace_id},
                    status="cached"
                )
            
            if status == 'generating' and not should_regenerate:
                # Truth Protocol V2: Concurrency Lock -> 409
                logger.warning(f"[GenerateDraft][trace={request_trace_id}] action=check_lock result=locked")
                raise DraftConcurrencyError(f"Draft generation already in progress for {dossier_id}")
            
            if should_regenerate:
                logger.info(f"[GenerateDraft][trace={request_trace_id}] action=check_cache result=bypass force={force_regenerate}")

        except DraftConcurrencyError:
            raise # Propagate up
        except Exception as e:
            logger.error(f"[GenerateDraft][trace={request_trace_id}] action=fetch_state result=error details={e}")
            raise e

        # 2. Atomic Lock (Upsert Pattern on drafts table)
        try:
            # Upsert 'generating' state using on_conflict
            self.db.table("drafts").upsert({
                "dossier_id": dossier_id,
                "status": "generating",
                "last_error": None,
                "updated_at": datetime.datetime.utcnow().isoformat()
            }, on_conflict="dossier_id").execute()
            
            logger.info(f"[GenerateDraft][trace={request_trace_id}] action=acquire_lock result=success")

        except Exception as e:
            logger.error(f"[GenerateDraft][trace={request_trace_id}] action=acquire_lock result=error details={e}")
            raise e

        # 3. Generate (Scout Constitution)
        try:
            # Prepare Context
            raw_data = dossier.get("raw_data", {})
            # Map known CSV columns to standard keys
            profile = {
                "full_name": dossier.get("full_name") or raw_data.get("Full Name") or "Broker",
                "title": dossier.get("role") or raw_data.get("Role") or "Benefits Leader",
                "firm": dossier.get("firm") or raw_data.get("Firm") or "Company",
                "region": raw_data.get("State") or raw_data.get("Region") or "Unknown",
                "linkedin_url": dossier.get("linkedin_url") or raw_data.get("LinkedIn URL") or "Unknown"
            }
            
            # Fetch Notes/Psyche for context
            analysis = {"notes": "No specific notes found. Use role insights."}
            try:
                notes_res = self.db.table("dossier_notes").select("content").eq("dossier_id", dossier_id).execute()
                if notes_res.data:
                    analysis["notes"] = " ".join([n["content"] for n in notes_res.data])
            except:
                pass

            # Core Generation Call
            logger.info(f"[GenerateDraft][trace={request_trace_id}] action=llm_call status=start")
            output = self._generate_core(profile, analysis, request_id=request_trace_id)

            # 4. Commit
            current_version = dossier.get("llm_draft_version", 0)
            new_version = current_version + 1 if force_regenerate else 1 

            # 4. Commit to drafts table
            self.db.table("drafts").upsert({
                "dossier_id": dossier_id,
                "email_subject": output.subject,
                "email_body": output.body_clean, # Store clean body
                "status": "ready",
                "updated_at": datetime.datetime.utcnow().isoformat(),
                "version": new_version
            }, on_conflict="dossier_id").execute()

            # 5. Dual-Write to 'dossiers' for Frontend Compatibility
            self.db.table("dossiers").update({
                "llm_email_subject": output.subject,
                "llm_email_body": output.body_with_signature, 
                "llm_draft_status": "ready",
                "llm_draft_version": new_version,
                "llm_last_error": None
            }).eq("id", dossier_id).execute()

            logger.info(f"[GenerateDraft][trace={request_trace_id}] action=persist_draft status=success")
            return output

        except Exception as e:
            # FAIL LOUD: Log error, release lock to error, then RE-RAISE
            logger.error(f"[GenerateDraft][trace={request_trace_id}] action=generate_commit result=error details={e}")
            
            try:
                # Attempt to release lock to error state for visibility
                self.db.table("dossiers").update({
                    "llm_draft_status": "error",
                    "llm_last_error": str(e)
                }).eq("id", dossier_id).execute()
                
                self.db.table("drafts").upsert({
                    "dossier_id": dossier_id,
                    "status": "error",
                    "last_error": str(e)
                }, on_conflict="dossier_id").execute()
            except:
                logger.critical(f"[GenerateDraft][trace={request_trace_id}] Failed to release lock during crash.")

            raise e # Propagation to Main for 500

    def _generate_core(self, broker_profile: dict, analysis: dict, request_id: str = None) -> DraftOutput:
        """
        Internal generation logic using the Constitution.
        Returns clean subject/body/full_email.
        """
        import hashlib
        import uuid
        
        # Use threaded ID if passed, else local
        if not request_id:
            request_id = str(uuid.uuid4())
        
        # 1. Build Dynamic Context Payload

        name = broker_profile.get("full_name", "Broker")
        title = broker_profile.get("title", "Benefits Leader")
        company = broker_profile.get("firm", "your firm")
        region = broker_profile.get("region", "Unknown")
        linkedin = broker_profile.get("linkedin_url", "Unknown")
        
        dossier_content = analysis.get("notes") or analysis.get("signal_reasoning") or "No dossier available. Use role-based insights and ask a smart question."

        user_content = f"""
        TARGET: {name}
        TITLE: {title}
        COMPANY: {company}
        REGION: {region}
        LINKEDIN: {linkedin}
        DOSSIER/NOTES: {dossier_content}

        TASK:
        Write a first-touch outreach email (subject + body). Keep it concise. Do not include a signature.
        
        FORMAT:
        Output MUST be valid JSON matching: {{ "subject": "...", "body": "...", "metadata": {{ "confidence": 0.9, "unknowns": [] }} }}
        """

        # Trace Data
        const_hash = hashlib.sha256(SCOUT_CONSTITUTION.encode()).hexdigest()
        trace = {
            "request_id": request_id,
            "dossier_id": "unknown_in_core", # Populated by caller
            "model": "gemini-flash-latest",
            "system_instruction_hash": const_hash,
            "system_instruction_len": len(SCOUT_CONSTITUTION),
            "user_prompt_len": len(user_content),
            "context_flags": {
                "has_role": bool(title),
                "has_notes": bool(analysis.get("notes")),
                "has_signals": bool(analysis.get("signal_reasoning"))
            },
            "guardrails_enabled": True
        }

        # Debug Logging for User
        logger.info(f"[Trace {request_id}] MODEL: {settings.GEMINI_MODEL_NAME}")
        logger.info(f"[Trace {request_id}] CONS_HASH: {const_hash}")
        
        # Compliance Guardrail Loop
        max_retries = 2 # STRICT LIMIT (Jan 2026 Standardization)
        current_attempt = 0
        
        BANNED_PHRASES = [
            "[Your Name]", "[Your Title]", "Pacific AI Systems", 
            "I hope this message finds you well", "innovative solution", 
            "align perfectly with your goals", "Scout AI", "Scout", "Gemini", "LLM", "automation",
            "open to a brief call", "Would you be open to", "(insert", "[insert", "Best regards,"
        ]
        
        import time

        while current_attempt <= max_retries:
            try:
                # Generate with Instrumentation
                start_ts = time.time()
                
                if current_attempt > 0:
                    # Retry Logic: Explicitly command NOT to use the banned phrase found
                    violation_msg = f"CRITICAL: The previous output contained banned phrase '{violation}'. REWRITE ENTIRELY WITHOUT IT. Do not mention internal tools. No greetings fluff."
                    retry_text = f"{user_content}\n\n{violation_msg}"
                    logger.warning(f"[Trace {request_id}] Retrying (Attempt {current_attempt})... Reason: {violation}")
                    response = self.model.generate_content(retry_text)
                else:
                    response = self.model.generate_content(user_content)
                
                latency_ms = (time.time() - start_ts) * 1000
                
                # Extract Token Usage (Best Effort)
                usage = getattr(response, "usage_metadata", "N/A")
                logger.info(f"[Trace {request_id}] LLM Call Finished. Latency={latency_ms:.2f}ms Tokens={usage}")

                raw_text = response.text.strip()
                
                # Check Compliance (Raw Text)
                violation = None
                for phrase in BANNED_PHRASES:
                    if phrase.lower() in raw_text.lower():
                        violation = phrase
                        break
                
                if violation:
                    logger.warning(f"[Trace {request_id}] Violation in RAW: '{violation}'")
                    current_attempt += 1
                    continue # Loop
                
                # Parse
                if raw_text.startswith("```json"):
                    raw_text = raw_text[7:]
                if raw_text.endswith("```"):
                    raw_text = raw_text[:-3]
                    
                data = json.loads(raw_text)
                
                clean_body = data.get("body", "").rstrip()
                subject = data.get("subject", "Introduction")
                
                # SIG STRIP LOGIC (Safety Net)
                # Remove common closings if the model hallucinated them despite instructions
                closings = ["Best,", "Best regards,", "Sincerely,", "Cheers,", "Thanks,"]
                for closing in closings:
                    if clean_body.strip().endswith(closing):
                        clean_body = clean_body.strip()[:-len(closing)].strip()
                
                # Double Check Parsed Body for phrases (safety net)
                for phrase in BANNED_PHRASES:
                    if phrase.lower() in clean_body.lower():
                         logger.warning(f"[Trace {request_id}] Violation in BODY: '{phrase}'")
                         violation = phrase
                         break
                
                if violation:
                    current_attempt += 1
                    continue

                # Success
                logger.info(f"[Trace {request_id}] Success. Appending Signature.")
                sig_block = OFFICIAL_SIGNATURE.strip()
                full_email = f"{clean_body}\n\n{sig_block}"
                
                # Add trace to metadata
                meta = data.get("metadata", {})
                meta["prompt_trace"] = trace
                # Sanitize tokens for HTTP Headers (No Newlines)
                tokens_str = "N/A"
                try:
                    if hasattr(usage, "total_token_count"):
                        tokens_str = str(usage.total_token_count)
                    else:
                        tokens_str = str(usage).replace("\n", " ").replace("\r", " ").strip()
                except:
                    pass

                meta["proof"] = {
                    "latency_ms": int(latency_ms),
                    "model": settings.GEMINI_MODEL_NAME,
                    "tokens": tokens_str
                }
                
                return DraftOutput(
                    subject=subject,
                    body_clean=clean_body,
                    signature_block=sig_block,
                    body_with_signature=full_email,
                    metadata=meta,
                    status="success"
                )
                
            except Exception as e:
                logger.error(f"[Trace {request_id}] Attempt {current_attempt} Failed: {e}")
                current_attempt += 1
        
        # FINAL FAILURE
        logger.error(f"[Trace {request_id}] GUARDRAIL FAILURE: Exceeded max retries.")
        raise ValueError("guardrail_failed")
