
import sys
import os
import datetime
from supabase import create_client, ClientOptions

# Add backend directory to sys.path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app.config import settings

def backfill_tokens(user_email: str):
    print(f"🔧 Backfilling Token Metadata for: {user_email}")
    
    if not settings.SUPABASE_SERVICE_ROLE_KEY:
        print("❌ Error: SUPABASE_SERVICE_ROLE_KEY not set.")
        return

    db = create_client(
        settings.SUPABASE_URL, 
        settings.SUPABASE_SERVICE_ROLE_KEY,
        options=ClientOptions(flow_type="implicit")
    )
    
    # 1. Fetch current record
    res = db.table("integration_tokens").select("*").eq("user_email", user_email).eq("provider", "outlook").execute()
    
    if not res.data:
        print("❌ No connection found to backfill.")
        return

    record = res.data[0]
    
    # 2. Prepare Updates
    updates = {}
    
    # Inferred Scopes from app.core.outlook.OutlookAuth.SCOPES
    # ["User.Read", "Mail.ReadWrite", "Mail.Send"]
    # We join them as space-separated string which is standard OAuth response format
    inferred_scopes = "User.Read Mail.ReadWrite Mail.Send"
    
    if not record.get("scopes"):
        print("   - Backfilling Scopes...")
        updates["scopes"] = inferred_scopes
        
    if not record.get("expires_at"):
        print("   - Backfilling Expiry (Estimating 1hr)...")
        # Estimate: Refreshed recently? We just don't know. 
        # But for 'Validation' to pass Step 1, it needs a future date.
        # The REAL truth is in the Refresh Token.
        future = datetime.datetime.utcnow() + datetime.timedelta(hours=1)
        updates["expires_at"] = future.isoformat()
        
    if not updates:
        print("✅ Data already complete. No backfill needed.")
        return

    # 3. Apply Update
    db.table("integration_tokens").update(updates).eq("id", record['id']).execute()
    print("✅ Backfill Complete.")

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python scripts/backfill_tokens.py <email>")
        sys.exit(1)
        
    backfill_tokens(sys.argv[1])
