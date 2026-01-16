from supabase import Client
from datetime import datetime, timedelta

def check_rolling_velocity(db: Client) -> int:
    """
    Enforces the rolling limit.
    Tries to use RPC for strictness, falls back to REST query.
    """
    try:
        # Try RPC (defined in migration 008)
        res = db.rpc("check_rolling_velocity").execute()
        # RPC returns scalar? Or list? typically scalar in data
        if res.data is not None:
             return int(res.data)
    except Exception:
        # Fallback to REST (Client Side calculation of server data)
        # sent_at > NOW() - 24 HOURS
        # Supabase filter: gt.sent_at.now()-24h hard to express in simple string without formatting
        # Easier to calc timestamp in python
        cutoff = (datetime.utcnow() - timedelta(hours=24)).isoformat()
        res = db.table("candidates").select("id", count="exact", head=True).gt("sent_at", cutoff).execute()
        return res.count

    return 0
