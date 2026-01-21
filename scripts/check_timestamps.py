
from app.main import get_service_db
db = get_service_db()
# Check duplicates in created_at
res = db.table('target_brokers').select('created_at, full_name, status').order('created_at').limit(10).execute()
for r in res.data:
    print(f"{r['created_at']} - {r['full_name']} ({r['status']})")

