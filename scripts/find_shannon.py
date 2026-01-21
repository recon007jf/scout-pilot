
from app.main import get_service_db
db = get_service_db()
res = db.table('target_brokers').select('*').ilike('full_name', '%Shannon%').execute()
for r in res.data:
    print(f"{r['full_name']} | {r['firm']} | Img: {r['profile_image']}")

