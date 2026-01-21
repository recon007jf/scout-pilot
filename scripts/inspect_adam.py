
from app.main import get_service_db
db = get_service_db()
res = db.table('target_brokers').select('*').eq('full_name', 'Adam McDonough').execute()
print(res.data)

