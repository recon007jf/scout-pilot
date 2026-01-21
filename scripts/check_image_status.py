
from app.main import get_service_db
db = get_service_db()
# Count total
total = db.table('target_brokers').select('id', count='exact').execute().count
# Count missing images
missing = db.table('target_brokers').select('id', count='exact').is_('profile_image', 'null').execute().count
print(f'Total Candidates: {total}')
print(f'Missing Images: {missing}')

