
from app.main import get_service_db
db = get_service_db()
bad_names = ['Sarah Chen', 'Michael Ross', 'Jessica Alpert', 'David Kim', 'Emily Watson']
print(f'Deleting {len(bad_names)} bad mock candidates...')
db.table('target_brokers').delete().in_('full_name', bad_names).execute()
print('Cleanup complete.')

