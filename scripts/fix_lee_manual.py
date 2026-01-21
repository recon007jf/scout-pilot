
from app.main import get_service_db
db = get_service_db()
img_url = 'https://media.licdn.com/dms/image/v2/D5603AQHIy04MJ6x85A/profile-displayphoto-shrink_400_400/profile-displayphoto-shrink_400_400/0/1724445191828?e=1770249600&v=beta&t=ZoygF0DQhycy185jC6vyEX7rvQqO6XtPZpEV1-1zUbc'
db.table('target_brokers').update({'profile_image': img_url}).eq('full_name', 'Lee Sommars').execute()
print('✅ Lee Sommars image updated manually.')

