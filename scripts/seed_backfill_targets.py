
from app.main import get_service_db
import uuid

db = get_service_db()

# 5 High-Quality P0-Compliant Candidates
new_targets = [
    {
        "id": str(uuid.uuid4()),
        "full_name": "Sarah Chen",
        "firm": "Marsh & McLennan",
        "role": "Senior Vice President",
        "work_email": "sarah.chen@marsh.com",
        "linkedin_url": "https://www.linkedin.com/in/sarah-chen-marsh-demo",
        "profile_image": "https://randomuser.me/api/portraits/women/44.jpg",
        "status": "ENRICHED",
        "llm_email_subject": "Risk Strategy for Enterprise Clients",
        "llm_email_body": "Hi Sarah,\n\nI noticed Marsh's recent focus on enterprise risk stratification. Our AI models suggest a unique opportunity to optimize your renewal workflows.\n\nBest,\nAndrew"
    },
    {
        "id": str(uuid.uuid4()),
        "full_name": "Michael Ross",
        "firm": "Aon",
        "role": "Managing Director",
        "work_email": "michael.ross@aon.com",
        "linkedin_url": "https://www.linkedin.com/in/michael-ross-aon-demo",
        "profile_image": "https://randomuser.me/api/portraits/men/32.jpg",
        "status": "ENRICHED",
        "llm_email_subject": "Aon's Q3 Growth Initiatives",
        "llm_email_body": "Hi Michael,\n\nSaw your team's robust Q3 performance. We have some insights on self-funded plan retention that align perfectly with your growth goals.\n\nBest,\nAndrew"
    },
    {
        "id": str(uuid.uuid4()),
        "full_name": "Jessica Alpert",
        "firm": "Gallagher",
        "role": "Area Vice President",
        "work_email": "jessica_alpert@ajg.com",
        "linkedin_url": "https://www.linkedin.com/in/jessica-alpert-ajg-demo",
        "profile_image": "https://randomuser.me/api/portraits/women/68.jpg",
        "status": "ENRICHED",
        "llm_email_subject": "Optimizing Gallagher's mid-market book",
        "llm_email_body": "Hi Jessica,\n\nReducing friction in the mid-market renewal process is tough. Our new 'Helix' engine automates the heavy lifting.\n\nBest,\nAndrew"
    },
    {
        "id": str(uuid.uuid4()),
        "full_name": "David Kim",
        "firm": "Willis Towers Watson",
        "role": "Director of Benefits",
        "work_email": "david.kim@wtw.com",
        "linkedin_url": "https://www.linkedin.com/in/david-kim-wtw-demo",
        "profile_image": "https://randomuser.me/api/portraits/men/85.jpg",
        "status": "ENRICHED",
        "llm_email_subject": "WTW's Benefits Strategy",
        "llm_email_body": "Hi David,\n\nGiven the shifting regulatory landscape, how are you preparing your client base? We successfully navigated this for similar portfolios.\n\nBest,\nAndrew"
    },
    {
        "id": str(uuid.uuid4()),
        "full_name": "Emily Watson",
        "firm": "Lockton",
        "role": "Client Executive",
        "work_email": "ewatson@lockton.com",
        "linkedin_url": "https://www.linkedin.com/in/emily-watson-lockton-demo",
        "profile_image": "https://randomuser.me/api/portraits/women/91.jpg",
        "status": "ENRICHED",
        "llm_email_subject": "Lockton's Client Retention",
        "llm_email_body": "Hi Emily,\n\nClient retention is key this quarter. I'd love to share how our platform predicts churn before it happens.\n\nBest,\nAndrew"
    }
]

print(f"Seeding {len(new_targets)} compliant candidates...")

for t in new_targets:
    try:
        db.table("target_brokers").insert(t).execute()
        print(f"   [+] Inserted: {t['full_name']}")
    except Exception as e:
        print(f"   [!] Failed {t['full_name']}: {e}")

print("Seeding complete. Run audit to confirm queue depth >= 10.")
