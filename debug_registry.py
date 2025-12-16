
import duckdb
import re

PATH = "backend/Scout_Data_Artifacts/Western_Broker_Map_2023.parquet"
SUFFIX_REGEX = re.compile(r'\b(INC|LLC|LTD|CORP|CORPORATION|CO|COMPANY)\b', re.IGNORECASE)

def normalize_text(text):
    if not isinstance(text, str): return ""
    text = text.upper().strip()
    text = re.sub(r'[^\w\s]', '', text)
    text = SUFFIX_REGEX.sub('', text)
    text = re.sub(r'\s+', ' ', text).strip()
    return text

con = duckdb.connect()
con.execute(f"CREATE VIEW v AS SELECT * FROM '{PATH}'")
firms = [r[0] for r in con.execute("SELECT DISTINCT TARGET_BROKER_FIRM FROM v WHERE TARGET_BROKER_FIRM IS NOT NULL").fetchall()]
norm_firms = sorted([normalize_text(f) for f in firms])

print("REGISTRY FIRM SAMLE (First 50):")
for f in norm_firms[:50]:
    print(f)
    
print("\nCHECKING TARGETS:")
targets = ["HUB", "ALLIANT", "SEQUOIA"]
for t in targets:
    t_norm = normalize_text(t)
    matches = [f for f in norm_firms if t_norm in f]
    print(f"Target '{t}' (Norm: {t_norm}) matches in Registry: {matches}")
