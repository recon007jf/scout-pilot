import requests

candidates = [
    "https://askebsa.dol.gov/FOIA%20Files/2023/Data/F_5500_2023.zip",
    "https://askebsa.dol.gov/FOIA%20Files/2023/Data/f_5500_2023.zip",
    "https://askebsa.dol.gov/FOIA%20Files/2023/Latest/F_5500_2023_Latest.zip",
    "https://askebsa.dol.gov/FOIA%20Files/2023/Data/Form_5500_2023.zip",
    "https://askebsa.dol.gov/FOIA%20Files/2023/Data/form_5500_2023.zip",
    "https://askebsa.dol.gov/FOIA%20Files/2023/Data/F_5500_2023_Data.zip"
]

print("Probing 2023 Base URLs...")
for url in candidates:
    try:
        r = requests.head(url, timeout=5)
        print(f"[{r.status_code}] {url}")
    except Exception as e:
        print(f"[ERR] {url}: {e}")
