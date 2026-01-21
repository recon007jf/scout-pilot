
import requests

URL = "https://media.licdn.com/dms/image/v2/C4D03AQFNzaG_LfSXAg/profile-displayphoto-shrink_200_200/profile-displayphoto-shrink_200_200/0/1519947168020"

def check():
    print(f"Checking URL: {URL}")
    
    # 1. Browser UA
    headers = {
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    }
    try:
        r = requests.get(URL, headers=headers, timeout=5)
        print(f"Browser UA Status: {r.status_code}")
        print(f"Content-Length: {len(r.content)}")
    except Exception as e:
        print(f"Error: {e}")

    # 2. Add Referer (Simulate App)
    headers["Referer"] = "http://localhost:3000/"
    try:
        r = requests.get(URL, headers=headers, timeout=5)
        print(f"With Referer Status: {r.status_code}")
    except:
        pass

if __name__ == "__main__":
    check()
