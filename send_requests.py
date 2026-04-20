import requests
from concurrent.futures import ThreadPoolExecutor

devices = [f"PLAT1-DEV-{i:04d}-000" for i in range(300)]

def hit(device):
    url = f"http://localhost:8001/pcid/PLATCUST0005/acid/APPCUST0001/id/{device}/export"
    try:
        res = requests.post(url, timeout=10)
        return f"{device} -> {res.status_code}"
    except Exception as e:
        return f"{device} -> ERROR {e}"

with ThreadPoolExecutor(max_workers=100) as executor:
    results = list(executor.map(hit, devices))

for r in results:
    print(r)