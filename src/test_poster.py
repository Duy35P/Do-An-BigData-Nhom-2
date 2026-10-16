import urllib.request
import urllib.parse
import re
import json

title = "Joy Ride 3"
query = f"{title} movie poster"
headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Referer": "https://duckduckgo.com/"
}

# B1: Lấy vqd
req1 = urllib.request.Request(f"https://duckduckgo.com/?q={urllib.parse.quote(query)}", headers=headers)
html = urllib.request.urlopen(req1, timeout=5).read().decode("utf-8")
m = re.search(r'vqd=([0-9\-]+)', html)
if not m:
    m = re.search(r'vqd="([^"]+)"', html)

if m:
    vqd = m.group(1)
    # B2: Lấy ảnh
    req2 = urllib.request.Request(f"https://duckduckgo.com/i.js?q={urllib.parse.quote(query)}&vqd={vqd}", headers=headers)
    data = json.loads(urllib.request.urlopen(req2, timeout=5).read().decode("utf-8"))
    res = data.get("results", [])
    if res:
        print("FOUND POSTER:", res[0].get("image"))
    else:
        print("No image in results")
else:
    print("Could not find vqd")
