import urllib.request, csv

with open("/home/dell/extra_movie_data/movies_metadata.csv", errors="ignore") as f:
    r = csv.DictReader(f)
    tested, ok = 0, 0
    for row in r:
        p = row.get("poster_path")
        vc = float(row.get("vote_count") or 0)
        if p and vc > 1500:
            tested += 1
            url = f"https://image.tmdb.org/t/p/w200{p}"
            try:
                req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
                with urllib.request.urlopen(req, timeout=3) as resp:
                    if resp.status == 200:
                        ok += 1
                        print("OK:", row.get("title"), "->", url)
            except Exception as e:
                print("FAIL:", row.get("title"), "->", e)
            if tested >= 10:
                break
print(f"Result: {ok}/{tested} working")
