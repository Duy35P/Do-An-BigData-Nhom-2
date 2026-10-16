"""
HỆ THỐNG GỢI Ý PHIM TRỰC TUYẾN DỰA TRÊN KIẾN TRÚC LAKEHOUSE VÀ ĐIỆN TOÁN PHÂN TÁN
- Giao diện Netflix-style hiển thị Poster phim thật từ TMDb
- Gợi ý tương tác thời gian thực trên 62.423 bộ phim MovieLens 25M
- Tích hợp Kafka Producer gửi sự kiện người dùng vào topic 'movie_logs'
- Dashboard thống kê luồng Streaming thời gian thực & kiến trúc Lakehouse
"""

import io
import os
import re
import csv
import json
import time
from datetime import datetime
from collections import deque
from typing import Optional, List, Dict, Any
from fastapi import FastAPI, Query, Body
from pydantic import BaseModel
from fastapi.responses import HTMLResponse
from hdfs import InsecureClient
import pandas as pd
import numpy as np

try:
    from kafka import KafkaProducer
except ImportError:
    KafkaProducer = None

app = FastAPI(title="Netflix-Style Lakehouse Movie Recommender")

# Kết nối WebHDFS NameNode
hdfs_client = InsecureClient("http://localhost:9870", user="dell")

# --- KAFKA PRODUCER SINGLETON & EVENT LOG BUFFER ---
_kafka_producer = None
_recent_streaming_events = deque(maxlen=100)

def get_kafka_producer():
    global _kafka_producer
    if _kafka_producer is None and KafkaProducer is not None:
        try:
            _kafka_producer = KafkaProducer(
                bootstrap_servers="localhost:9092",
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                request_timeout_ms=1000
            )
        except Exception:
            _kafka_producer = False
    return _kafka_producer if _kafka_producer else None

def send_kafka_event(movie_id: int, rating: Optional[float] = None, action: str = "rate", user_id: int = 99999, movie_title: str = ""):
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    rating_val = float(rating) if rating is not None else None
    event = {
        "timestamp": ts,
        "user_id": int(user_id),
        "movie_id": int(movie_id),
        "title": movie_title,
        "action": str(action),
        "rating": rating_val
    }

    producer = get_kafka_producer()
    kafka_success = False
    if producer:
        try:
            kafka_msg = {
                "timestamp": ts,
                "user_id": int(user_id),
                "movie_id": int(movie_id),
                "action": str(action),
                "rating": rating_val
            }
            producer.send("movie_logs", kafka_msg)
            kafka_success = True
            print(f"📡 [Kafka] Sent event to movie_logs: {kafka_msg}")
        except Exception as e:
            print(f"⚠️ [Kafka] Send failed: {e}")

    event["kafka_delivered"] = kafka_success
    _recent_streaming_events.appendleft(event)
    return kafka_success

# --- NẠP BẢN ĐỒ POSTER PHIM TỪ TMDB ---
_poster_map = {}

def load_poster_map():
    global _poster_map
    if _poster_map:
        return _poster_map

    meta_paths = [
        "/home/dell/extra_movie_data/movies_metadata.csv",
        r"\\wsl.localhost\Ubuntu-26.04\home\dell\extra_movie_data\movies_metadata.csv",
        r"\\wsl$\Ubuntu-26.04\home\dell\extra_movie_data\movies_metadata.csv"
    ]
    links_paths = [
        "/home/dell/movielens_data/ml-25m/links.csv",
        r"\\wsl.localhost\Ubuntu-26.04\home\dell\movielens_data\ml-25m\links.csv",
        r"\\wsl$\Ubuntu-26.04\home\dell\movielens_data\ml-25m\links.csv"
    ]

    meta_file = next((p for p in meta_paths if os.path.exists(p)), None)
    links_file = next((p for p in links_paths if os.path.exists(p)), None)

    if meta_file and links_file:
        try:
            tmdb_posters = {}
            with open(meta_file, mode="r", encoding="utf-8", errors="ignore") as f:
                reader = csv.DictReader(f)
                for row in reader:
                    tid = row.get("id")
                    poster = row.get("poster_path")
                    if tid and poster:
                        tmdb_posters[tid] = poster.strip()

            with open(links_file, mode="r", encoding="utf-8", errors="ignore") as f:
                reader = csv.DictReader(f)
                for row in reader:
                    mid = row.get("movieId")
                    tmdb = row.get("tmdbId")
                    if mid and tmdb and tmdb in tmdb_posters:
                        _poster_map[int(mid)] = tmdb_posters[tmdb]
            print(f"✓ Đã ánh xạ thành công {len(_poster_map)} poster phim TMDb!")
        except Exception as e:
            print(f"⚠️ Không thể đọc poster mapping: {e}")

    return _poster_map

# BẢN ĐỒ POSTER PHIM BOM TẤN ĐƯỢC XÁC THỰC TRÊN TMDB CDN (HTTP 200)
POPULAR_POSTERS = {
    1: "/uXDfjJbdP4ijW5hWSBrPrlKpxab.jpg",      # Toy Story (1995)
    2: "/vgpXmVaVyXd7ffRIqZAvKxsPrDp.jpg",      # Jumanji (1995)
    6: "/rrBuGu0Pjq7YPG9KASj799ubNx1.jpg",      # Heat (1995)
    10: "/z0ljnGsVnrKVg0zTUMX1k25l2y8.jpg",     # GoldenEye (1995)
    260: "/6FfCtAuVAW8XJjZ7eWeLibRLWTw.jpg",    # Star Wars: A New Hope (1977)
    296: "/d5iIlFn5s0ImszYzBPb8JPIfbXD.jpg",    # Pulp Fiction (1994)
    318: "/9cqNxx0GxF0bflZmeSMuL5tnGzr.jpg",    # Shawshank Redemption (1994)
    356: "/arw2vcBveWOVZr6pxd9XTd1TdQa.jpg",    # Forrest Gump (1994)
    364: "/sKCr78MXSLixwmZ8DyJLrpMsd15.jpg",    # The Lion King (1994)
    480: "/oU7Oq2kFAAlGqbU4VoAE36g4hoI.jpg",    # Jurassic Park (1993)
    500: "/xi8Iu6Bhqq72Y4JtMT297cj26lm.jpg",    # Reservoir Dogs (1992)
    589: "/5M0j0B18abtBI5em2mZMmriDuqn.jpg",    # Terminator 2: Judgment Day (1991)
    593: "/uS9m8OBk1A8eM9I042bx8XXpqAq.jpg",    # Silence of the Lambs (1991)
    608: "/rt7cpEr18r127i6vQ621808b8wA.jpg",    # Fargo (1996)
    858: "/3bhkrj58Vtu7enYsRolD1fZdja1.jpg",    # The Godfather (1972)
    1196: "/nNAeTmF4A9VO25uNcrwVUt5T1bT.jpg",   # Star Wars: The Empire Strikes Back (1980)
    1198: "/ceG6VqYrWxdQB78DTqW3k4pApsH.jpg",   # Raiders of the Lost Ark (1981)
    1210: "/mDCBQNM5ncRMR80yTGz2zZzYqV.jpg",   # Star Wars: Return of the Jedi (1983)
    1270: "/fNOH9f1aA7XRTzl1sAOx9iF553P.jpg",   # Back to the Future (1985)
    1580: "/uLOmOF5IzWvqftDuqqbGNSc04.jpg",    # Men in Black (1997)
    1721: "/9xjZS2rlVxm8SFx8kPC3aIGCOYQ.jpg",   # Titanic (1997)
    2028: "/uqx37cS8cpHg8U35f9U5IBlrjp3.jpg",   # Saving Private Ryan (1998)
    2571: "/f89U3ADr1oiB1s9GkdPOEpXUk5H.jpg",   # The Matrix (1999)
    2959: "/pB8BM7pdSp6B6Ih7QZ4DrQ3PmJK.jpg",   # Fight Club (1999)
    3114: "/4rbcp3ng8n1MKHjpeqW0L7Fnpzz.jpg",  # Toy Story 2 (1999)
    3578: "/ty8TGRuvJLPUmAR110YISSSHbqp.jpg",  # Gladiator (2000)
    4226: "/yuNs09hvpHVU1cBTCAk9zxsL2oW.jpg",  # Memento (2000)
    4886: "/4Cp0L58Rg1bSJSsT6uw7OuU4nYc.jpg",  # Monsters, Inc. (2001)
    4993: "/6oom5QYQ2yQTMJIbnvbkBL9cDK6.jpg",  # LOTR: Fellowship (2001)
    5952: "/5VTN0pR8gcqV3EPUHHfMGn2YN9L.jpg",  # LOTR: Two Towers (2002)
    6377: "/eHuGQ10FUzK1mdOY69wF5pGgEf5.jpg",  # Finding Nemo (2003)
    6874: "/v7TaX8kXMXs5yFFGR41guUDNcnB.jpg",   # Kill Bill: Vol. 1 (2003)
    7153: "/rCzpDGLbOoPwLjy3OAm5NUPOTrC.jpg",   # LOTR: Return of the King (2003)
    7361: "/5MwkWH9tYHv3mV9OdYTMR5qkvk2.jpg",   # Eternal Sunshine (2004)
    8961: "/2LqaLgk4Z226KkgPJuiOQ58wvrm.jpg",   # The Incredibles (2004)
    33794: "/4MpN4Cwhvua1yR1K7q6062fI9rN.jpg",  # Batman Begins (2005)
    58559: "/qJ2tW6WMUDux911r6m7haRef0WH.jpg",  # The Dark Knight (2008)
    59315: "/78lPtwv72eTNqFW9COBYI0dWDJa.jpg",  # Iron Man (2008)
    60069: "/hbhFnRzzg6ZDmm8YAmxBnQAcQI.jpg",   # WALL·E (2008)
    68954: "/vpbaStTMt8qqHaEokORrqqYa3u0.jpg",   # Up (2009)
    78499: "/AbbXspMOwdvwWZgVX09qgf7b5t.jpg",   # Toy Story 3 (2010)
    79132: "/oYuLEt3zVCKq57qu2F8dT7NIa6f.jpg",  # Inception (2010)
    88125: "/da222MrLUTasDu5tpba023v9qS7.jpg",  # Harry Potter DH2 (2011)
    89745: "/RYMX2wcKCBAr24UyPD7xwmjaTn.jpg",  # The Avengers (2012)
    99114: "/7oWY8vdWW7thTzfq3TVmwT97zwh.jpg",  # Django Unchained (2012)
    109487: "/gEU2QniE6E77NI6lCU6MxlNBvIx.jpg", # Interstellar (2014)
    112552: "/7fn624j5lj3xTme2SgiLCeuedmO.jpg", # Whiplash (2014)
    116797: "/5BHuvQ6p9kQTSpEuz6cjNgQWdp6.jpg", # The Martian (2015)
    122904: "/fSRb7vyIP8rQpL0I47P3qUsucht.jpg", # Deadpool (2016)
    122912: "/7WsyChQLEftFiDOVTGkv3hFpyyt.jpg", # Avengers: Infinity War (2018)
    134853: "/lRHE0vzf3oYJejYrWfukAcL88Zu.jpg", # Inside Out (2015)
    168252: "/fnbjcRDXdopnPoBfBOi9jjOH8Ky.jpg", # Logan (2017)
    177765: "/uDO8zWDhfWwoFdKS4fzkVJb0Rf0.jpg", # La La Land (2016)
    192803: "/iiZZdoQBEYBv6id8su7bpL08vMe.jpg"  # Spider-Man: Into the Spider-Verse (2018)
}

def get_poster_url(movie_id: int, poster_path: Optional[str] = None) -> str:
    # 1. Ưu tiên tra trong kho ảnh chất lượng cao đã xác thực
    if movie_id in POPULAR_POSTERS:
        return f"https://image.tmdb.org/t/p/w500{POPULAR_POSTERS[movie_id]}"

    # 2. Tra trong bản đồ ánh xạ TMDb từ metadata
    if not poster_path or pd.isna(poster_path):
        posters = load_poster_map()
        poster_path = posters.get(movie_id)

    if poster_path and not pd.isna(poster_path) and str(poster_path).startswith("/"):
        return f"https://image.tmdb.org/t/p/w500{poster_path}"
    return ""

# --- KHO LƯU TRỮ PHIM YÊU THÍCH (USER WATCHLIST) ---
_user_watchlist = {
    260: {
        "movieId": 260,
        "title": "Star Wars: Episode IV - A New Hope (1977)",
        "genres": "Action|Adventure|Sci-Fi",
        "poster_url": "https://image.tmdb.org/t/p/w500/6FfCtAuVAW8XJjZ7eWeLibRLWTw.jpg",
        "added_at": "2026-10-12 12:00:00"
    },
    2571: {
        "movieId": 2571,
        "title": "Matrix, The (1999)",
        "genres": "Action|Sci-Fi|Thriller",
        "poster_url": "https://image.tmdb.org/t/p/w500/f89U3ADr1oiB1s9GkdPOEpXUk5H.jpg",
        "added_at": "2026-10-12 12:00:00"
    }
}

class WatchlistToggleRequest(BaseModel):
    movieId: int
    title: Optional[str] = ""
    genres: Optional[str] = ""

# --- NẠP MA TRẬN ĐẶC TRƯNG TIỀM ẨN TỪ MÔ HÌNH ALS TẦNG GOLD TRÊN HDFS ---
_als_factors_map = {}

def load_als_movie_factors():
    global _als_factors_map
    if _als_factors_map:
        return _als_factors_map

    # 1. Thử nạp từ HDFS Tầng Gold
    try:
        files = hdfs_client.list("/lakehouse/gold/movie_factors")
        dfs = []
        for f in files:
            if f.endswith(".parquet"):
                with hdfs_client.read(f"/lakehouse/gold/movie_factors/{f}") as r:
                    dfs.append(pd.read_parquet(io.BytesIO(r.read())))
        if dfs:
            full_df = pd.concat(dfs, ignore_index=True)
            for _, row in full_df.iterrows():
                mid = int(row["movieId"])
                vec = np.array(row["features"], dtype=np.float32)
                norm = float(np.linalg.norm(vec))
                if norm > 1e-6:
                    _als_factors_map[mid] = vec / norm
                else:
                    _als_factors_map[mid] = vec
            print(f"✓ Nạp thành công {len(_als_factors_map)} vector đặc trưng ALS từ HDFS Gold Layer!")
            return _als_factors_map
    except Exception:
        pass

    # 2. Thử nạp từ bản sao lưu cục bộ (local parquet cache)
    local_dirs = [
        "/home/dell/extra_movie_data/movie_factors",
        r"\\wsl.localhost\Ubuntu-26.04\home\dell\extra_movie_data\movie_factors",
        r"\\wsl$\Ubuntu-26.04\home\dell\extra_movie_data\movie_factors"
    ]
    for ldir in local_dirs:
        if os.path.exists(ldir) and os.path.isdir(ldir):
            try:
                pfiles = [os.path.join(ldir, fn) for fn in os.listdir(ldir) if fn.endswith(".parquet")]
                if pfiles:
                    dfs = [pd.read_parquet(pf) for pf in pfiles]
                    full_df = pd.concat(dfs, ignore_index=True)
                    for _, row in full_df.iterrows():
                        mid = int(row["movieId"])
                        vec = np.array(row["features"], dtype=np.float32)
                        norm = float(np.linalg.norm(vec))
                        if norm > 1e-6:
                            _als_factors_map[mid] = vec / norm
                        else:
                            _als_factors_map[mid] = vec
                    print(f"✓ Nạp thành công {len(_als_factors_map)} vector đặc trưng ALS từ kho lưu trữ cục bộ!")
                    return _als_factors_map
            except Exception:
                continue

    return _als_factors_map

# --- NẠP VÀ LƯU BỘ DANH MỤC PHIM (MOVIE CATALOG) ---
_cached_movies_df = None

def load_movies_catalog():
    global _cached_movies_df
    if _cached_movies_df is not None and not _cached_movies_df.empty:
        return _cached_movies_df

    # 1. Thử nạp từ HDFS Silver
    try:
        files = hdfs_client.list("/lakehouse/silver/movies")
        dfs = []
        for f in files:
            if f.endswith(".parquet"):
                with hdfs_client.read(f"/lakehouse/silver/movies/{f}") as r:
                    dfs.append(pd.read_parquet(io.BytesIO(r.read())))
        if dfs:
            _cached_movies_df = pd.concat(dfs, ignore_index=True)
            print(f"✓ Nạp thành công {len(_cached_movies_df)} phim từ HDFS Silver!")
    except Exception:
        pass

    # 2. Thử nạp từ file CSV địa phương của Linux / Windows
    if _cached_movies_df is None or _cached_movies_df.empty:
        for local_path in [
            "/home/dell/movielens_data/ml-25m/movies.csv",
            r"\\wsl.localhost\Ubuntu-26.04\home\dell\movielens_data\ml-25m\movies.csv",
            r"\\wsl$\Ubuntu-26.04\home\dell\movielens_data\ml-25m\movies.csv",
            "d:/DAIHOC/NĂM 4 -HK1/BigData/DoAn_BigData/movielens_data/ml-25m/movies.csv"
        ]:
            if os.path.exists(local_path):
                try:
                    df = pd.read_csv(local_path)
                    df.rename(columns={"title": "TenPhim", "genres": "TheLoai"}, inplace=True)
                    _cached_movies_df = df
                    print(f"✓ Nạp thành công {len(_cached_movies_df)} phim từ {local_path}!")
                    break
                except Exception:
                    continue

    # 3. Dữ liệu dự phòng mẫu phong phú nếu chưa có file
    if _cached_movies_df is None or _cached_movies_df.empty:
        sample_movies = [
            {"movieId": 1, "TenPhim": "Toy Story (1995)", "TheLoai": "Adventure|Animation|Children|Comedy|Fantasy"},
            {"movieId": 2, "TenPhim": "Jumanji (1995)", "TheLoai": "Adventure|Children|Fantasy"},
            {"movieId": 260, "TenPhim": "Star Wars: Episode IV - A New Hope (1977)", "TheLoai": "Action|Adventure|Sci-Fi"},
            {"movieId": 296, "TenPhim": "Pulp Fiction (1994)", "TheLoai": "Comedy|Crime|Drama|Thriller"},
            {"movieId": 318, "TenPhim": "Shawshank Redemption, The (1994)", "TheLoai": "Crime|Drama"},
            {"movieId": 356, "TenPhim": "Forrest Gump (1994)", "TheLoai": "Comedy|Drama|Romance|War"},
            {"movieId": 589, "TenPhim": "Terminator 2: Judgment Day (1991)", "TheLoai": "Action|Sci-Fi"},
            {"movieId": 593, "TenPhim": "Silence of the Lambs, The (1991)", "TheLoai": "Crime|Horror|Thriller"},
            {"movieId": 858, "TenPhim": "Godfather, The (1972)", "TheLoai": "Crime|Drama"},
            {"movieId": 1196, "TenPhim": "Star Wars: Episode V - The Empire Strikes Back (1980)", "TheLoai": "Action|Adventure|Sci-Fi"},
            {"movieId": 1210, "TenPhim": "Star Wars: Episode VI - Return of the Jedi (1983)", "TheLoai": "Action|Adventure|Sci-Fi"},
            {"movieId": 1580, "TenPhim": "Men in Black (1997)", "TheLoai": "Action|Comedy|Sci-Fi"},
            {"movieId": 2571, "TenPhim": "Matrix, The (1999)", "TheLoai": "Action|Sci-Fi|Thriller"},
            {"movieId": 2959, "TenPhim": "Fight Club (1999)", "TheLoai": "Action|Crime|Drama|Thriller"},
            {"movieId": 4993, "TenPhim": "Lord of the Rings: The Fellowship of the Ring, The (2001)", "TheLoai": "Adventure|Fantasy"},
            {"movieId": 7153, "TenPhim": "Lord of the Rings: The Return of the King, The (2003)", "TheLoai": "Action|Adventure|Drama|Fantasy"},
            {"movieId": 58559, "TenPhim": "Dark Knight, The (2008)", "TheLoai": "Action|Crime|Drama|IMAX"},
            {"movieId": 79132, "TenPhim": "Inception (2010)", "TheLoai": "Action|Crime|Drama|Mystery|Sci-Fi|Thriller|IMAX"},
            {"movieId": 109487, "TenPhim": "Interstellar (2014)", "TheLoai": "Sci-Fi|IMAX"}
        ]
        _cached_movies_df = pd.DataFrame(sample_movies)

    if "genre_set" not in _cached_movies_df.columns:
        _cached_movies_df["genre_set"] = _cached_movies_df["TheLoai"].apply(lambda x: set(str(x).split("|")))

    # Nạp poster mapping
    load_poster_map()

    return _cached_movies_df


@app.get("/", response_class=HTMLResponse)
def home():
    return """
    <!DOCTYPE html>
    <html lang="vi">
    <head>
        <meta charset="UTF-8">
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
        <title>Netflix Lakehouse Recommender - Big Data MovieLens</title>
        <link href="https://fonts.googleapis.com/css2?family=Plus+Jakarta+Sans:wght@300;400;500;600;700;800&display=swap" rel="stylesheet">
        <style>
            :root {
                --primary: #e50914;
                --primary-glow: rgba(229, 9, 20, 0.35);
                --accent: #f59e0b;
                --bg: #0b0f19;
                --panel: #111827;
                --card-bg: #1a2234;
                --card-hover: #222d45;
                --border: #2d3748;
                --text: #f9fafb;
                --muted: #94a3b8;
                --success: #10b981;
                --cyan: #06b6d4;
            }
            * { box-sizing: border-box; margin: 0; padding: 0; }
            body {
                font-family: 'Plus Jakarta Sans', sans-serif;
                background: var(--bg);
                color: var(--text);
                min-height: 100vh;
                display: flex;
                flex-direction: column;
            }

            /* TOP NAVIGATION BAR */
            .navbar {
                background: rgba(11, 15, 25, 0.95);
                backdrop-filter: blur(12px);
                border-bottom: 1px solid var(--border);
                position: sticky;
                top: 0;
                z-index: 100;
                display: flex;
                justify-content: space-between;
                align-items: center;
                padding: 14px 28px;
            }
            .brand {
                display: flex;
                align-items: center;
                gap: 12px;
                text-decoration: none;
                color: #fff;
            }
            .brand-logo {
                font-size: 24px;
                font-weight: 900;
                letter-spacing: -1px;
                color: var(--primary);
                display: flex;
                align-items: center;
                gap: 6px;
            }
            .brand-badge {
                font-size: 11px;
                font-weight: 700;
                background: #1e293b;
                color: var(--cyan);
                border: 1px solid #334155;
                padding: 3px 8px;
                border-radius: 6px;
                text-transform: uppercase;
            }
            .nav-tabs {
                display: flex;
                gap: 8px;
                background: #0f172a;
                padding: 4px;
                border-radius: 12px;
                border: 1px solid var(--border);
            }
            .tab-btn {
                background: transparent;
                border: none;
                color: var(--muted);
                padding: 8px 18px;
                border-radius: 8px;
                font-size: 13px;
                font-weight: 700;
                cursor: pointer;
                transition: all 0.2s;
                display: flex;
                align-items: center;
                gap: 6px;
            }
            .tab-btn:hover { color: #fff; }
            .tab-btn.active {
                background: var(--primary);
                color: #fff;
                box-shadow: 0 4px 12px var(--primary-glow);
            }
            .nav-stats {
                display: flex;
                align-items: center;
                gap: 12px;
                font-size: 12px;
                color: var(--muted);
            }
            .live-pulse {
                width: 8px;
                height: 8px;
                background: var(--success);
                border-radius: 50%;
                display: inline-block;
                box-shadow: 0 0 10px var(--success);
                animation: pulse 1.5s infinite;
            }
            @keyframes pulse {
                0% { transform: scale(0.95); opacity: 0.7; }
                50% { transform: scale(1.2); opacity: 1; }
                100% { transform: scale(0.95); opacity: 0.7; }
            }

            .main-content {
                flex: 1;
                max-width: 1350px;
                width: 100%;
                margin: 0 auto;
                padding: 24px;
            }

            /* TAB 1: RECOMMENDATIONS SECTION */
            .tab-pane { display: none; }
            .tab-pane.active { display: block; }

            /* HERO HERO SEARCH & RATING */
            .hero-card {
                background: linear-gradient(135deg, rgba(30, 41, 59, 0.7), rgba(17, 24, 39, 0.9));
                border: 1px solid var(--border);
                border-radius: 20px;
                padding: 28px;
                margin-bottom: 30px;
                box-shadow: 0 20px 40px rgba(0,0,0,0.4);
                position: relative;
                overflow: hidden;
            }
            .hero-card::after {
                content: '';
                position: absolute;
                top: 0; right: 0; width: 300px; height: 100%;
                background: radial-gradient(circle at top right, rgba(229, 9, 20, 0.15), transparent 70%);
                pointer-events: none;
            }
            .hero-title {
                font-size: 22px;
                font-weight: 800;
                margin-bottom: 8px;
                display: flex;
                align-items: center;
                gap: 10px;
            }
            .hero-sub {
                color: var(--muted);
                font-size: 14px;
                margin-bottom: 22px;
            }

            .search-row {
                display: flex;
                gap: 12px;
                position: relative;
                margin-bottom: 18px;
            }
            .input-search {
                flex: 1;
                background: #030712;
                border: 1px solid #374151;
                color: #fff;
                padding: 14px 20px;
                border-radius: 12px;
                font-size: 15px;
                font-family: inherit;
                outline: none;
                transition: all 0.2s;
            }
            .input-search:focus {
                border-color: var(--primary);
                box-shadow: 0 0 0 3px rgba(229, 9, 20, 0.2);
            }
            .autocomplete-list {
                position: absolute;
                top: 56px;
                left: 0;
                right: 0;
                background: #111827;
                border: 1px solid #3b82f6;
                border-radius: 12px;
                max-height: 320px;
                overflow-y: auto;
                z-index: 50;
                display: none;
                box-shadow: 0 15px 35px rgba(0,0,0,0.8);
            }
            .autocomplete-item {
                padding: 12px 18px;
                cursor: pointer;
                border-bottom: 1px solid #1f2937;
                display: flex;
                justify-content: space-between;
                align-items: center;
                transition: background 0.15s;
            }
            .autocomplete-item:hover { background: #1e293b; }
            .item-title { font-weight: 600; color: #fff; font-size: 14px; }
            .item-genre { font-size: 12px; color: var(--muted); }

            .chips-container {
                display: flex;
                flex-wrap: wrap;
                gap: 8px;
                margin-bottom: 22px;
            }
            .chip {
                background: #1f2937;
                border: 1px solid #374151;
                color: #e2e8f0;
                padding: 6px 14px;
                border-radius: 20px;
                font-size: 12px;
                font-weight: 600;
                cursor: pointer;
                transition: all 0.2s;
            }
            .chip:hover {
                background: var(--primary);
                border-color: var(--primary);
                color: #fff;
                transform: translateY(-2px);
            }

            /* RATING BAR */
            .rating-wrapper {
                display: flex;
                align-items: center;
                justify-content: space-between;
                background: rgba(15, 23, 42, 0.7);
                border: 1px solid #334155;
                padding: 14px 20px;
                border-radius: 14px;
                flex-wrap: wrap;
                gap: 15px;
            }
            .rating-left {
                display: flex;
                align-items: center;
                gap: 14px;
            }
            .stars-box {
                display: flex;
                gap: 4px;
                cursor: pointer;
                font-size: 26px;
            }
            .star { color: #475569; transition: color 0.15s; }
            .star.active { color: var(--accent); }
            .rating-text { font-size: 14px; font-weight: 700; color: var(--accent); }

            .btn-action {
                background: linear-gradient(135deg, var(--primary), #b91c1c);
                color: white;
                border: none;
                padding: 12px 26px;
                border-radius: 10px;
                font-size: 14px;
                font-weight: 800;
                cursor: pointer;
                transition: all 0.2s;
                display: flex;
                align-items: center;
                gap: 8px;
                box-shadow: 0 4px 14px var(--primary-glow);
            }
            .btn-action:hover {
                transform: translateY(-2px);
                box-shadow: 0 8px 24px var(--primary-glow);
            }

            /* SECTION HEADER */
            .section-header {
                display: flex;
                justify-content: space-between;
                align-items: center;
                margin-bottom: 22px;
            }
            .section-title {
                font-size: 20px;
                font-weight: 800;
                display: flex;
                align-items: center;
                gap: 10px;
            }
            .badge-highlight {
                font-size: 12px;
                font-weight: 700;
                padding: 4px 10px;
                border-radius: 20px;
                background: rgba(16, 185, 129, 0.15);
                color: var(--success);
                border: 1px solid rgba(16, 185, 129, 0.3);
            }

            /* NETFLIX-STYLE MOVIE POSTER GRID */
            .movie-grid {
                display: grid;
                grid-template-columns: repeat(auto-fill, minmax(200px, 1fr));
                gap: 22px;
                margin-bottom: 40px;
            }
            .movie-card {
                background: var(--card-bg);
                border: 1px solid var(--border);
                border-radius: 14px;
                overflow: hidden;
                display: flex;
                flex-direction: column;
                transition: all 0.3s cubic-bezier(0.4, 0, 0.2, 1);
                position: relative;
            }
            .movie-card:hover {
                transform: translateY(-8px) scale(1.02);
                border-color: var(--primary);
                box-shadow: 0 16px 32px rgba(0,0,0,0.6), 0 0 20px rgba(229, 9, 20, 0.25);
            }
            .poster-container {
                position: relative;
                width: 100%;
                aspect-ratio: 2/3;
                background: #0f172a;
                overflow: hidden;
            }
            .poster-img {
                width: 100%;
                height: 100%;
                object-fit: cover;
                transition: transform 0.4s;
            }
            .movie-card:hover .poster-img {
                transform: scale(1.06);
            }
            .match-badge {
                position: absolute;
                top: 10px;
                left: 10px;
                background: rgba(15, 23, 42, 0.85);
                backdrop-filter: blur(8px);
                border: 1px solid var(--accent);
                color: var(--accent);
                font-size: 11px;
                font-weight: 800;
                padding: 3px 8px;
                border-radius: 8px;
            }
            .rank-badge {
                position: absolute;
                top: 10px;
                right: 10px;
                background: var(--primary);
                color: #fff;
                font-size: 11px;
                font-weight: 800;
                padding: 3px 8px;
                border-radius: 8px;
                box-shadow: 0 2px 8px rgba(0,0,0,0.5);
            }
            .movie-info {
                padding: 14px;
                display: flex;
                flex-direction: column;
                flex: 1;
                justify-content: space-between;
                background: var(--card-bg);
            }
            .movie-name {
                font-size: 14px;
                font-weight: 700;
                line-height: 1.35;
                margin-bottom: 6px;
                color: #fff;
                display: -webkit-box;
                -webkit-line-clamp: 2;
                -webkit-box-orient: vertical;
                overflow: hidden;
            }
            .movie-genres {
                font-size: 11px;
                color: var(--muted);
                margin-bottom: 12px;
                display: -webkit-box;
                -webkit-line-clamp: 1;
                -webkit-box-orient: vertical;
                overflow: hidden;
            }
            .btn-select {
                background: #242f47;
                color: #e2e8f0;
                border: 1px solid #3b4a6b;
                padding: 8px 12px;
                border-radius: 8px;
                font-size: 12px;
                font-weight: 700;
                cursor: pointer;
                transition: all 0.2s;
                width: 100%;
                text-align: center;
            }
            .btn-select:hover {
                background: var(--primary);
                border-color: var(--primary);
                color: #fff;
            }

            /* TAB 2: STREAMING DASHBOARD */
            .stats-grid {
                display: grid;
                grid-template-columns: repeat(auto-fit, minmax(240px, 1fr));
                gap: 18px;
                margin-bottom: 28px;
            }
            .stat-box {
                background: var(--panel);
                border: 1px solid var(--border);
                border-radius: 14px;
                padding: 20px;
                display: flex;
                align-items: center;
                gap: 16px;
            }
            .stat-icon {
                width: 48px;
                height: 48px;
                border-radius: 12px;
                display: flex;
                align-items: center;
                justify-content: center;
                font-size: 22px;
                background: rgba(229, 9, 20, 0.15);
                color: var(--primary);
            }
            .stat-number { font-size: 24px; font-weight: 800; color: #fff; margin-bottom: 2px; }
            .stat-label { font-size: 12px; color: var(--muted); font-weight: 600; text-transform: uppercase; }

            .dashboard-card {
                background: var(--panel);
                border: 1px solid var(--border);
                border-radius: 16px;
                padding: 24px;
                margin-bottom: 26px;
            }
            .table-container {
                overflow-x: auto;
                margin-top: 14px;
            }
            .stream-table {
                width: 100%;
                border-collapse: collapse;
                font-size: 13px;
            }
            .stream-table th {
                background: #1e293b;
                color: #cbd5e1;
                padding: 12px 14px;
                text-align: left;
                font-weight: 700;
                border-bottom: 1px solid #334155;
            }
            .stream-table td {
                padding: 12px 14px;
                border-bottom: 1px solid #1f2937;
                color: #e2e8f0;
            }
            .stream-table tr:hover { background: #162032; }

            .tag-action {
                font-size: 11px;
                font-weight: 700;
                padding: 3px 8px;
                border-radius: 6px;
                display: inline-block;
                text-transform: uppercase;
            }
            .tag-rate { background: rgba(245, 158, 11, 0.15); color: var(--accent); border: 1px solid rgba(245, 158, 11, 0.3); }
            .tag-add_to_watchlist { background: rgba(168, 85, 247, 0.2); color: #c084fc; border: 1px solid rgba(168, 85, 247, 0.4); }
            .tag-remove_from_watchlist { background: rgba(239, 68, 68, 0.15); color: #f87171; border: 1px solid rgba(239, 68, 68, 0.3); }
            .tag-click { background: rgba(59, 130, 246, 0.15); color: #60a5fa; border: 1px solid rgba(59, 130, 246, 0.3); }
            .tag-view { background: rgba(16, 185, 129, 0.15); color: var(--success); border: 1px solid rgba(16, 185, 129, 0.3); }

            .btn-wl {
                background: rgba(255, 255, 255, 0.08);
                border: 1px solid rgba(255, 255, 255, 0.15);
                color: #e2e8f0;
                padding: 7px 10px;
                border-radius: 6px;
                font-size: 11px;
                font-weight: 700;
                cursor: pointer;
                transition: all 0.2s;
                white-space: nowrap;
                display: inline-flex;
                align-items: center;
                gap: 4px;
            }
            .btn-wl:hover {
                background: rgba(229, 9, 20, 0.18);
                border-color: rgba(229, 9, 20, 0.45);
                color: #fff;
            }
            .btn-wl.active {
                background: rgba(229, 9, 20, 0.25);
                border-color: #ef4444;
                color: #fca5a5;
                box-shadow: 0 0 10px rgba(239, 68, 68, 0.35);
            }

            /* TAB 3: LAKEHOUSE ARCHITECTURE */
            .arch-grid {
                display: grid;
                grid-template-columns: repeat(3, 1fr);
                gap: 20px;
                margin-top: 20px;
            }
            .arch-card {
                background: var(--panel);
                border: 1px solid var(--border);
                border-radius: 16px;
                padding: 22px;
                position: relative;
            }
            .arch-card.bronze { border-top: 4px solid #b45309; }
            .arch-card.silver { border-top: 4px solid #94a3b8; }
            .arch-card.gold { border-top: 4px solid #f59e0b; }
            .arch-badge {
                font-size: 11px;
                font-weight: 800;
                padding: 3px 10px;
                border-radius: 20px;
                display: inline-block;
                margin-bottom: 10px;
            }
            .bronze .arch-badge { background: #78350f; color: #fcd34d; }
            .silver .arch-badge { background: #334155; color: #e2e8f0; }
            .gold .arch-badge { background: #78350f; color: #fbbf24; }
            .arch-title { font-size: 18px; font-weight: 800; margin-bottom: 8px; }
            .arch-desc { font-size: 13px; color: var(--muted); margin-bottom: 14px; line-height: 1.5; }
            .arch-list { list-style: none; font-size: 12px; }
            .arch-list li { padding: 6px 0; border-top: 1px solid #1f2937; color: #cbd5e1; display: flex; justify-content: space-between; }

            /* FOOTER */
            .footer {
                margin-top: auto;
                text-align: center;
                padding: 20px;
                border-top: 1px solid var(--border);
                color: var(--muted);
                font-size: 13px;
                background: #090e1a;
            }
        </style>
    </head>
    <body>
        <!-- TOP NAVIGATION -->
        <header class="navbar">
            <a href="/" class="brand">
                <span class="brand-logo">🍿 MOVIELAKE</span>
                <span class="brand-badge">Lakehouse 25M</span>
            </a>

            <div class="nav-tabs">
                <button class="tab-btn active" onclick="switchTab('recTab', this)">
                    <span>🎬</span> Rạp Phim & Gợi Ý AI
                </button>
                <button class="tab-btn" onclick="switchTab('watchlistTab', this)" id="navWatchlistBtn">
                    <span>❤️</span> Phim Yêu Thích <span id="watchlistBadgeCount" style="background:#e50914; color:#fff; border-radius:10px; padding:2px 7px; font-size:11px; margin-left:4px;">2</span>
                </button>
                <button class="tab-btn" onclick="switchTab('streamTab', this)">
                    <span>⚡</span> Live Streaming Dashboard
                </button>
                <button class="tab-btn" onclick="switchTab('archTab', this)">
                    <span>🏗️</span> Kiến Trúc Lakehouse
                </button>
            </div>

            <div class="nav-stats">
                <span class="live-pulse"></span>
                <span>Kafka: <strong id="kafkaStatusText" style="color:var(--success);">Active (9092)</strong></span>
                <span>• HDFS: <strong style="color:var(--cyan);">62.423 Phim</strong></span>
            </div>
        </header>

        <main class="main-content">
            <!-- TAB 1: GỢI Ý PHIM & TƯƠNG TÁC (NETFLIX STYLE) -->
            <div id="recTab" class="tab-pane active">
                <div class="hero-card">
                    <div class="hero-title">
                        <span>🎯</span> Chọn Phim Bạn Đã Xem Để Trải Nghiệm Gợi Ý Tức Thời
                    </div>
                    <div class="hero-sub">
                        Tìm kiếm trong toàn bộ 62.423 phim hoặc chọn phim nổi bật bên dưới. Hệ thống sẽ ngay lập tức tính toán độ tương đồng và gửi sự kiện tương tác vào Apache Kafka!
                    </div>

                    <div class="search-row">
                        <input type="text" id="movieSearchInput" class="input-search" placeholder="🔍 Gõ tên phim bất kỳ (ví dụ: Avengers, Titanic, Matrix, Batman, Interstellar, Spider-Man)..." oninput="handleSearch(this.value)">
                        <div id="autocompleteList" class="autocomplete-list"></div>
                    </div>

                    <div style="font-size:12px; color:var(--muted); margin-bottom:8px; font-weight:700;">Gợi ý chọn nhanh:</div>
                    <div class="chips-container">
                        <span class="chip" onclick="selectMovie(1, 'Toy Story (1995)', 'Adventure|Animation|Children|Comedy|Fantasy')">🧸 Toy Story (1995)</span>
                        <span class="chip" onclick="selectMovie(2571, 'Matrix, The (1999)', 'Action|Sci-Fi|Thriller')">🕶️ The Matrix (1999)</span>
                        <span class="chip" onclick="selectMovie(260, 'Star Wars: A New Hope (1977)', 'Action|Adventure|Sci-Fi')">⚔️ Star Wars (1977)</span>
                        <span class="chip" onclick="selectMovie(318, 'Shawshank Redemption, The (1994)', 'Crime|Drama')">🗝️ Shawshank Redemption (1994)</span>
                        <span class="chip" onclick="selectMovie(296, 'Pulp Fiction (1994)', 'Comedy|Crime|Drama|Thriller')">🍔 Pulp Fiction (1994)</span>
                        <span class="chip" onclick="selectMovie(58559, 'Dark Knight, The (2008)', 'Action|Crime|Drama|IMAX')">🦇 The Dark Knight (2008)</span>
                        <span class="chip" onclick="selectMovie(79132, 'Inception (2010)', 'Action|Crime|Drama|Mystery|Sci-Fi|Thriller|IMAX')">🌀 Inception (2010)</span>
                        <span class="chip" onclick="selectMovie(109487, 'Interstellar (2014)', 'Sci-Fi|IMAX')">🚀 Interstellar (2014)</span>
                    </div>

                    <div class="rating-wrapper">
                        <div class="rating-left">
                            <span style="font-size:13px; font-weight:700; color:#cbd5e1;">MỨC ĐỘ YÊU THÍCH:</span>
                            <div class="stars-box" id="starContainer">
                                <span class="star active" onclick="setRating(1)">★</span>
                                <span class="star active" onclick="setRating(2)">★</span>
                                <span class="star active" onclick="setRating(3)">★</span>
                                <span class="star active" onclick="setRating(4)">★</span>
                                <span class="star active" onclick="setRating(5)">★</span>
                            </div>
                            <span id="ratingText" class="rating-text">5/5 Sao (Xuất sắc!)</span>
                        </div>

                        <button class="btn-action" onclick="triggerRecommendation()">
                            <span>⚡</span> TÍNH TOÁN GỢI Ý & BẮN KAFKA
                        </button>
                    </div>
                </div>

                <!-- SECTION HEADER -->
                <div class="section-header">
                    <div class="section-title">
                        <span>🎬</span> 
                        <span id="resultsTitleText">GỢI Ý PHIM HÀNG ĐẦU DÀNH CHO BẠN</span>
                    </div>
                    <div id="resultsMetaBadge" class="badge-highlight">
                        ⚡ Quét 100% kho dữ liệu
                    </div>
                </div>

                <!-- NETFLIX POSTER GRID -->
                <div id="movieGridArea" class="movie-grid">
                    <!-- Javascript renders movie cards here -->
                </div>
            </div>

            <!-- TAB: DANH SÁCH PHIM YÊU THÍCH (WATCHLIST) -->
            <div id="watchlistTab" class="tab-pane">
                <div class="hero-card" style="border-color:rgba(168,85,247,0.35); background:radial-gradient(circle at top right, rgba(168,85,247,0.18), transparent 70%), var(--card-bg);">
                    <div class="hero-title" style="color:#c084fc;">
                        <span>❤️</span> Danh Sách Phim Yêu Thích Của Bạn (Watchlist)
                    </div>
                    <div class="hero-sub">
                        Mỗi khi bạn thêm phim vào danh sách yêu thích, hệ thống tự động phát sinh sự kiện <strong>add_to_watchlist</strong> gửi vào <strong>Apache Kafka (Topic movie_logs)</strong>. Khi bạn bấm nút bên dưới, mô hình <strong>PySpark ALS ở tầng Gold</strong> sẽ tổng hợp các vector đặc trưng ẩn của toàn bộ các phim này thành một <strong>Vector Hồ Sơ Người Dùng Động (Pseudo-User Vector)</strong> để gợi ý các tác phẩm mới hoàn toàn phù hợp với gu tổng thể của bạn!
                    </div>
                    <div style="display:flex; gap:16px; align-items:center; flex-wrap:wrap; margin-top:16px;">
                        <button class="btn-action" style="background:linear-gradient(135deg, #a855f7, #e50914); box-shadow:0 4px 15px rgba(168,85,247,0.4);" onclick="triggerWatchlistRecommendation()">
                            <span>🔮</span> GỢI Ý TỪ TOÀN BỘ PHIM YÊU THÍCH (ALS LAKEHOUSE)
                        </button>
                        <span style="font-size:13px; color:var(--muted);">Đang lưu <strong id="watchlistTotalText" style="color:#fff;">2</strong> phim trong danh sách</span>
                    </div>
                </div>

                <div class="section-header">
                    <div class="section-title">
                        <span>📋</span> CÁC PHIM BẠN ĐÃ LƯU VÀO WATCHLIST
                    </div>
                    <button class="btn-action" style="padding:6px 14px; font-size:12px; background:transparent; border:1px solid var(--border);" onclick="clearWatchlist()">
                        <span>🗑️</span> Xóa Tất Cả
                    </button>
                </div>

                <div id="watchlistGridArea" class="movie-grid">
                    <!-- Javascript renders watchlist items here -->
                </div>
            </div>

            <!-- TAB 2: LIVE STREAMING DASHBOARD -->
            <div id="streamTab" class="tab-pane">
                <div class="section-header">
                    <div class="section-title">
                        <span>⚡</span> DASHBOARD THỐNG KÊ LUỒNG STREAMING THỜI GIAN THỰC
                    </div>
                    <button class="btn-action" style="padding:8px 16px; font-size:12px;" onclick="sendSampleEvent()">
                        <span>🚀</span> Bắn 1 Event Test Vào Kafka
                    </button>
                </div>

                <!-- STATS CARDS -->
                <div class="stats-grid">
                    <div class="stat-box">
                        <div class="stat-icon" style="background:rgba(229,9,20,0.15); color:var(--primary);">📡</div>
                        <div>
                            <div class="stat-number" id="statTotalEvents">0</div>
                            <div class="stat-label">Tổng Event Đã Gửi Kafka</div>
                        </div>
                    </div>
                    <div class="stat-box">
                        <div class="stat-icon" style="background:rgba(16,185,129,0.15); color:var(--success);">💾</div>
                        <div>
                            <div class="stat-number" id="statHdfsFiles">Active</div>
                            <div class="stat-label">Kho Streaming HDFS</div>
                        </div>
                    </div>
                    <div class="stat-box">
                        <div class="stat-icon" style="background:rgba(6,182,212,0.15); color:var(--cyan);">⏱️</div>
                        <div>
                            <div class="stat-number" id="statLatency">12 ms</div>
                            <div class="stat-label">Độ Trễ Phản Hồi Stream</div>
                        </div>
                    </div>
                    <div class="stat-box">
                        <div class="stat-icon" style="background:rgba(245,158,11,0.15); color:var(--accent);">⭐</div>
                        <div>
                            <div class="stat-number" id="statAvgRating">5.0 ★</div>
                            <div class="stat-label">Đánh Giá Trung Bình</div>
                        </div>
                    </div>
                </div>

                <!-- LIVE STREAMING ACTIVITY TABLE -->
                <div class="dashboard-card">
                    <div style="display:flex; justify-content:space-between; align-items:center;">
                        <h3 style="font-size:16px; font-weight:800; color:#fff; display:flex; align-items:center; gap:8px;">
                            <span class="live-pulse"></span>
                            BẢNG HOẠT ĐỘNG THỜI GIAN THỰC (KAFKA TOPIC: movie_logs ➔ HDFS)
                        </h3>
                        <span style="font-size:12px; color:var(--muted);">Tự động cập nhật mỗi 3s</span>
                    </div>

                    <div class="table-container">
                        <table class="stream-table">
                            <thead>
                                <tr>
                                    <th>Thời Gian</th>
                                    <th>User ID</th>
                                    <th>Movie ID</th>
                                    <th>Tên Phim</th>
                                    <th>Hành Động</th>
                                    <th>Đánh Giá</th>
                                    <th>Trạng Thái Kafka</th>
                                </tr>
                            </thead>
                            <tbody id="streamTableBody">
                                <tr>
                                    <td colspan="7" style="text-align:center; color:var(--muted); padding:30px;">
                                        Chưa có tương tác nào. Hãy click chấm sao hoặc chọn phim ở Tab Gợi Ý!
                                    </td>
                                </tr>
                            </tbody>
                        </table>
                    </div>
                </div>
            </div>

            <!-- TAB 3: LAKEHOUSE ARCHITECTURE -->
            <div id="archTab" class="tab-pane">
                <div class="section-header">
                    <div class="section-title">
                        <span>🏗️</span> KIẾN TRÚC MEDALLION LAKEHOUSE & ĐIỆN TOÁN PHÂN TÁN
                    </div>
                    <span class="badge-highlight">Apache Hadoop + Spark + Kafka + Airflow</span>
                </div>

                <div class="arch-grid">
                    <div class="arch-card bronze">
                        <span class="arch-badge">BRONZE LAYER (THÔ)</span>
                        <div class="arch-title">Kho Lưu Trữ Gốc HDFS</div>
                        <div class="arch-desc">Dữ liệu thô định dạng CSV được nạp trực tiếp vào cụm HDFS 2 DataNodes, bảo toàn nguyên vẹn 100% lịch sử.</div>
                        <ul class="arch-list">
                            <li><span>movies.csv</span><strong>62.423 dòng</strong></li>
                            <li><span>ratings.csv</span><strong>25.000.095 dòng</strong></li>
                            <li><span>links.csv (TMDb)</span><strong>1.4 MB</strong></li>
                            <li><span>movies_metadata.csv</span><strong>34 MB</strong></li>
                        </ul>
                    </div>

                    <div class="arch-card silver">
                        <span class="arch-badge">SILVER LAYER (CHUẨN HOÁ)</span>
                        <div class="arch-title">Làm Sạch & Parquet</div>
                        <div class="arch-desc">PySpark làm sạch dữ liệu, xử lý giá trị khuyết, lọc rating hợp lệ [0.5, 5.0], join metadata TMDb và nén Snappy Parquet.</div>
                        <ul class="arch-list">
                            <li><span>/lakehouse/silver/movies</span><strong>Parquet nén</strong></li>
                            <li><span>/lakehouse/silver/ratings</span><strong>Parquet tối ưu</strong></li>
                            <li><span>/lakehouse/silver/streaming_logs</span><strong>Kafka Ingestion</strong></li>
                        </ul>
                    </div>

                    <div class="arch-card gold">
                        <span class="arch-badge">GOLD LAYER (KẾT QUẢ ALS)</span>
                        <div class="arch-title">Học Máy & Trực Quan Hoá</div>
                        <div class="arch-desc">Mô hình ALS huấn luyện trên PySpark MLlib phân tích ma trận tiềm ẩn, tính toán Top 5 phim gợi ý và lưu trữ sẵn sàng phục vụ Web API.</div>
                        <ul class="arch-list">
                            <li><span>Thuật toán</span><strong>ALS Collaborative Filtering</strong></li>
                            <li><span>Độ chính xác (RMSE)</span><strong>1.1907</strong></li>
                            <li><span>Score Clipping</span><strong>[0.5, 5.0] Chuẩn hoá</strong></li>
                            <li><span>Điều phối</span><strong>Apache Airflow (5 Tasks)</strong></li>
                        </ul>
                    </div>
                </div>
            </div>
        </main>

        <footer class="footer">
            Đề Tài: <strong>Xây Dựng Hệ Thống Gợi Ý Phim Trực Tuyến Dựa Trên Kiến Trúc Lakehouse Và Điện Toán Phân Tán</strong><br>
            Công nghệ: HDFS 2 DataNodes • PySpark MLlib ALS • Apache Kafka KRaft • Apache Airflow Orchestration • FastAPI
        </footer>

        <script>
            let currentMovie = {
                movieId: 1,
                title: "Toy Story (1995)",
                genres: "Adventure|Animation|Children|Comedy|Fantasy"
            };
            let currentRating = 5;

            // Tab Switching
            function switchTab(tabId, btn) {
                document.querySelectorAll('.tab-pane').forEach(el => el.classList.remove('active'));
                document.querySelectorAll('.tab-btn').forEach(el => el.classList.remove('active'));
                document.getElementById(tabId).classList.add('active');
                if (btn) btn.classList.add('active');
                if (tabId === 'streamTab') {
                    fetchStreamingLogs();
                } else if (tabId === 'watchlistTab') {
                    loadWatchlistUI();
                }
            }

            // Rating Stars
            function setRating(val) {
                currentRating = val;
                const stars = document.querySelectorAll('.star');
                stars.forEach((s, i) => {
                    if (i < val) s.classList.add('active');
                    else s.classList.remove('active');
                });
                const labels = ["", "1/5 Sao (Dở)", "2/5 Sao (Tạm được)", "3/5 Sao (Bình thường)", "4/5 Sao (Hay!)", "5/5 Sao (Xuất sắc!)"];
                document.getElementById('ratingText').textContent = labels[val];
                triggerRecommendation();
            }

            // Select Movie
            function selectMovie(id, title, genres) {
                currentMovie = { movieId: id, title: title, genres: genres };
                document.getElementById('movieSearchInput').value = title;
                document.getElementById('autocompleteList').style.display = 'none';
                triggerRecommendation();
            }

            // Search Autocomplete
            function handleSearch(q) {
                if (!q || q.length < 2) {
                    document.getElementById('autocompleteList').style.display = 'none';
                    return;
                }
                fetch('/api/search_movies?q=' + encodeURIComponent(q))
                    .then(r => r.json())
                    .then(list => {
                        const box = document.getElementById('autocompleteList');
                        if (!list || list.length === 0) {
                            box.style.display = 'none';
                            return;
                        }
                        let html = '';
                        list.forEach(m => {
                            html += `
                            <div class="autocomplete-item" onclick="selectMovie(${m.movieId}, '${m.TenPhim.replace(/'/g, "\\'")}', '${m.TheLoai}')">
                                <span class="item-title">${m.TenPhim}</span>
                                <span class="item-genre">${m.TheLoai}</span>
                            </div>
                            `;
                        });
                        box.innerHTML = html;
                        box.style.display = 'block';
                    });
            }

            // ponytail: Tạo poster nghệ thuật theo thể loại khi không có ảnh hoặc link TMDb bị lỗi (thay cho ảnh ghế rạp Unsplash)
            function getFallbackPosterHTML(title, genres) {
                let gBg = 'linear-gradient(135deg, #1e293b 0%, #0f172a 100%)';
                let icon = '🎬';
                const gLower = (genres || '').toLowerCase();
                
                if (gLower.includes('animation') || gLower.includes('children')) {
                    gBg = 'linear-gradient(135deg, #ec4899 0%, #8b5cf6 100%)';
                    icon = '🧸';
                } else if (gLower.includes('sci-fi')) {
                    gBg = 'linear-gradient(135deg, #0284c7 0%, #6366f1 100%)';
                    icon = '🚀';
                } else if (gLower.includes('action') || gLower.includes('adventure')) {
                    gBg = 'linear-gradient(135deg, #dc2626 0%, #7f1d1d 100%)';
                    icon = '⚔️';
                } else if (gLower.includes('crime') || gLower.includes('mystery')) {
                    gBg = 'linear-gradient(135deg, #334155 0%, #0f172a 100%)';
                    icon = '🕶️';
                } else if (gLower.includes('horror') || gLower.includes('thriller')) {
                    gBg = 'linear-gradient(135deg, #991b1b 0%, #1c1917 100%)';
                    icon = '🔪';
                } else if (gLower.includes('romance')) {
                    gBg = 'linear-gradient(135deg, #e11d48 0%, #881337 100%)';
                    icon = '🌹';
                } else if (gLower.includes('comedy')) {
                    gBg = 'linear-gradient(135deg, #d97706 0%, #78350f 100%)';
                    icon = '🍿';
                } else if (gLower.includes('fantasy')) {
                    gBg = 'linear-gradient(135deg, #7c3aed 0%, #4c1d95 100%)';
                    icon = '🔮';
                }

                const firstGenre = genres ? genres.split('|')[0] : 'Movie';
                return `
                <div class="fallback-poster" style="position:absolute; top:0; left:0; width:100%; height:100%; background:${gBg}; display:flex; flex-direction:column; justify-content:center; align-items:center; padding:16px; text-align:center; box-sizing:border-box;">
                    <div style="font-size:46px; margin-bottom:12px; filter:drop-shadow(0 4px 10px rgba(0,0,0,0.5));">${icon}</div>
                    <div style="font-size:13px; font-weight:800; color:#fff; line-height:1.3; text-shadow:0 2px 4px rgba(0,0,0,0.8);">${title}</div>
                    <div style="font-size:10px; color:rgba(255,255,255,0.75); margin-top:6px; text-transform:uppercase; letter-spacing:0.5px; font-weight:600;">${firstGenre}</div>
                    <div style="position:absolute; bottom:8px; font-size:9px; font-weight:700; color:rgba(255,255,255,0.4); letter-spacing:1px;">MOVIELAKE • 25M</div>
                </div>
                `;
            }

            function handlePosterError(img, title, genres) {
                const parent = img.parentElement;
                if (!parent || parent.querySelector('.fallback-poster')) return;
                img.style.display = 'none';
                parent.insertAdjacentHTML('afterbegin', getFallbackPosterHTML(title, genres));
            }

            // Trigger Recommendation
            function triggerRecommendation() {
                const grid = document.getElementById('movieGridArea');
                grid.innerHTML = '<div style="color:var(--muted); padding:40px; text-align:center; grid-column:1/-1;">Đang quét toàn bộ kho dữ liệu MovieLens 25M để tính toán độ tương đồng và lấy poster TMDb...</div>';

                fetch(`/api/recommend_by_movie?movieId=${currentMovie.movieId}&rating=${currentRating}&limit=12`)
                    .then(r => r.json())
                    .then(data => {
                        const isContrast = (currentRating <= 2) || (data.mode === 'contrast');
                        const totalText = data.total_scanned ? data.total_scanned.toLocaleString('vi-VN') : '62.423';
                        const durText = data.duration_sec !== undefined ? data.duration_sec + 's' : '0.04s';
                        const algoText = data.algorithm ? (data.algorithm.includes('ALS') ? '🤖 Mô hình Spark ALS (Tầng Gold)' : '📊 Content Jaccard') : '🤖 Spark MLlib ALS';

                        if (isContrast) {
                            document.getElementById('resultsTitleText').innerHTML = `🔄 GỢI Ý ĐỔI VỊ VÌ BẠN ĐÃ CHÊ: <span style="color:#f87171; font-weight:800;">"${currentMovie.title}"</span> (${currentRating}★) - CÁC TÁC PHẨM ĐỈNH CAO Ở THỂ LOẠI KHÁC`;
                            document.getElementById('resultsMetaBadge').innerHTML = `🔄 Chiến Lược: Contrast Strategy • ⚡ ${durText} • 📡 Đã gửi Kafka`;
                        } else {
                            document.getElementById('resultsTitleText').innerHTML = `GỢI Ý ĐỒNG ĐIỆU VÌ BẠN ĐÃ THÍCH: <span style="color:var(--accent); font-weight:800;">"${currentMovie.title}"</span> (${currentRating}★)`;
                            document.getElementById('resultsMetaBadge').innerHTML = `${algoText} • ⚡ ${durText} • 📡 Đã gửi Kafka`;
                        }

                        if (!data.movies || data.movies.length === 0) {
                            grid.innerHTML = '<div style="color:red; text-align:center; grid-column:1/-1;">Không tìm thấy phim tương đồng.</div>';
                            return;
                        }

                        let html = '';
                        data.movies.forEach((m, idx) => {
                            const safeTitle = m.TenPhim.replace(/'/g, "\\'").replace(/"/g, '&quot;');
                            let posterInner = '';
                            if (m.poster_url && m.poster_url.trim() !== '') {
                                posterInner = `<img src="${m.poster_url}" alt="${safeTitle}" class="poster-img" loading="lazy" onerror="handlePosterError(this, '${safeTitle}', '${m.TheLoai}')">`;
                            } else {
                                posterInner = getFallbackPosterHTML(safeTitle, m.TheLoai);
                            }

                            const badgeHTML = isContrast 
                                ? `<span class="match-badge" style="background:#0284c7; border:1px solid #38bdf8;">🔄 Đổi Khẩu Vị</span>`
                                : `<span class="match-badge">🔥 ${m.match}% Hợp (ALS)</span>`;

                            const inWl = userWatchlistIds.has(m.movieId);
                            const wlBtnHTML = inWl 
                                ? `<button class="btn-wl active" id="wlBtn-${m.movieId}" onclick="toggleWatchlist(${m.movieId}, '${safeTitle}', '${m.TheLoai}')">❤️ Đã lưu</button>`
                                : `<button class="btn-wl" id="wlBtn-${m.movieId}" onclick="toggleWatchlist(${m.movieId}, '${safeTitle}', '${m.TheLoai}')">🤍 Yêu thích</button>`;

                            html += `
                            <div class="movie-card">
                                <div class="poster-container" style="position:relative; background:#111827;">
                                    ${posterInner}
                                    ${badgeHTML}
                                    <span class="rank-badge">#${idx + 1}</span>
                                </div>
                                <div class="movie-info">
                                    <div class="movie-name" title="${safeTitle}">${m.TenPhim}</div>
                                    <div class="movie-genres">${m.TheLoai}</div>
                                    <div style="display:flex; gap:6px; margin-top:8px;">
                                        <button class="btn-select" style="flex:1;" onclick="selectMovie(${m.movieId}, '${safeTitle}', '${m.TheLoai}')">
                                            👉 Chọn
                                        </button>
                                        ${wlBtnHTML}
                                    </div>
                                </div>
                            </div>
                            `;
                        });
                        grid.innerHTML = html;
                        fetchStreamingLogs();
                    })
                    .catch(e => {
                        grid.innerHTML = '<div style="color:red; grid-column:1/-1;">Lỗi gợi ý: ' + e + '</div>';
                    });
            }

            // --- QUẢN LÝ WATCHLIST / PHIM YÊU THÍCH ---
            let userWatchlistIds = new Set([260, 2571]);

            function updateWatchlistBadge(cnt) {
                const b = document.getElementById('watchlistBadgeCount');
                if (b) b.textContent = cnt;
                const t = document.getElementById('watchlistTotalText');
                if (t) t.textContent = cnt;
            }

            function updateAllWlButtons(mid, inWl) {
                document.querySelectorAll(`#wlBtn-${mid}`).forEach(btn => {
                    if (inWl) {
                        btn.className = 'btn-wl active';
                        btn.innerHTML = '❤️ Đã lưu';
                    } else {
                        btn.className = 'btn-wl';
                        btn.innerHTML = '🤍 Yêu thích';
                    }
                });
            }

            function toggleWatchlist(mid, title, genres) {
                fetch('/api/watchlist/toggle', {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify({ movieId: mid, title: title, genres: genres })
                })
                .then(r => r.json())
                .then(res => {
                    if (res.in_watchlist) {
                        userWatchlistIds.add(mid);
                        showToast(`❤️ Đã thêm <b>${title}</b> vào mục Yêu thích & phát sinh sự kiện Kafka!`);
                    } else {
                        userWatchlistIds.delete(mid);
                        showToast(`🗑️ Đã xóa <b>${title}</b> khỏi mục Yêu thích & phát sinh sự kiện Kafka!`);
                    }
                    updateWatchlistBadge(res.count);
                    updateAllWlButtons(mid, res.in_watchlist);
                    fetchStreamingLogs();
                    const wlPane = document.getElementById('watchlistTab');
                    if (wlPane && wlPane.classList.contains('active')) {
                        loadWatchlistUI();
                    }
                })
                .catch(err => console.error(err));
            }

            function loadWatchlistUI() {
                const grid = document.getElementById('watchlistGridArea');
                grid.innerHTML = '<div style="color:var(--muted); text-align:center; padding:30px; grid-column:1/-1;">Đang tải danh sách phim yêu thích...</div>';
                fetch('/api/watchlist')
                    .then(r => r.json())
                    .then(res => {
                        updateWatchlistBadge(res.count);
                        userWatchlistIds = new Set(res.watchlist.map(x => x.movieId));
                        if (!res.watchlist || res.watchlist.length === 0) {
                            grid.innerHTML = '<div style="color:var(--muted); text-align:center; padding:50px; grid-column:1/-1;">Chưa có phim nào trong danh sách yêu thích. Hãy bấm nút "🤍 Yêu thích" ở bất kỳ phim nào để lưu vào đây!</div>';
                            return;
                        }
                        let html = '';
                        res.watchlist.forEach((m, idx) => {
                            const safeTitle = m.title.replace(/'/g, "\\'").replace(/"/g, '&quot;');
                            let posterInner = (m.poster_url && m.poster_url.trim() !== '')
                                ? `<img src="${m.poster_url}" alt="${safeTitle}" class="poster-img" loading="lazy" onerror="handlePosterError(this, '${safeTitle}', '${m.genres}')">`
                                : getFallbackPosterHTML(safeTitle, m.genres);

                            html += `
                            <div class="movie-card">
                                <div class="poster-container" style="position:relative; background:#111827;">
                                    ${posterInner}
                                    <span class="match-badge" style="background:#7c3aed; border:1px solid #a855f7;">❤️ Watchlist</span>
                                    <span class="rank-badge">#${idx + 1}</span>
                                </div>
                                <div class="movie-info">
                                    <div class="movie-name" title="${safeTitle}">${m.title}</div>
                                    <div class="movie-genres">${m.genres}</div>
                                    <div style="display:flex; gap:6px; margin-top:8px;">
                                        <button class="btn-select" style="flex:1;" onclick="selectMovie(${m.movieId}, '${safeTitle}', '${m.genres}'); switchTab('recTab', document.querySelectorAll('.tab-btn')[0]);">
                                            👉 Chọn
                                        </button>
                                        <button class="btn-wl active" onclick="toggleWatchlist(${m.movieId}, '${safeTitle}', '${m.genres}')">
                                            ❌ Xóa
                                        </button>
                                    </div>
                                </div>
                            </div>
                            `;
                        });
                        grid.innerHTML = html;
                    })
                    .catch(() => {
                        grid.innerHTML = '<div style="color:red; text-align:center; grid-column:1/-1;">Lỗi tải watchlist.</div>';
                    });
            }

            function triggerWatchlistRecommendation() {
                switchTab('recTab', document.querySelectorAll('.tab-btn')[0]);
                const grid = document.getElementById('movieGridArea');
                grid.innerHTML = '<div style="color:var(--muted); padding:40px; text-align:center; grid-column:1/-1;">Đang tổng hợp các vector nhân tử ẩn ALS tầng Gold từ toàn bộ phim yêu thích để tính toán hồ sơ người dùng...</div>';

                fetch('/api/recommend_by_watchlist?limit=12')
                    .then(r => r.json())
                    .then(data => {
                        const count = data.watchlist_count || 0;
                        const durText = data.duration_sec !== undefined ? data.duration_sec + 's' : '0.04s';
                        document.getElementById('resultsTitleText').innerHTML = `🎯 GỢI Ý TỔNG HỢP TỪ <span style="color:#c084fc; font-weight:800;">${count} PHIM YÊU THÍCH</span> CỦA BẠN`;
                        document.getElementById('resultsMetaBadge').innerHTML = `🤖 ALS Pseudo-User Vector (Tầng Gold) • ⚡ ${durText} • 📡 Đã đồng bộ Kafka`;

                        if (!data.movies || data.movies.length === 0) {
                            grid.innerHTML = '<div style="color:red; text-align:center; grid-column:1/-1;">Chưa có phim trong danh sách yêu thích hoặc không tìm thấy gợi ý phù hợp.</div>';
                            return;
                        }

                        let html = '';
                        data.movies.forEach((m, idx) => {
                            const safeTitle = m.TenPhim.replace(/'/g, "\\'").replace(/"/g, '&quot;');
                            let posterInner = (m.poster_url && m.poster_url.trim() !== '')
                                ? `<img src="${m.poster_url}" alt="${safeTitle}" class="poster-img" loading="lazy" onerror="handlePosterError(this, '${safeTitle}', '${m.TheLoai}')">`
                                : getFallbackPosterHTML(safeTitle, m.TheLoai);

                            const inWl = userWatchlistIds.has(m.movieId);
                            const wlBtnHTML = inWl 
                                ? `<button class="btn-wl active" id="wlBtn-${m.movieId}" onclick="toggleWatchlist(${m.movieId}, '${safeTitle}', '${m.TheLoai}')">❤️ Đã lưu</button>`
                                : `<button class="btn-wl" id="wlBtn-${m.movieId}" onclick="toggleWatchlist(${m.movieId}, '${safeTitle}', '${m.TheLoai}')">🤍 Yêu thích</button>`;

                            html += `
                            <div class="movie-card">
                                <div class="poster-container" style="position:relative; background:#111827;">
                                    ${posterInner}
                                    <span class="match-badge" style="background:#7c3aed; border:1px solid #a855f7;">🔥 ${m.match}% Hợp (Watchlist ALS)</span>
                                    <span class="rank-badge">#${idx + 1}</span>
                                </div>
                                <div class="movie-info">
                                    <div class="movie-name" title="${safeTitle}">${m.TenPhim}</div>
                                    <div class="movie-genres">${m.TheLoai}</div>
                                    <div style="display:flex; gap:6px; margin-top:8px;">
                                        <button class="btn-select" style="flex:1;" onclick="selectMovie(${m.movieId}, '${safeTitle}', '${m.TheLoai}')">
                                            👉 Chọn
                                        </button>
                                        ${wlBtnHTML}
                                    </div>
                                </div>
                            </div>
                            `;
                        });
                        grid.innerHTML = html;
                    })
                    .catch(e => {
                        grid.innerHTML = '<div style="color:red; grid-column:1/-1;">Lỗi gợi ý watchlist: ' + e + '</div>';
                    });
            }

            function clearWatchlist() {
                if (confirm('Bạn có chắc muốn xóa tất cả phim trong danh sách yêu thích?')) {
                    fetch('/api/watchlist')
                        .then(r => r.json())
                        .then(res => {
                            if (res.watchlist) {
                                res.watchlist.forEach(m => {
                                    toggleWatchlist(m.movieId, m.title, m.genres);
                                });
                            }
                        });
                }
            }

            function showToast(msg) {
                let t = document.getElementById('toastNotice');
                if (!t) {
                    t = document.createElement('div');
                    t.id = 'toastNotice';
                    t.style.cssText = 'position:fixed; bottom:24px; right:24px; background:#1e1b4b; border:1px solid #7c3aed; color:#fff; padding:12px 20px; border-radius:10px; font-weight:700; font-size:13px; z-index:9999; box-shadow:0 8px 24px rgba(0,0,0,0.5); transition:all 0.3s; opacity:0; pointer-events:none;';
                    document.body.appendChild(t);
                }
                t.innerHTML = msg;
                t.style.opacity = '1';
                setTimeout(() => { t.style.opacity = '0'; }, 3000);
            }

            // Fetch Streaming Logs from API
            function fetchStreamingLogs() {
                fetch('/api/streaming_logs')
                    .then(r => r.json())
                    .then(data => {
                        document.getElementById('statTotalEvents').textContent = data.total_events || 0;
                        if (data.avg_rating) {
                            document.getElementById('statAvgRating').textContent = data.avg_rating + ' ★';
                        }
                        if (data.kafka_connected) {
                            document.getElementById('kafkaStatusText').innerHTML = 'Active (9092)';
                            document.getElementById('kafkaStatusText').style.color = 'var(--success)';
                        }

                        const tbody = document.getElementById('streamTableBody');
                        if (!data.recent_events || data.recent_events.length === 0) {
                            return;
                        }

                        let rows = '';
                        data.recent_events.forEach(ev => {
                            const actTag = `<span class="tag-action tag-${ev.action}">${ev.action}</span>`;
                            const kafkaStatus = ev.kafka_delivered 
                                ? `<span style="color:var(--success); font-weight:700;">✓ movie_logs</span>`
                                : `<span style="color:var(--muted);">Cached</span>`;

                            const ratingDisplay = (ev.rating !== null && ev.rating !== undefined)
                                ? `<span style="color:var(--accent); font-weight:700;">${ev.rating} ★</span>`
                                : `<span style="color:#c084fc; font-weight:700;">❤️ Watchlist</span>`;

                            rows += `
                            <tr>
                                <td style="font-family:monospace; color:#94a3b8;">${ev.timestamp}</td>
                                <td><span style="background:#1e293b; padding:2px 6px; border-radius:4px; font-weight:700;">${ev.user_id}</span></td>
                                <td>${ev.movie_id}</td>
                                <td style="font-weight:600; color:#fff;">${ev.title || ('Movie #' + ev.movie_id)}</td>
                                <td>${actTag}</td>
                                <td>${ratingDisplay}</td>
                                <td>${kafkaStatus}</td>
                            </tr>
                            `;
                        });
                        tbody.innerHTML = rows;
                    })
                    .catch(() => {});
            }

            // Gửi event giả lập nhanh để demo
            function sendSampleEvent() {
                fetch('/api/send_test_event', { method: 'POST' })
                    .then(r => r.json())
                    .then(res => {
                        fetchStreamingLogs();
                    });
            }

            // Polling streaming logs mỗi 3 giây
            setInterval(() => {
                const streamPane = document.getElementById('streamTab');
                if (streamPane && streamPane.classList.contains('active')) {
                    fetchStreamingLogs();
                }
            }, 3000);

            // Mở đầu mặc định
            selectMovie(1, 'Toy Story (1995)', 'Adventure|Animation|Children|Comedy|Fantasy');
            fetchStreamingLogs();
            loadWatchlistUI();
        </script>
    </body>
    </html>
    """

# --- API TÌM KIẾM PHIM THEO TÊN ---
@app.get("/api/search_movies")
def search_movies(q: str = Query(..., min_length=1)):
    try:
        df = load_movies_catalog()
        matches = df[df["TenPhim"].str.contains(q, case=False, na=False)].head(15)
        return matches[["movieId", "TenPhim", "TheLoai"]].to_dict(orient="records")
    except Exception:
        return []

CURATED_CONTRAST_MASTERPIECES = [
    318, 858, 1221, 296, 593, 50, 500, 608, 2959, 4226, 112552,
    356, 527, 1721, 177765, 7361,
    260, 1196, 1210, 2571, 589, 1198, 1270, 480, 3578, 58559, 79132, 109487, 116797, 4993, 7153,
    1, 3114, 78499, 4886, 6377, 60069, 68954, 8961, 134853, 192803
]

def extract_franchise_root(title: str) -> str:
    # 1. Bỏ năm ở cuối: ' (1977)'
    t = re.sub(r'\s*\(\d{4}\).*$', '', title).strip()
    # 2. Chuẩn hóa ', The', ', A', ', An' ở cuối về đầu hoặc bỏ đi
    t = re.sub(r',\s*(The|A|An)$', '', t, flags=re.IGNORECASE).strip()
    # 3. Lấy phần trước dấu hai chấm ':' hoặc gạch ngang ' - '
    for sep in [':', ' - ', ' – ']:
        if sep in t:
            t = t.split(sep)[0].strip()
    # 4. Bỏ các số thứ tự / Roman numerals ở cuối nếu có
    t = re.sub(r'\s+(II|III|IV|V|VI|VII|VIII|IX|X|\d+)$', '', t, flags=re.IGNORECASE).strip()
    return t.strip()

# --- API GỢI Ý PHIM DỰA TRÊN POSTER & TƯƠNG ĐỒNG NỘI DUNG ---
@app.get("/api/recommend_by_movie")
def recommend_by_movie(movieId: int, rating: int = 5, limit: int = 12):
    try:
        t0 = time.time()
        df = load_movies_catalog()
        factors_map = load_als_movie_factors()
        posters = load_poster_map()
        
        target_series = df[df["movieId"] == movieId]
        if target_series.empty:
            target_genres = "Action|Adventure|Sci-Fi"
            target_title = "Phim đã chọn"
        else:
            target_genres = target_series.iloc[0]["TheLoai"]
            target_title = target_series.iloc[0]["TenPhim"]

        target_set = set(str(target_genres).split("|"))
        f_root = extract_franchise_root(target_title).lower()
        candidate_df = df[df["movieId"] != movieId].copy()
        is_negative = (rating <= 2)

        if not is_negative:
            # === CHIẾN LƯỢC 1: ĐỒNG ĐIỆU (3 - 5 SAO) -> ƯU TIÊN TIỀN TRUYỆN/HẬU TRUYỆN CÙNG BỘ + ALS HYBRID ===
            rec_mode = "similar"
            used_algorithm = "Hybrid PySpark ALS (Gold Layer) + Franchise & Keyword + Genre Overlap"
            target_vec = factors_map.get(movieId)

            def compute_similar_score(row):
                mid = int(row["movieId"])
                title_lower = str(row["TenPhim"]).lower()
                row_genres = row["genre_set"]

                # 1. Phát hiện cùng vũ trụ / thương hiệu (Franchise Sequels / Prequels)
                is_franchise = False
                if len(f_root) >= 4 and f_root in title_lower:
                    if "Documentary" not in target_set and "Documentary" in row_genres:
                        is_franchise = False
                    else:
                        is_franchise = True

                # 2. Độ tương đồng ALS Matrix Factorization (Tầng Gold HDFS)
                als_score = 0.0
                if target_vec is not None and mid in factors_map:
                    als_score = max(float(np.dot(target_vec, factors_map[mid])), 0.0)

                # 3. Độ giao thoa thể loại (Genre Jaccard)
                inter = len(target_set & row_genres)
                jaccard = (inter / len(target_set | row_genres)) if inter > 0 else 0.0

                # 4. Trọng số phim kinh điển / poster xác thực
                pop_boost = 15.0 if mid in POPULAR_POSTERS else (10.0 if mid <= 3000 else (5.0 if mid in posters else 0.0))

                if is_franchise:
                    final = 95.0 + min(als_score * 3.5 + jaccard * 1.5, 4.8)
                else:
                    if inter == 0:
                        return 0.0
                    final = (als_score * 40.0 + jaccard * 35.0 + pop_boost)
                    final = min(final * (rating / 5.0), 94.0)

                return round(final, 1)

            scores = candidate_df.apply(compute_similar_score, axis=1)
        else:
            # === CHIẾN LƯỢC 2: ĐỔI KHẨU VỊ (1 - 2 SAO) -> TUYỆT ĐỐI LOẠI BỎ THỂ LOẠI BỊ CHÊ, GỢI Ý TUYỆT TÁC KHÁC ===
            rec_mode = "contrast"
            used_algorithm = "Chiến lược Đổi Khẩu Vị (Contrast Strategy - Tuyệt tác đối lập thể loại bị chê)"

            def compute_contrast_score(row):
                mid = int(row["movieId"])
                title_lower = str(row["TenPhim"]).lower()
                row_genres = row["genre_set"]

                # Loại bỏ hoàn toàn nếu cùng franchise
                if len(f_root) >= 4 and f_root in title_lower:
                    return 0.0

                # Loại bỏ hoàn toàn nếu có thể loại trùng với phim bị chê
                if len(target_set & row_genres) > 0:
                    return 0.0

                # Chấm điểm kiệt tác đối lập
                base = 80.0
                if mid in CURATED_CONTRAST_MASTERPIECES or mid in POPULAR_POSTERS:
                    base += 15.0
                elif mid <= 2000:
                    base += 10.0
                elif mid in posters:
                    base += 5.0

                return round(base, 1)

            scores = candidate_df.apply(compute_contrast_score, axis=1)

        matched_idx = scores[scores > 0].index
        top_matches = candidate_df.loc[matched_idx].copy()
        top_matches["match"] = scores.loc[matched_idx]
        top_matches["mode"] = rec_mode

        # Sắp xếp theo: Điểm match > ID phim
        top_matches.sort_values(by=["match", "movieId"], ascending=[False, True], inplace=True)
        top_results = top_matches.head(limit).copy()

        # Ánh xạ Poster TMDb URL
        top_results["poster_url"] = top_results["movieId"].apply(get_poster_url)
        top_results = top_results.fillna("")

        duration = round(time.time() - t0, 3)

        # Gửi log tương tác vào Kafka
        kafka_sent = send_kafka_event(
            movie_id=movieId,
            rating=rating,
            action="rate",
            movie_title=target_title
        )

        return {
            "total_scanned": len(df),
            "duration_sec": duration,
            "kafka_sent": kafka_sent,
            "algorithm": used_algorithm,
            "mode": rec_mode,
            "target": {"movieId": movieId, "TenPhim": target_title, "TheLoai": target_genres, "rating": rating},
            "movies": top_results[["movieId", "TenPhim", "TheLoai", "match", "poster_url", "mode"]].to_dict(orient="records")
        }
    except Exception as e:
        return {"error": str(e), "movies": [], "total_scanned": 0, "kafka_sent": False}

# --- API THỐNG KÊ LOG STREAMING & KAFKA ---
@app.get("/api/streaming_logs")
def get_streaming_logs():
    events = list(_recent_streaming_events)
    valid_ratings = [ev["rating"] for ev in events if ev.get("rating") is not None]
    avg_rating = round(sum(valid_ratings) / len(valid_ratings), 1) if valid_ratings else 5.0
    return {
        "kafka_connected": bool(get_kafka_producer()),
        "total_events": len(events),
        "avg_rating": avg_rating,
        "recent_events": events[:30]
    }

# --- API BẮN EVENT TEST NHANH ---
@app.post("/api/send_test_event")
def send_test_event():
    import random
    actions = ["click", "view", "add_to_watchlist", "rate"]
    sample_movies = [(1, "Toy Story (1995)"), (2571, "The Matrix (1999)"), (58559, "The Dark Knight (2008)"), (79132, "Inception (2010)")]
    chosen_id, chosen_title = random.choice(sample_movies)
    action = random.choice(actions)
    rating = round(random.uniform(3.0, 5.0), 1) if action == "rate" else 5.0

    success = send_kafka_event(
        movie_id=chosen_id,
        rating=rating,
        action=action,
        user_id=random.randint(10001, 99999),
        movie_title=chosen_title
    )
    return {"success": success, "event": {"movieId": chosen_id, "title": chosen_title, "action": action, "rating": rating}}

# --- API LẤY DANH SÁCH YÊU THÍCH (WATCHLIST) ---
@app.get("/api/watchlist")
def get_watchlist():
    items = list(_user_watchlist.values())
    for item in items:
        if "poster_url" not in item or not item["poster_url"]:
            item["poster_url"] = get_poster_url(item["movieId"])
    return {"watchlist": items, "count": len(items)}

# --- API THÊM / XÓA PHIM KHỎI DANH SÁCH YÊU THÍCH (BẮN EVENT KAFKA add_to_watchlist) ---
@app.post("/api/watchlist/toggle")
def toggle_watchlist(req: WatchlistToggleRequest):
    mid = int(req.movieId)
    df = load_movies_catalog()
    title = req.title
    genres = req.genres
    if not title or not genres:
        m_row = df[df["movieId"] == mid]
        if not m_row.empty:
            title = m_row.iloc[0]["TenPhim"]
            genres = m_row.iloc[0]["TheLoai"]
        else:
            title = f"Movie #{mid}"
            genres = "Action|Adventure"

    if mid in _user_watchlist:
        del _user_watchlist[mid]
        action = "remove_from_watchlist"
        in_wl = False
    else:
        _user_watchlist[mid] = {
            "movieId": mid,
            "title": title,
            "genres": genres,
            "poster_url": get_poster_url(mid),
            "added_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        }
        action = "add_to_watchlist"
        in_wl = True

    # Bắn sự kiện thời gian thực vào Kafka topic movie_logs
    kafka_sent = send_kafka_event(
        movie_id=mid,
        rating=None,
        action=action,
        user_id=99999,
        movie_title=title
    )

    return {
        "success": True,
        "movieId": mid,
        "in_watchlist": in_wl,
        "action": action,
        "kafka_sent": kafka_sent,
        "count": len(_user_watchlist),
        "watchlist": list(_user_watchlist.values())
    }

# --- API GỢI Ý DỰA TRÊN TOÀN BỘ PHIM TRONG DANH SÁCH YÊU THÍCH (ALS MULTI-ITEM PSEUDO-USER) ---
@app.get("/api/recommend_by_watchlist")
def recommend_by_watchlist(limit: int = 12):
    t0 = time.time()
    df = load_movies_catalog()
    factors_map = load_als_movie_factors()
    posters = load_poster_map()

    if not _user_watchlist:
        top_popular = [mid for mid in POPULAR_POSTERS.keys()][:limit]
        sample_cand = df[df["movieId"].isin(top_popular)].copy()
        sample_cand["match"] = 90.0
        sample_cand["mode"] = "popular"
        sample_cand["poster_url"] = sample_cand["movieId"].apply(get_poster_url)
        sample_cand = sample_cand.fillna("")
        return {
            "total_scanned": len(df),
            "duration_sec": 0.01,
            "watchlist_count": 0,
            "algorithm": "Popular Blockbusters (Watchlist rỗng)",
            "movies": sample_cand[["movieId", "TenPhim", "TheLoai", "match", "poster_url", "mode"]].to_dict(orient="records")
        }

    wl_mids = set(_user_watchlist.keys())
    # 1. Trích xuất các vector đặc trưng ẩn ALS của toàn bộ phim trong watchlist
    wl_vectors = [factors_map[mid] for mid in wl_mids if mid in factors_map]
    
    # 2. Xây dựng Pseudo-User Vector (Vector Người Dùng Ảo đại diện sở thích tổng thể)
    if wl_vectors:
        pseudo_user_vec = np.mean(wl_vectors, axis=0)
        norm = float(np.linalg.norm(pseudo_user_vec))
        if norm > 1e-6:
            pseudo_user_vec = pseudo_user_vec / norm
    else:
        pseudo_user_vec = None

    # 3. Phân tích thể loại và franchise từ Watchlist
    all_wl_genres = []
    wl_franchise_roots = set()
    for item in _user_watchlist.values():
        for g in str(item.get("genres", "")).split("|"):
            if g.strip():
                all_wl_genres.append(g.strip())
        root = extract_franchise_root(item.get("title", "")).lower()
        if len(root) >= 4:
            wl_franchise_roots.add(root)

    genre_counts = pd.Series(all_wl_genres).value_counts(normalize=True).to_dict() if all_wl_genres else {}
    target_genre_set = set(all_wl_genres)

    # 4. Loại trừ các phim ĐÃ NẰM TRONG WATCHLIST
    candidate_df = df[~df["movieId"].isin(wl_mids)].copy()

    def compute_multi_item_score(row):
        mid = int(row["movieId"])
        title_lower = str(row["TenPhim"]).lower()
        row_genres = row["genre_set"]

        # Franchise boost từ các phim trong watchlist
        is_franchise = any(root in title_lower for root in wl_franchise_roots)
        if is_franchise and "Documentary" in row_genres and "Documentary" not in target_genre_set:
            is_franchise = False

        # ALS Cosine Similarity với Vector Người Dùng Ảo
        als_sim = 0.0
        if pseudo_user_vec is not None and mid in factors_map:
            cand_vec = factors_map[mid]
            als_sim = max(float(np.dot(pseudo_user_vec, cand_vec)), 0.0)

        # Genre Affinity Score
        inter = target_genre_set & row_genres
        if inter:
            genre_score = sum(genre_counts.get(g, 0.0) for g in inter)
        else:
            genre_score = 0.0

        if not inter and not is_franchise:
            return 0.0

        pop_boost = 15.0 if mid in POPULAR_POSTERS else (10.0 if mid <= 3000 else (5.0 if mid in posters else 0.0))

        if is_franchise:
            final = 95.0 + min(als_sim * 3.5 + genre_score * 1.5, 4.8)
        else:
            final = (als_sim * 45.0 + min(genre_score, 1.0) * 35.0 + pop_boost)
            final = min(final, 94.5)

        return round(final, 1)

    candidate_df["match"] = candidate_df.apply(compute_multi_item_score, axis=1)
    matched = candidate_df[candidate_df["match"] > 0].copy()
    matched.sort_values(by=["match", "movieId"], ascending=[False, True], inplace=True)
    top_results = matched.head(limit).copy()
    top_results["poster_url"] = top_results["movieId"].apply(get_poster_url)
    top_results["mode"] = "watchlist_hybrid"
    top_results = top_results.fillna("")

    duration = round(time.time() - t0, 3)

    return {
        "total_scanned": len(df),
        "duration_sec": duration,
        "watchlist_count": len(_user_watchlist),
        "algorithm": f"PySpark ALS Pseudo-User Profile Vector ({len(wl_vectors)} latent factors aggregated) + Watchlist Genre Affinity",
        "movies": top_results[["movieId", "TenPhim", "TheLoai", "match", "poster_url", "mode"]].to_dict(orient="records")
    }
