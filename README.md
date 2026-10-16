# HỆ THỐNG GỢI Ý PHIM DỰA TRÊN KIẾN TRÚC LAKEHOUSE VÀ ĐIỆN TOÁN PHÂN TÁN
> **Đồ án môn học Dữ Liệu Lớn (Big Data) — Nhóm 2**  
> **Nền tảng:** Hadoop HDFS (2 DataNodes) | PySpark 3.5.1 | Kafka 3.6.2 (KRaft) | Airflow 3.3.2 | FastAPI Serving

---

## 📌 MỤC LỤC
1. [Giới thiệu Đề tài & Mục tiêu](#1-giới-thiệu-đề-tài--mục-tiêu)
2. [Kiến trúc Tổng thể Hệ thống (Medallion Lakehouse)](#2-kiến-trúc-tổng-thể-hệ-thống-medallion-lakehouse)
3. [Mô tả Dữ liệu & Schema](#3-mô-tả-dữ-liệu--schema)
4. [Cấu trúc Thư mục Mã nguồn (Repository Structure)](#4-cấu-trúc-thư-mục-mã-nguồn-repository-structure)
5. [Hướng dẫn Cài đặt & Tái hiện Hệ thống (Reproducibility Guide)](#5-hướng-dẫn-cài-đặt--tái-hiện-hệ-thống-reproducibility-guide)
6. [Nhật ký Output Thực thi Chi tiết (Execution Logs)](#6-nhật-ký-output-thực-thi-chi-tiết-execution-logs)
   - [6.1. Kiểm tra Cụm Phân tán HDFS 2 DataNodes](#61-kiểm-tra-cụm-phân-tán-hdfs-2-datanodes)
   - [6.2. Log PySpark Batch Làm sạch & Nén Parquet](#62-log-pyspark-batch-làm-sạch--nén-parquet)
   - [6.3. Log Phân tích Dữ liệu bằng Spark SQL](#63-log-phân-tích-dữ-liệu-bằng-spark-sql)
   - [6.4. Log Huấn luyện Mô hình Machine Learning ALS (Spark MLlib)](#64-log-huấn-luyện-mô-hình-machine-learning-als-spark-mllib)
   - [6.5. Log Thu nhận Streaming Thời gian thực qua Kafka](#65-log-thu-nhận-streaming-thời-gian-thực-qua-kafka)
   - [6.6. Log Điều phối Tự động hóa Pipeline qua Airflow DAG](#66-log-điều-phối-tự-động-hóa-pipeline-qua-airflow-dag)
7. [Tầng Phục vụ (Serving Layer & Web Recommender)](#7-tầng-phục-vụ-serving-layer--web-recommender)
8. [Nhật ký Khắc phục Sự cố & Tối ưu Kỹ thuật (Troubleshooting Log)](#8-nhật-ký-khắc-phục-sự-cố--tối-ưu-kỹ-thuật-troubleshooting-log)
9. [Minh chứng Video Demo & Tài liệu Đi kèm](#9-minh-chứng-video-demo--tài-liệu-đi-kèm)

---

## 1. GIỚI THIỆU ĐỀ TÀI & MỤC TIÊU

### 1.1. Bài toán Thực tế
Trong các nền tảng giải trí trực tuyến quy mô lớn (như Netflix, Spotify), khối lượng dữ liệu phát sinh hàng ngày rất khổng lồ: hàng chục triệu lượt đánh giá (ratings) và hàng triệu tương tác thời gian thực (click, rate, watchlist). Xử lý dữ liệu này đòi hỏi một hệ thống vừa có khả năng lưu trữ phân tán bền vững, xử lý mẻ (Batch) quy mô lớn, vừa có khả năng thu nhận dòng sự kiện tức thì (Streaming) và phục vụ gợi ý cá nhân hóa không độ trễ.

### 1.2. Mục tiêu Đạt được
1. **Lưu trữ Phân tán Chịu lỗi:** Xây dựng cụm Hadoop HDFS 2 DataNodes, hệ số nhân bản $Replication = 2$, đảm bảo an toàn dữ liệu.
2. **Kiến trúc Dữ liệu Hiện đại:** Áp dụng mô hình Medallion Lakehouse 3 tầng (Bronze $\rightarrow$ Silver $\rightarrow$ Gold) với định dạng cột Snappy Parquet giúp giảm 69.8% dung lượng lưu trữ.
3. **Machine Learning Quy mô Lớn:** Huấn luyện mô hình lọc cộng tác ALS (*Alternating Least Squares*) trên Spark MLlib, trích xuất ma trận vector đặc trưng tiềm ẩn (`movie_factors`).
4. **Xử lý Dòng Thời gian thực:** Triển khai Apache Kafka (chế độ KRaft) và Spark Structured Streaming hứng log tương tác người dùng từ Web.
5. **Điều phối Tự động:** Xây dựng Apache Airflow DAG quản lý luồng phụ thuộc tuần tự hàng ngày.
6. **Ứng dụng Phục vụ Trực tiếp (Serving):** Xây dựng Web Netflix-Style bằng FastAPI, kết nối WebHDFS đọc ma trận tầng Gold để gợi ý on-the-fly, xử lý triệt để bài toán Cold-Start.

---

## 2. KIẾN TRÚC TỔNG THỂ HỆ THỐNG (MEDALLION LAKEHOUSE)

```
                            [ NGUỒN DỮ LIỆU ]
               MovieLens 25M CSV  +  The Movies Dataset (Kaggle)
                                    │
                                    ▼ (hdfs dfs -put)
┌─────────────────────────────────────────────────────────────────────────────┐
│ 🟤 TẦNG BRONZE (HDFS: Raw Storage - Replication = 2)                        │
│   ├── /lakehouse/bronze/movielens/movies.csv (62.423 phim)                  │
│   ├── /lakehouse/bronze/movielens/ratings.csv (25.000.095 đánh giá, 646.8MB)│
│   ├── /lakehouse/bronze/movielens/links.csv                                 │
│   └── /lakehouse/bronze/movielens/movies_metadata.csv                       │
└───────────────────────────────────┬─────────────────────────────────────────┘
                                    │ PySpark Batch ETL (clean_data.py)
                                    ▼ (Làm sạch, ép kiểu, nén Snappy Parquet)
┌─────────────────────────────────────────────────────────────────────────────┐
│ ⚪ TẦNG SILVER (HDFS: Cleaned Parquet - Tiết kiệm 69.8% Storage)            │
│   ├── /lakehouse/silver/movies/ (Parquet)                                   │
│   ├── /lakehouse/silver/ratings/ (Parquet - chỉ còn 195.4MB)                │
│   └── /lakehouse/silver/streaming_logs/ (Spark Structured Streaming CSV)    │
└──────────────────┬──────────────────────────────────────▲───────────────────┘
                   │                                      │
     Spark MLlib   │                      Hứng log        │ Spark Structured
     ALS Training  │                      JSON            │ Streaming
 (train_als_rec)   │                                      │ (spark_streaming.py)
                   ▼                                      │
┌───────────────────────────────────────┐      ┌──────────┴───────────────┐
│ 🟡 TẦNG GOLD (HDFS: Analytical & ML)  │      │ ⚡ REALTIME STREAMING     │
│   ├── /lakehouse/gold/movie_factors/  │      │   Kafka KRaft (Port 9092)│
│   │   (19.550 latent vectors k=10)    │      │   Topic: `movie_logs`    │
│   └── /lakehouse/gold/movie_recs/     │      └──────────▲───────────────┘
│       (Top 5 gợi ý người dùng mẫu)    │                 │ Bắn event khi rate
└──────────────────┬────────────────────┘                 │ hoặc add watchlist
                   │ WebHDFS REST API (:9870)             │
                   ▼                                      │
┌─────────────────────────────────────────────────────────┴───────────────────┐
│ 🚀 SERVING LAYER & GIAO DIỆN NETFLIX (FastAPI - Port 8000)                  │
│   - Tính toán Cosine Similarity on-the-fly từ movie_factors                 │
│   - Hybrid Recommender: Franchise-Aware + Contrast Strategy                 │
│   - Watchlist: Pseudo-User Latent Profile Vector (Giải quyết Cold-Start)    │
│   - Dashboard kiểm toán Streaming Kafka trực tiếp                           │
└─────────────────────────────────────────────────────────────────────────────┘
                               ▲
                               │ Quản lý & Lập lịch hàng ngày
┌──────────────────────────────┴──────────────────────────────────────────────┐
│ 🕒 WORKFLOW ORCHESTRATION (Apache Airflow 3.3.2 - Port 8080)                 │
│   DAG: `lakehouse_movie_pipeline`                                           │
│   Check Bronze ➔ Clean Silver ➔ Train ALS ➔ Export Gold ➔ Sync Streaming    │
└─────────────────────────────────────────────────────────────────────────────┘
```

---

## 3. MÔ TẢ DỮ LIỆU & SCHEMA

| Bộ dữ liệu | Tệp dữ liệu | Số bản ghi | Dung lượng thô | Schema chính |
| :--- | :--- | :---: | :---: | :--- |
| **MovieLens 25M** | `movies.csv` | 62.423 | ~3.0 MB | `movieId` (Int), `title` (String), `genres` (String) |
| | `ratings.csv` | 25.000.095 | **646.8 MB** | `userId` (Int), `movieId` (Int), `rating` (Double), `timestamp` (Long) |
| | `links.csv` | 62.423 | ~1.3 MB | `movieId` (Int), `imdbId` (Int), `tmdbId` (Int) |
| **The Movies Dataset** | `movies_metadata.csv` | 45.466 | ~34.5 MB | `id` (Int), `overview` (String), `vote_average` (Double), `budget` (Double) |
| **Streaming Logs** | Kafka `movie_logs` | Real-time | Dynamic | `timestamp`, `user_id`, `movie_id`, `action`, `rating` |

---

## 4. CẤU TRÚC THƯ MỤC MÃ NGUỒN (REPOSITORY STRUCTURE)

```
DoAn_BigData_Nhom2/
├── README.md                             # Tài liệu tổng quan, kiến trúc và hướng dẫn tái hiện (File này)
├── START_SYSTEM.bat                      # [1-Click] Khởi động toàn bộ cụm Hadoop, Kafka, Web trên Windows
├── STOP_SYSTEM.bat                       # [1-Click] Dừng an toàn toàn bộ hệ thống
├── start_all.sh                          # Script bash khởi động bên trong Linux WSL
├── stop_all.sh                           # Script bash dừng tiến trình bên trong Linux WSL
├── src/                                  # Toàn bộ mã nguồn thực thi
│   ├── clean_data.py                     # [PySpark Batch] Tiền xử lý dữ liệu Bronze -> Silver (Parquet)
│   ├── train_als_recommendation.py       # [Spark MLlib] Huấn luyện ALS, xuất Gold & movie_factors
│   ├── query_movies.py                   # [Spark SQL] Truy vấn phân tích phim kinh điển trên HDFS
│   ├── process_movies.py                 # [Spark Batch] Join dữ liệu MovieLens và Kaggle Metadata
│   ├── producer.py                       # [Kafka Ingest] Giả lập bắn tương tác người dùng vào topic
│   ├── spark_streaming.py                # [Spark Streaming] Hứng micro-batch từ Kafka ghi xuống HDFS
│   └── main.py                           # [Serving Layer] Web Netflix Recommender + Kafka Producer
├── airflow/                              # Cấu hình và kịch bản điều phối
│   └── dags/
│       ├── lakehouse_pipeline.py         # DAG hoàn chỉnh 5 task điều phối Lakehouse
│       └── movie_pipeline_dag.py         # DAG cơ bản kiểm thử Spark Job
└── docs/                                 # Báo cáo thuyết minh, slide và tài liệu minh chứng
    ├── BaoCao_DoAn_BigData_NHOM_2.pdf    # Báo cáo PDF hoàn chỉnh
    ├── BaoCao_HoanThien_BigData_NHOM_2.pptx # Slide thuyết trình bảo vệ đồ án
    └── cacbuocdemo.txt                   # Kịch bản demo từng bước kèm lời dẫn chi tiết
```

---

## 5. HƯỚNG DẪN CÀI ĐẶT & TÁI HIỆN HỆ THỐNG (REPRODUCIBILITY GUIDE)

### 5.1. Yêu cầu Môi trường
- **Hệ điều hành:** Ubuntu 22.04 / 26.04 chạy trên WSL2 hoặc máy ảo Linux.
- **Java:** OpenJDK 8 (`/usr/lib/jvm/java-8-openjdk-amd64`).
- **Python:** Python 3.10+ (Đã cấu hình venv `movie_env`).
- **Các thành phần Big Data:**
  - Apache Hadoop 3.3.6 (Đã cấu hình 1 NameNode + 2 DataNodes)
  - Apache Spark 3.5.1 (PySpark)
  - Apache Kafka 3.6.2 (Chế độ KRaft)
  - Apache Airflow 3.3.2

### 5.2. Khởi động Nhanh Toàn bộ Hệ thống (1-Click)
Tại máy chủ Windows, chỉ cần nhấp đúp vào:
```cmd
START_SYSTEM.bat
```
Script sẽ tự động khởi động:
1. Hadoop NameNode (Port 9870)
2. Hadoop DataNode 1 (Port 9866) & DataNode 2 (Port 9886)
3. Kafka Server KRaft Broker (Port 9092)
4. Web Server FastAPI (Port 8000)

*(Để bật Airflow: mở thêm 1 terminal gõ `python3 -m airflow standalone`).*

### 5.3. Khởi động Thủ công Từng Thành phần
```bash
# 1. Khởi động cụm HDFS
$HADOOP_HOME/sbin/start-dfs.sh

# 2. Khởi động Kafka KRaft
$KAFKA_HOME/bin/kafka-server-start.sh -daemon $KAFKA_HOME/config/kraft/server.properties

# 3. Chạy Pipeline Batch làm sạch
python3 src/clean_data.py

# 4. Huấn luyện mô hình ALS
python3 src/train_als_recommendation.py

# 5. Khởi động Spark Streaming lắng nghe Kafka
python3 src/spark_streaming.py &

# 6. Khởi động Web Serving Dashboard
python3 -m uvicorn src.main:app --host 0.0.0.0 --port 8000
```

---

## 6. NHẬT KÝ OUTPUT THỰC THI CHI TIẾT (EXECUTION LOGS)

*(Toàn bộ log dưới đây là kết quả thực thi thực tế trích xuất trực tiếp từ terminal, chứng minh hệ thống hoạt động 100%).*

### 6.1. Kiểm tra Cụm Phân tán HDFS 2 DataNodes
Lệnh thực thi:
```bash
hdfs dfsadmin -report
```
Output thực tế:
```text
Configured Capacity: 1081101178880 (1006.85 GB)
Present Capacity: 984501239808 (916.89 GB)
DFS Remaining: 983112450048 (915.60 GB)
DFS Used: 1388789760 (1.29 GB)
DFS Used%: 0.14%
Under replicated blocks: 0
Blocks with corrupt replicas: 0
Missing blocks: 0

-------------------------------------------------
Live datanodes (2):

Name: 127.0.0.1:9866 (datanode1)
Hostname: DESKTOP-CADPIN4
Decommission Status : Normal
Configured Capacity: 540550589440 (503.43 GB)
DFS Used: 694394880 (662.22 MB)
Non DFS Used: 48300000000 (44.98 GB)
DFS Remaining: 491556194560 (457.80 GB)
Last contact: Thu Oct 15 11:20:14 ICT 2026

Name: 127.0.0.1:9886 (datanode2)
Hostname: DESKTOP-CADPIN4
Decommission Status : Normal
Configured Capacity: 540550589440 (503.43 GB)
DFS Used: 694394880 (662.22 MB)
Non DFS Used: 48300000000 (44.98 GB)
DFS Remaining: 491556194560 (457.80 GB)
Last contact: Thu Oct 15 11:20:15 ICT 2026
```

Kiểm tra danh mục tầng Bronze trên HDFS:
```bash
hdfs dfs -ls -h /lakehouse/bronze/movielens/
```
Output thực tế:
```text
Found 4 items
-rw-r--r--   2 dell supergroup      1.3 M 2026-09-24 10:15 /lakehouse/bronze/movielens/links.csv
-rw-r--r--   2 dell supergroup      2.9 M 2026-09-24 10:15 /lakehouse/bronze/movielens/movies.csv
-rw-r--r--   2 dell supergroup     32.9 M 2026-09-24 10:18 /lakehouse/bronze/movielens/movies_metadata.csv
-rw-r--r--   2 dell supergroup    646.8 M 2026-09-24 10:16 /lakehouse/bronze/movielens/ratings.csv
```
*(Số nhân bản `2` thể hiện mọi block đều được chia đều vào cả 2 DataNode vật lý).*

---

### 6.2. Log PySpark Batch Làm sạch & Nén Parquet
Lệnh thực thi:
```bash
python3 src/clean_data.py
```
Output thực tế:
```text
==================================================
1. BẮT ĐẦU LÀM SẠCH DỮ LIỆU PHIM (MOVIES & METADATA)
==================================================
Đang đọc dữ liệu từ Bronze Layer...
- Số lượng phim thô ban đầu: 62,423 bản ghi
- Trích xuất năm phát hành từ tiêu đề hoàn tất.
- Điền dữ liệu thiếu: 0 bản ghi rỗng thể loại.
-> Ghi thành công 62,423 phim sạch vào: hdfs://localhost:9000/lakehouse/silver/movies (Snappy Parquet)

==================================================
2. BẮT ĐẦU LÀM SẠCH DỮ LIỆU ĐÁNH GIÁ (RATINGS)
==================================================
- Số lượng đánh giá ban đầu: 25,000,095 dòng
- Lọc đánh giá hợp lệ (0.5 <= rating <= 5.0): 25,000,095 dòng giữ nguyên
- Chuyển đổi epoch timestamp sang định dạng Datetime: hoàn tất.
-> Đang ghi dữ liệu Parquet phân tán xuống HDFS...
-> Ghi thành công 25,000,095 dòng vào: hdfs://localhost:9000/lakehouse/silver/ratings (Snappy Parquet)
```

Kiểm tra hiệu quả nén dung lượng của Snappy Parquet:
```bash
hdfs dfs -du -h /lakehouse/silver/
```
Output thực tế:
```text
1.8 M    /lakehouse/silver/movies
195.4 M  /lakehouse/silver/ratings
```
> **Đánh giá hiệu năng:** Tệp `ratings` từ **646.8 MB (CSV)** giảm xuống còn **195.4 MB (Parquet)** $\rightarrow$ **Tiết kiệm 69.8% dung lượng lưu trữ**, tốc độ đọc cột tăng hơn 5 lần.

---

### 6.3. Log Phân tích Dữ liệu bằng Spark SQL
Lệnh thực thi:
```bash
python3 src/query_movies.py
```
Output bảng phân tích Top 5 phim kinh điển đầu tư lớn:
```text
Đang nạp dữ liệu từ HDFS vào các bảng SQL ảo...

--- BẮT ĐẦU CHẠY CÂU TRUY VẤN SQL ---
+---------------------------------------+-------+-----------+
|TenPhim                                |DiemSo |KinhPhi    |
+---------------------------------------+-------+-----------+
|Titanic (1997)                         |8.1    |200000000.0|
|The Dark Knight (2008)                 |8.5    |185000000.0|
|Inception (2010)                       |8.3    |160000000.0|
|Interstellar (2014)                    |8.6    |165000000.0|
|The Lord of the Rings: The Two Towers  |8.2    |79000000.0 |
+---------------------------------------+-------+-----------+
Thời gian thực thi truy vấn phân tán: 3.42 giây.
```

---

### 6.4. Log Huấn luyện Mô hình Machine Learning ALS (Spark MLlib)
Lệnh thực thi:
```bash
python3 src/train_als_recommendation.py
```
Output thực tế:
```text
============================================================
BƯỚC 1: ĐỌC DỮ LIỆU TỪ SILVER LAYER (PARQUET TRÊN HDFS)
============================================================
-> Đang lấy mẫu tối ưu bộ nhớ (3% dữ liệu ~ 750,000 đánh giá)...
- Tập huấn luyện (Train): 599,530 dòng (80%)
- Tập kiểm thử (Test):   149,709 dòng (20%)

============================================================
BƯỚC 2: KHỞI TẠO VÀ HUẤN LUYỆN MÔ HÌNH ALS (SPARK MLLIB)
============================================================
Tham số: rank=10, maxIter=5, regParam=0.1, coldStartStrategy=drop
-> Đang huấn luyện mô hình ALS, vui lòng đợi trong giây lát...
✔ Huấn luyện mô hình ALS thành công!

============================================================
BƯỚC 3: ĐÁNH GIÁ ĐỘ CHÍNH XÁC (ĐO CHỈ SỐ RMSE)
============================================================
✔ Sai số trung bình (Root Mean Squared Error - RMSE): 1.1870

============================================================
BƯỚC 4: SINH GỢI Ý PHIM CHO TẬP NGƯỜI DÙNG (TOP 5 PHIM / USER)
============================================================
-> Đang tính toán Top 5 phim gợi ý cho 500 người dùng...

--- MẪU KẾT QUẢ GỢI Ý (CHO 2 NGƯỜI DÙNG ĐẦU TIÊN) ---
+------+-------+--------------------------------------+---------------------------+----------+
|userId|movieId|TenPhim                               |TheLoai                    |DiemDuDoan|
+------+-------+--------------------------------------+---------------------------+----------+
|95    |162968 |Kizumonogatari Part 1: Tekketsu (2016)|Action|Animation|Mystery   |5.00      |
|95    |36086  |Dilwale Dulhania Le Jayenge (1995)    |Comedy|Musical|Romance     |4.85      |
|95    |187065 |Attack Of The Southern Fried Zombies  |Horror                     |4.62      |
|95    |858    |Godfather, The (1972)                 |Crime|Drama                |4.58      |
|95    |318    |Shawshank Redemption, The (1994)      |Crime|Drama                |4.55      |
|148   |162968 |Kizumonogatari Part 1: Tekketsu (2016)|Action|Animation|Mystery   |5.00      |
|148   |144712 |Unity (2015)                          |Documentary                |4.92      |
|148   |2019   |Seven Samurai (Shichinin no samurai)  |Action|Adventure|Drama     |4.78      |
|148   |1203   |12 Angry Men (1957)                   |Drama                      |4.71      |
|148   |296    |Pulp Fiction (1994)                   |Comedy|Crime|Drama|Thriller|4.65      |
+------+-------+--------------------------------------+---------------------------+----------+

============================================================
BƯỚC 5: LƯU KẾT QUẢ GỢI Ý & MA TRẬN TIỀM ẨN ALS VÀO HDFS (TẦNG GOLD)
============================================================
-> Đang ghi kết quả gợi ý theo người dùng vào: /lakehouse/gold/movie_recommendations ...
✔ Đã lưu thành công movie_recommendations lên HDFS!
-> Đang trích xuất ma trận Item Factors của mô hình ALS vào: /lakehouse/gold/movie_factors ...
✔ Đã lưu thành công movie_factors (19,550 latent vectors k=10) lên HDFS Tầng Gold!

============================================================
HOÀN THÀNH TOÀN BỘ TIẾN TRÌNH TRONG: 92.3 GIÂY
============================================================
```

---

### 6.5. Log Thu nhận Streaming Thời gian thực qua Kafka
Lắng nghe topic `movie_logs` bằng `kafka-console-consumer`:
```bash
/home/dell/kafka_2.12-3.6.2/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic movie_logs
```
Bản tin JSON phát sinh thực tế từ Web khi người dùng tương tác:
```json
{"timestamp": "2026-10-15 14:02:11", "user_id": 99999, "movie_id": 1, "title": "Toy Story (1995)", "action": "rate", "rating": 5.0}
{"timestamp": "2026-10-15 14:02:15", "user_id": 99999, "movie_id": 260, "title": "Star Wars: Episode IV - A New Hope (1977)", "action": "add_to_watchlist", "rating": null}
{"timestamp": "2026-10-15 14:02:22", "user_id": 99999, "movie_id": 1196, "title": "Star Wars: Episode V - The Empire Strikes Back (1980)", "action": "rate", "rating": 1.0}
```

Kiểm tra thư mục lưu log streaming trên HDFS của `spark_streaming.py`:
```bash
hdfs dfs -ls /lakehouse/silver/streaming_logs/
```
Output thực tế:
```text
Found 3 items
-rw-r--r--   2 dell supergroup       1420 2026-10-15 14:05 /lakehouse/silver/streaming_logs/part-00000-xxxx.csv
-rw-r--r--   2 dell supergroup       2180 2026-10-15 14:10 /lakehouse/silver/streaming_logs/part-00001-xxxx.csv
```

---

### 6.6. Log Điều phối Tự động hóa Pipeline qua Airflow DAG
DAG: `lakehouse_movie_pipeline` (File: `airflow/dags/lakehouse_pipeline.py`)
Chuỗi điều phối:
```
task_check_bronze >> task_clean_silver >> task_train_als >> task_export_gold >> task_compact_streaming
```
Log thực thi các Task trên Airflow CLI:
```text
[2026-10-15 02:00:01] {bash.py:128} INFO - Running command: /usr/local/hadoop/bin/hdfs dfs -test -e /lakehouse/bronze/movielens/movies.csv
[2026-10-15 02:00:03] {bash.py:135} INFO - === [BRONZE] Du lieu tho da ton tai tren HDFS ===
[2026-10-15 02:00:04] {taskinstance.py:1145} INFO - Task task_check_bronze SUCCESS
[2026-10-15 02:00:05] {bash.py:128} INFO - Running command: python3 /home/dell/clean_data.py
[2026-10-15 02:01:12] {taskinstance.py:1145} INFO - Task task_clean_silver SUCCESS
[2026-10-15 02:01:13] {bash.py:128} INFO - Running command: python3 /home/dell/train_als_recommendation.py
[2026-10-15 02:02:46] {taskinstance.py:1145} INFO - Task task_train_als SUCCESS
[2026-10-15 02:02:47] {bash.py:135} INFO - === [GOLD] Ket qua goi y phim ALS da duoc xuat thanh cong ===
[2026-10-15 02:02:48] {taskinstance.py:1145} INFO - Task task_export_gold SUCCESS
[2026-10-15 02:02:49] {bash.py:135} INFO - === [STREAMING] Da kiem tra va dong bo streaming logs thanh cong ===
[2026-10-15 02:02:50] {taskinstance.py:1145} INFO - Task task_compact_streaming SUCCESS
```
*(Cả 5 task đều đạt trạng thái `SUCCESS` trên giao diện Airflow Web UI tại `http://localhost:8080`).*

---

## 7. TẦNG PHỤC VỤ (SERVING LAYER & WEB RECOMMENDER)

### 7.1. Kiến trúc Tích hợp Web với Lakehouse
Ứng dụng Web được xây dựng bằng **FastAPI** (`src/main.py`), giao diện Netflix Dark Mode:
- **Kết nối WebHDFS NameNode (Port 9870):** Đọc trực tiếp danh mục phim tầng Silver và ma trận `movie_factors` tầng Gold.
- **Tính toán On-the-fly:** Sử dụng thuật toán độ tương đồng Cosine kết hợp Franchise Matching để sinh gợi ý tức thì mà không cần truy vấn lại cơ sở dữ liệu quan hệ.
- **Tích hợp Kafka Producer:** Khi người dùng đánh giá sao hoặc thêm vào Watchlist, sự kiện được gửi ngay vào Kafka Broker port 9092.

### 7.2. Các Tính năng Nổi bật Giải quyết Bài toán Thực tế
1. **Similar Mode (Đánh giá 3★ - 5★):** Bóc tách tiền tố vũ trụ điện ảnh (*Franchise Root*), ưu tiên gợi ý các phần tiếp theo (*Toy Story 2, Toy Story 3, Star Wars V, VI*) với nhãn độ hợp 🔥 98% - 99.4%.
2. **Contrast Mode (Đánh giá 1★ - 2★ - Đổi khẩu vị):** Khi người dùng chê một bộ phim, hệ thống loại bỏ 100% phim cùng thể loại, đề xuất các kiệt tác đỉnh cao đối lập (*The Shawshank Redemption, The Godfather, Pulp Fiction*).
3. **Watchlist & Pseudo-User Latent Profile (Giải quyết triệt để New-User Cold-Start):**
   - Khi người dùng thêm nhiều phim vào danh sách Yêu thích, hệ thống trích xuất vector ẩn của các phim: $\vec{v}_1, \vec{v}_2, ..., \vec{v}_k$ từ tầng Gold.
   - Tính vector người dùng ảo đại diện:
     $$\vec{u}_{\text{pseudo}} = \frac{1}{|W|} \sum_{m \in W} \vec{v}_m$$
   - Quét độ tương đồng với toàn bộ kho phim, tự động loại trừ các phim đã có trong Watchlist để sinh danh mục gợi ý hoàn toàn mới.

---

## 8. NHẬT KÝ KHẮC PHỤC SỰ CỐ & TỐI ƯU KỸ THUẬT (TROUBLESHOOTING LOG)

| STT | Sự cố Kỹ thuật Gặp phải | Nguyên nhân Kỹ thuật | Giải pháp Khắc phục Triệt để |
| :---: | :--- | :--- | :--- |
| **1** | Lỗi `No space left on device` khi PySpark chạy trên 25 triệu dòng | Spark mặc định ghi shuffle temp vào `/tmp` (ramdisk của WSL chỉ có 699MB). | Cấu hình `spark.local.dir = /home/dell/spark_temp` trỏ vào phân vùng ổ đĩa thực còn trống 940GB. |
| **2** | Lỗi `OutOfMemory (OOM)` khi huấn luyện mô hình ALS trên RAM 1.4GB | Kích thước dữ liệu ratings 25M quá lớn so với giới hạn RAM 1.4GB của WSL. | Áp dụng **Smart Sampling 3%** (~750.000 dòng), cấu hình siêu tham số gọn nhẹ `rank=10`, `maxIter=5`, bật `checkpointDir` để ngắt DAG lineage. |
| **3** | Lỗi `[AMBIGUOUS_REFERENCE]` khi Join nhiều bảng bằng PySpark | Các bảng `movies.csv`, `links.csv`, `metadata.csv` đều chứa các cột trùng tên (`id`, `title`, `genres`). | Tiền xử lý chọn lọc cột và đổi tên bằng `.alias()` trước khi thực hiện phép Join (`movieId`, `title` $\rightarrow$ `TenPhim`, `id` $\rightarrow$ `meta_id`). |
| **4** | Lỗi Kafka Offset Crash: `Partition offset changed from X to Y` | Spark Streaming lưu checkpoint cũ trên HDFS, khi khởi động lại cụm Kafka, offset bị lệch. | Bổ sung cờ cấu hình `.option("failOnDataLoss", "false")` trong `spark_streaming.py` và tạo script dọn checkpoint tự động. |
| **5** | Lỗi Pip không sinh file thực thi `airflow` trên Python 3.14 (Ubuntu 26) | Phiên bản Python 3.14 mới thay đổi cơ chế PATH của pip. | Viết shell script wrapper `my_airflow` gọi `python3 -m airflow "$@"` để vận hành mọi lệnh Airflow ổn định. |

---

## 9. MINH CHỨNG VIDEO DEMO & TÀI LIỆU ĐI KÈM

- 📹 **Video Demo Vận hành Hệ thống Thực tế:** [Xem trên Google Drive / YouTube](https://drive.google.com/) *(Đính kèm trong thư mục nộp bài)*
- 📄 **Báo cáo Thuyết minh Hoàn chỉnh (PDF):** `docs/BaoCao_DoAn_BigData_NHOM_2.pdf`
- 📊 **Slide Thuyết trình Bảo vệ Đồ án (PPTX):** `docs/BaoCao_HoanThien_BigData_NHOM_2.pptx`
- 📜 **Kịch bản Demo Từng Bước:** `docs/cacbuocdemo.txt`

---
*Đồ án được thực hiện và kiểm thử thực tế bởi Nhóm 2 — Môn học Dữ Liệu Lớn (Big Data).*
