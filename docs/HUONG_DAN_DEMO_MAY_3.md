# HƯỚNG DẪN THAO TÁC DEMO — MÁY 3
## VAI TRÒ TRÊN SƠ ĐỒ: DATABASE SINK (HDFS Lakehouse), MACHINE LEARNING ALS & AIRFLOW PIPELINE

---

### I. CÁC BƯỚC CHUẨN BỊ TRƯỚC KHI BẮT ĐẦU (SETUP TRƯỚC 3 PHÚT)

1. **Khởi động dịch vụ HDFS & Airflow:**
   - Cụm Hadoop HDFS (NameNode + 2 DataNodes) đã chạy bình thường.
   - Airflow đã khởi động (chạy lệnh: `python3 -m airflow standalone`).
2. **Mở sẵn 2 Tab Trình duyệt:**
   - **Tab 1 (WebHDFS NameNode):** Mở `http://localhost:9870`.
   - **Tab 2 (Apache Airflow Web UI):** Mở `http://localhost:8080`, đăng nhập bằng:
     * Tài khoản: `admin`
     * Mật khẩu: `rQkxSVcQXRPZq7sZ` (hoặc mật khẩu hiển thị khi chạy standalone).
3. **Mở sẵn Terminal WSL:** Gõ sẵn lệnh sau nhưng **chưa bấm Enter**:
   ```bash
   python3 query_movies.py
   ```

---

### II. THỨ TỰ CÁC THAO TÁC THỰC HIỆN TRONG KHI DEMO

#### BƯỚC 1: DEMO DATABASE SINK (HẠ TẦNG PHÂN TÁN HDFS 2 DATANODES)
* **Thời gian thực hiện:** ~40 giây.
* **Màn hình thao tác:** Trình duyệt Tab 1 (WebHDFS `http://localhost:9870`).
* **Các thao tác cụ thể:**
  1. Tại trang chủ **Overview**:
     * Chỉ chuột vào dòng chữ: **Live Nodes = 2** (chứng minh cụm HDFS có 2 DataNode vật lý hoạt động song song, đảm bảo khả năng chịu lỗi với Replication = 2).
  2. Trên menu trên cùng, nhấp vào **Utilities** $\rightarrow$ chọn **Browse the file system**:
     * Gõ đường dẫn: `/lakehouse/silver/streaming_logs/` và bấm Enter.
     * Chỉ cho Thầy/Cô thấy các tệp log streaming (CSV/Parquet) vừa được Spark Streaming từ Máy 2 ghi xuống tầng Silver.
  3. Gõ chuyển sang đường dẫn: `/lakehouse/silver/ratings/`:
     * Nhấn mạnh: 25 triệu dòng đánh giá đã được nén sang chuẩn Snappy Parquet dạng cột, giảm dung lượng từ 646.8 MB xuống còn 195.4 MB (tiết kiệm 69.8% dung lượng đĩa).

---

#### BƯỚC 2: DEMO TẦNG GOLD (MACHINE LEARNING ALS) & TRUY VẤN SPARK SQL
* **Thời gian thực hiện:** ~40 giây.
* **Màn hình thao tác:** WebHDFS và Cửa sổ Terminal WSL.
* **Các thao tác cụ thể:**
  1. Trên WebHDFS, gõ đường dẫn: `/lakehouse/gold/`:
     * Chỉ vào thư mục `movie_factors/`: Giải thích đây là nơi lưu **19.550 vector đặc trưng tiềm ẩn** ($k=10$) được trích xuất từ mô hình Spark MLlib ALS.
     * Chỉ vào thư mục `movie_recommendations/`: Chứa danh sách Top 5 phim gợi ý cho từng người dùng.
  2. Chuyển sang cửa sổ **Terminal WSL**, nhấn phím **Enter** để chạy lệnh truy vấn đã chuẩn bị sẵn:
     ```bash
     python3 query_movies.py
     ```
  3. Quan sát và chỉ vào bảng kết quả phân tích Top 5 phim kinh điển đầu tư lớn (điểm $\ge 8.0$, kinh phí $> 10$ triệu USD) xuất hiện trên terminal trong vòng 3 giây:
     ```text
     +---------------------------------------+-------+-----------+
     |TenPhim                                |DiemSo |KinhPhi    |
     +---------------------------------------+-------+-----------+
     |Titanic (1997)                         |8.1    |200000000.0|
     |The Dark Knight (2008)                 |8.5    |185000000.0|
     |Inception (2010)                       |8.3    |160000000.0|
     ...
     ```

---

#### BƯỚC 3: DEMO ĐIỀU PHỐI TỰ ĐỘNG HÓA PIPELINE BẰNG APACHE AIRFLOW
* **Thời gian thực hiện:** ~40 giây.
* **Màn hình thao tác:** Trình duyệt Tab 2 (Apache Airflow `http://localhost:8080`).
* **Các thao tác cụ thể:**
  1. Trên thanh tìm kiếm của Airflow, tìm và nhấp chọn DAG: `lakehouse_movie_pipeline`.
  2. Bấm vào tab **Graph View**:
     * Chỉ chuột vào chuỗi 5 Task liên hoàn tuần tự từ trái qua phải:
       $$\text{task\_check\_bronze} \rightarrow \text{task\_clean\_silver} \rightarrow \text{task\_train\_als} \rightarrow \text{task\_export\_gold} \rightarrow \text{task\_compact\_streaming}$$
  3. Bấm vào nút **Trigger DAG** (Nút Play hình tam giác ở góc trên bên phải):
     * Quan sát các ô Task chuyển trạng thái từ màu xám sang màu xanh lá cây (`Success`).
  4. Nhấp vào task `task_train_als` $\rightarrow$ chọn mục **Logs**:
     * Kéo xuống chỉ vào dòng log: `Train: 599,530 rows`, `Test: 149,709 rows`, `RMSE: 1.1870` chứng minh mô hình Machine Learning ALS đã được huấn luyện tự động thành công.
  5. Cúi chào Thầy/Cô và kết thúc bài demo của toàn nhóm.
