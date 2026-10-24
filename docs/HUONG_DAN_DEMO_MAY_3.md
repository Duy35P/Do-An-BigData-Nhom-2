# HƯỚNG DẪN THAO TÁC DEMO ĐỘC LẬP — MÁY 3
## VAI TRÒ TRÊN SƠ ĐỒ: DATABASE SINK (HDFS Lakehouse), MACHINE LEARNING ALS & AIRFLOW PIPELINE
> *(Máy này hoạt động hoàn toàn độc lập, có thể demo bất cứ lúc nào giảng viên gọi tên, không phụ thuộc vào Máy 1 hay Máy 2).*

---

### I. CÔNG VIỆC CHUẨN BỊ (TRƯỚC KHI GIẢNG VIÊN GỌI)

1. **Khởi động dịch vụ HDFS & Airflow:**
   * Cụm Hadoop HDFS (NameNode + 2 DataNodes) đã chạy bình thường (`$HADOOP_HOME/sbin/start-dfs.sh`).
   * Airflow đã bật (chạy lệnh: `python3 -m airflow standalone`).
2. **Mở sẵn 2 Tab Trình duyệt:**
   * **Tab 1 (WebHDFS NameNode):** Mở `http://localhost:9870`.
   * **Tab 2 (Apache Airflow Web UI):** Mở `http://localhost:8080`, đăng nhập bằng tài khoản: `admin` (mật khẩu: `rQkxSVcQXRPZq7sZ` hoặc mật khẩu in ra khi chạy standalone).
3. **Mở sẵn Terminal WSL:** Gõ sẵn lệnh sau nhưng **chưa bấm Enter**:
   ```bash
   python3 query_movies.py
   ```

---

### II. CÁC BƯỚC THAO TÁC THỰC HIỆN KHI ĐẾN LƯỢT DEMO

#### BƯỚC 1: DEMO DATABASE SINK (HẠ TẦNG LƯU TRỮ PHÂN TÁN HDFS 2 DATANODES)
* **Màn hình:** Trình duyệt Tab 1 (WebHDFS `http://localhost:9870`).
* **Thao tác thực hiện:**
  1. Tại trang chủ **Overview**:
     * Chỉ chuột vào dòng chữ: **Live Nodes = 2**.
     * *Giải thích:* Hệ thống triển khai 2 DataNode vật lý hoạt động độc lập, cấu hình Replication Factor = 2 để dữ liệu được nhân bản song song, đảm bảo khả năng chịu lỗi (Fault Tolerance).
  2. Bấm vào menu **Utilities** (ở góc trên) $\rightarrow$ chọn **Browse the file system**:
     * Gõ đường dẫn: `/lakehouse/silver/streaming_logs/` và bấm Enter.
     * *Chỉ vào màn hình:* Đây là nơi nhóm **Consumer Group thứ hai (Database Sink)** lưu trữ các gói dữ liệu từ luồng streaming xuống HDFS để phục vụ tra cứu và kiểm toán.
  3. Gõ chuyển sang đường dẫn: `/lakehouse/silver/ratings/`:
     * *Giải thích:* Toàn bộ 25 triệu dòng đánh giá thô (646.8 MB CSV) sau khi qua PySpark làm sạch đã được nén sang định dạng cột Snappy Parquet chỉ còn 195.4 MB (tiết kiệm 69.8% dung lượng đĩa và tối ưu hóa I/O đọc).

---

#### BƯỚC 2: DEMO TẦNG GOLD (MACHINE LEARNING ALS) & TRUY VẤN SPARK SQL
* **Màn hình:** WebHDFS và Cửa sổ Terminal WSL.
* **Thao tác thực hiện:**
  1. Trên WebHDFS, gõ đường dẫn: `/lakehouse/gold/`:
     * Chỉ vào thư mục `movie_factors/`: Giải thích đây là nơi lưu **19.550 vector đặc trưng tiềm ẩn** ($k=10$) từ thuật toán lọc cộng tác Spark MLlib ALS.
     * Chỉ vào thư mục `movie_recommendations/`: Chứa danh sách Top 5 phim gợi ý đã được sinh sẵn.
  2. Chuyển sang cửa sổ **Terminal WSL**, nhấn phím **Enter** để chạy lệnh truy vấn Spark SQL:
     ```bash
     python3 query_movies.py
     ```
  3. Chỉ tay vào bảng kết quả phân tích Top 5 tác phẩm kinh điển kinh phí lớn xuất hiện trên terminal trong 3 giây:
     ```text
     +---------------------------------------+-------+-----------+
     |TenPhim                                |DiemSo |KinhPhi    |
     +---------------------------------------+-------+-----------+
     |Titanic (1997)                         |8.1    |200000000.0|
     |The Dark Knight (2008)                 |8.5    |185000000.0|
     |Inception (2010)                       |8.3    |160000000.0|
     |Interstellar (2014)                    |8.6    |165000000.0|
     |The Lord of the Rings: The Two Towers  |8.2    |79000000.0 |
     +---------------------------------------+-------+-----------+
     ```

---

#### BƯỚC 3: DEMO ĐIỀU PHỐI TỰ ĐỘNG HÓA PIPELINE BẰNG APACHE AIRFLOW
* **Màn hình:** Trình duyệt Tab 2 (Apache Airflow `http://localhost:8080`).
* **Thao tác thực hiện:**
  1. Trên danh sách DAGs, nhấp chọn DAG: `lakehouse_movie_pipeline`.
  2. Bấm vào tab **Graph View**:
     * Chỉ vào chuỗi 5 Task liên hoàn tuần tự từ trái qua phải:
       $$\text{task\_check\_bronze} \rightarrow \text{task\_clean\_silver} \rightarrow \text{task\_train\_als} \rightarrow \text{task\_export\_gold} \rightarrow \text{task\_compact\_streaming}$$
     * *Giải thích:* Đây là bộ điều phối trung tâm (Workflow Orchestrator) lên lịch chạy tự động định kỳ mỗi ngày.
  3. Bấm vào nút **Trigger DAG** (Nút Play hình tam giác ở góc trên bên phải):
     * Quan sát các ô Task chuyển trạng thái sang màu xanh lá cây (`Success`).
  4. Nhấp chuột vào task `task_train_als` $\rightarrow$ chọn mục **Logs**:
     * Kéo xuống chỉ vào kết quả đo lường: `Train: 599,530 rows`, `Test: 149,709 rows`, `RMSE: 1.1870` chứng minh mô hình Machine Learning ALS đã được huấn luyện tự động thành công.

---

### III. CÂU HỎI GIẢNG VIÊN HAY HỎI MÁY 3 & CÁCH TRẢ LỜI
* **Hỏi:** *Tại sao lại chọn mô hình ALS (Alternating Least Squares)? Sai số RMSE là bao nhiêu?*  
  $\rightarrow$ **Trả lời:** Dạ ALS là thuật toán phân rã ma trận tối ưu riêng cho ma trận thưa User-Item trong các bài toán Big Data. Mô hình đạt sai số RMSE = 1.1870 trên thang điểm 1-5, huấn luyện phân tán chỉ mất 92 giây nhờ kỹ thuật Smart Sampling 3% và cấu hình `rank=10`, `maxIter=5` tránh tràn RAM.
* **Hỏi:** *Airflow giải quyết vấn đề gì trong kiến trúc này?*  
  $\rightarrow$ **Trả lời:** Dạ Airflow đóng vai trò điều phối luồng dữ liệu mẻ (Batch Orchestration), đảm bảo các tác vụ phụ thuộc nghiêm ngặt: Phải kiểm tra Bronze có dữ liệu $\rightarrow$ mới làm sạch ra Silver $\rightarrow$ làm sạch thành công mới huấn luyện ALS ra Gold $\rightarrow$ và tự động đồng bộ lại log streaming.
