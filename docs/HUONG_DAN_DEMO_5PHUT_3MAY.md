# KỊCH BẢN & HƯỚNG DẪN DEMO BẢO VỆ ĐỒ ÁN BIG DATA (5 PHÚT)
## DÀNH CHO 3 THÀNH VIÊN TRÊN 3 MÁY ĐỘC LẬP (KHÔNG CẦN NỐI MẠNG LAN)

> **Mô hình kiến trúc ánh xạ theo sơ đồ:**  
> - **MÁY 1 (Người 1):** Phụ trách nhánh **DATA PRODUCERS** *(Web Netflix Serving & Producer Simulator)*  
> - **MÁY 2 (Người 2):** Phụ trách khối **KAFKA CLUSTER & SPARK REALTIME** *(Kafka KRaft Broker & Spark Structured Streaming)*  
> - **MÁY 3 (Người 3):** Phụ trách khối **DATABASE SINK (HDFS) & AIRFLOW PIPELINE** *(Cụm phân tán HDFS, Spark MLlib ALS & Airflow DAG)*  

---

## 📌 PHẦN I: BẢNG TỔNG QUAN PHÂN VAI & CHUẨN BỊ MÀN HÌNH

Trước khi lên bàn thuyết trình hoặc mở máy tính trước mặt Thầy/Cô, **mỗi người chỉ cần mở sẵn các cửa sổ sau trên máy mình**:

| Máy / Thành viên | Nhiệm vụ chính trên sơ đồ | Màn hình chuẩn bị sẵn trước khi bấm giờ | Lệnh / Đường dẫn chuẩn bị |
| :---: | :--- | :--- | :--- |
| **MÁY 1**<br>*(Người 1)* | **DATA PRODUCERS**<br>• Giao diện Web Client<br>• Bộ sinh sự kiện JSON | **1.** Trình duyệt: Giao diện Web Netflix<br>**2.** Terminal: Chuẩn bị sẵn lệnh chạy `producer.py` | • Web: `http://localhost:8000`<br>• Terminal: `python3 producer.py` (để sẵn, chưa Enter) |
| **MÁY 2**<br>*(Người 2)* | **KAFKA CLUSTER & SPARK REALTIME**<br>• Kafka KRaft Broker<br>• Spark Structured Streaming | **1.** Terminal trái: Đang chạy Kafka Consumer lắng nghe<br>**2.** Terminal phải: Đang chạy `spark_streaming.py`<br>**3.** Trình duyệt: Tab *Streaming & Kafka Logs* | • Kafka listen: `bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic movie_logs`<br>• Spark Streaming: `python3 spark_streaming.py` |
| **MÁY 3**<br>*(Người 3)* | **DATABASE SINK & ORCHESTRATION**<br>• Lưu trữ HDFS 2 DataNodes<br>• Machine Learning ALS Tầng Gold<br>• Điều phối Airflow DAG | **1.** Trình duyệt Tab 1: WebHDFS NameNode (:9870)<br>**2.** Trình duyệt Tab 2: Apache Airflow (:8080)<br>**3.** Terminal: Chuẩn bị lệnh chạy `query_movies.py` | • WebHDFS: `http://localhost:9870`<br>• Airflow: `http://localhost:8080` (admin/rQkxSVcQXRPZq7sZ)<br>• Terminal: `python3 query_movies.py` |

---

## 📌 PHẦN II: KỊCH BẢN CHI TIẾT 5 PHÚT (TỪNG GIÂY & LỜI THOẠI)

```
[00:00 - 01:20] MÁY 1: Nguồn sinh dữ liệu (Web Client & Producer Event)
                      │
                      ▼ (Chuyển giao mượt mà)
[01:20 - 03:00] MÁY 2: Trục xương sống Kafka & Xử lý thời gian thực Spark
                      │
                      ▼ (Chuyển giao mượt mà)
[03:00 - 05:00] MÁY 3: Kho lưu trữ HDFS Sink, Học máy ALS & Điều phối Airflow
```

---

### ⏱️ GIAI ĐOẠN 1: MÁY 1 (NGƯỜI 1) — TỪ 00:00 ĐẾN 01:20 (1 Phút 20 Giây)
**Trọng tâm:** Chứng minh nhánh **DATA PRODUCERS** đang hoạt động thực tế.

#### 🎯 Thao tác trên màn hình Máy 1:
1. Mở cửa sổ Web `http://localhost:8000`, gõ tìm phim: **"Toy Story (1995)"**.
2. Nhấp chuột vào **5★** để đánh giá phim.
3. Nhấp nút **"❤️ Yêu thích"** (Watchlist) để thêm phim vào danh sách quan tâm.
4. Chuyển sang cửa sổ Terminal, nhấn Enter để chạy:
   ```bash
   python3 producer.py
   ```
   *(Để terminal hiển thị 3-4 dòng log JSON bắn ra liên tục rồi dừng lại hoặc để chạy ngầm).*

#### 🎙️ Lời thoại của Người 1 (Đọc tự tin, dõng dạc):
> *"Kính thưa Thầy/Cô, nhóm chúng em xây dựng Hệ thống Gợi ý Phim trực tuyến mô phỏng kiến trúc Big Data thời gian thực.  
> Máy tính thứ nhất của em đóng vai trò là **nhánh Data Producers** bên trái mô hình kiến trúc.  
> Tại đây, các tương tác của người dùng cuối từ Web Serving hoặc thiết bị di động đều được số hóa thành các sự kiện phân tán. Khi em vừa đánh giá 5 sao cho phim Toy Story và thêm vào mục Yêu thích, hàm `send_kafka_event` trên máy em đã lập tức tuần tự hóa hành động này thành bản tin chuẩn JSON gồm `timestamp`, `user_id`, `movie_id`, `action: rate` và `rating: 5.0`.  
> Đồng thời, script `producer.py` đang giả lập hàng nghìn lượt tương tác từ các ứng dụng Client khác bắn liên tục vào hệ thống.  
> Ngay sau đây, xin mời Thầy/Cô nhìn sang màn hình Máy 2 của bạn [Tên Người 2] để xem luồng sự kiện này được Kafka tiếp nhận và xử lý streaming ra sao."*

---

### ⏱️ GIAI ĐOẠN 2: MÁY 2 (NGƯỜI 2) — TỪ 01:20 ĐẾN 03:00 (1 Phút 40 Giây)
**Trọng tâm:** Chứng minh khối trung tâm **KAFKA CLUSTER** và nhánh **SPARK REALTIME**.

#### 🎯 Thao tác trên màn hình Máy 2:
1. **Chỉ vào Terminal Kafka:** Chỉ tay vào màn hình Kafka Console Consumer đang chạy (nơi các bản tin JSON vừa xuất hiện):
   ```json
   {"timestamp": "2026-10-...", "user_id": 99999, "movie_id": 1, "action": "rate", "rating": 5.0}
   ```
2. **Chỉ vào Terminal Spark Streaming:** Cho thấy tiến trình `spark_streaming.py` đang liên tục nhận các micro-batch với độ trễ tính bằng mili-giây.
3. **Mở Web Serving trên Máy 2:**
   * Bấm vào tab **"📡 Streaming & Kafka Logs"** $\rightarrow$ bảng thống kê sự kiện đang tự động nhảy số.
   * Thao tác đổi đánh giá sang **1★** $\rightarrow$ Hệ thống lập tức kích hoạt **Contrast Mode (Đổi khẩu vị)**, tự động loại bỏ phim hoạt hình và gợi ý sang các kiệt tác đỉnh cao thể loại khác (*The Shawshank Redemption*, *The Godfather*).

#### 🎙️ Lời thoại của Người 2:
> *"Tiếp nối luồng dữ liệu từ nhánh Producer, màn hình Máy 2 của em đảm nhận khối trung tâm **Kafka Cluster Broker** và nhánh **Spark Realtime Consumer Group**.  
> Hệ thống sử dụng Apache Kafka phiên bản 3.6.2 chế độ KRaft vận hành tại cổng 9092, topic `movie_logs`. Mọi bản tin JSON phát sinh từ người dùng đều được ghi nhận vào hàng đợi với cơ chế phân tách hoàn toàn giữa bên gửi và bên nhận.  
> Ở nửa màn hình bên phải, chương trình `spark_streaming.py` thuộc nhóm Consumer thứ nhất đang sử dụng Spark Structured Streaming để hứng các luồng micro-batch, bóc tách JSON và cập nhật trạng thái lên bảng điều khiển thời gian thực.  
> Đặc biệt, engine gợi ý tức thời phản hồi không độ trễ: khi người dùng chê 1 sao, hệ thống kích hoạt chế độ Đối lập Contrast Mode để đổi khẩu vị ngay lập tức.  
> Đồng thời, Spark Streaming cũng đảm nhận việc chuyển tiếp các gói dữ liệu này sang Consumer Group thứ hai để lưu trữ lâu dài. Xin mời Thầy/Cô hướng mắt về Máy 3 của bạn [Tên Người 3]."*

---

### ⏱️ GIAI ĐOẠN 3: MÁY 3 (NGƯỜI 3) — TỪ 03:00 ĐẾN 05:00 (2 Phút)
**Trọng tâm:** Chứng minh khối **DATABASE SINK (HDFS Lakehouse)**, **Spark MLlib ALS** và **Điều phối Airflow**.

#### 🎯 Thao tác trên màn hình Máy 3:
1. **Trình diễn HDFS Sink (Lưu trữ bền vững):**
   * Mở tab WebHDFS: `http://localhost:9870`, chỉ vào dòng **Live Nodes = 2** (chứng minh cụm lưu trữ phân tán 2 DataNode thật).
   * Vào `Utilities -> Browse the file system` $\rightarrow$ duyệt vào thư mục `/lakehouse/silver/streaming_logs/` (cho thấy các file dữ liệu streaming đã được ghi thành công xuống HDFS).
2. **Trình diễn Tầng Gold & Machine Learning:**
   * Mở thư mục `/lakehouse/gold/movie_factors/` $\rightarrow$ Giải thích đây là nơi lưu 19.550 vector đặc trưng tiềm ẩn ($k=10$) từ mô hình Spark MLlib ALS.
   * Chạy nhanh lệnh truy vấn trong terminal:
     ```bash
     python3 query_movies.py
     ```
     *(Bảng Top 5 phim hay đầu tư lớn bằng Spark SQL hiện ra trong 3 giây).*
3. **Trình diễn Điều phối Airflow DAG:**
   * Mở tab Apache Airflow: `http://localhost:8080`, mở DAG `lakehouse_movie_pipeline`.
   * Chỉ vào sơ đồ 5 task liên hoàn: `task_check_bronze >> task_clean_silver >> task_train_als >> task_export_gold >> task_compact_streaming`.
   * Bấm nút **Trigger DAG** $\rightarrow$ Các task đổi trạng thái màu xanh lá cây (`Success`).

#### 🎙️ Lời thoại của Người 3 (Tổng kết bài báo cáo):
> *"Kính thưa Thầy/Cô, Máy 3 của em đóng vai trò là **Trung tâm Dữ liệu Lakehouse & Bộ điều phối Orchestration**.  
> Ở Consumer Group thứ hai mang tên Database Sink, toàn bộ dữ liệu streaming từ Máy 2 đã được Spark Structured Streaming lưu trữ an toàn xuống HDFS tầng Silver tại `/lakehouse/silver/streaming_logs/`. Cụm HDFS của chúng em gồm 2 DataNodes phân tán chịu lỗi với Replication Factor = 2.  
> Dữ liệu streaming mới này được định kỳ kết hợp cùng 25 triệu đánh giá lịch sử để huấn luyện lại mô hình Lọc cộng tác Spark MLlib ALS. Kết quả huấn luyện là ma trận nhân tử ẩn `movie_factors` gồm 19.550 vector 10 chiều được lưu trữ tại tầng Gold, làm nguồn tri thức phục vụ gợi ý cá nhân hóa và giải quyết bài toán Cold-Start.  
> Toàn bộ quy trình từ kiểm tra dữ liệu thô Bronze, làm sạch Silver Parquet, huấn luyện ALS Gold đến đồng bộ Streaming Logs được điều phối tự động hoàn toàn bằng Apache Airflow DAG hàng ngày.  
> Thông qua sự phối hợp nhịp nhàng giữa 3 máy trạm, nhóm chúng em đã chứng minh trọn vẹn vòng đời xử lý dữ liệu lớn theo đúng mô hình kiến trúc chuẩn doanh nghiệp.  
> Nhóm em xin chân thành cảm ơn Thầy/Cô đã theo dõi và lắng nghe!"*

---

## 📌 PHẦN III: 4 MẸO "DIỄN" PHỐI HỢP ĂN Ý GIỮA 3 NGƯỜI

1. **Khớp nhịp "Bấm ở đây - Nhìn sang kia":**
   * Mặc dù 3 máy chạy độc lập trên localhost của mỗi máy, nhưng khi Người 1 vừa bấm rate ở Máy 1, Người 1 nói: *"Em vừa gửi sự kiện..."* $\rightarrow$ Người 2 **lập tức chỉ tay vào màn hình Máy 2** và nói: *"Và trên màn hình của em, Kafka vừa tiếp nhận được ngay lập tức!"*.  
   * Giảng viên sẽ thấy sự liền mạch và nhịp độ demo chuyên nghiệp như một hệ sinh thái phân tán thực thụ.
2. **Thứ tự đứng đúng sơ đồ:**
   * **[Người 1 - Máy 1]** đứng bên trái $\rightarrow$ **[Người 2 - Máy 2]** đứng ở giữa $\rightarrow$ **[Người 3 - Máy 3]** đứng bên phải.
   * Thứ tự này trùng khớp 100% từ trái qua phải của hình ảnh kiến trúc.
3. **Phân chia trả lời câu hỏi:**
   * Thầy/Cô hỏi về Web, Client, dữ liệu MovieLens $\rightarrow$ **Người 1** trả lời.
   * Thầy/Cô hỏi về Kafka KRaft, Spark Streaming, Schema JSON, Offset $\rightarrow$ **Người 2** trả lời.
   * Thầy/Cô hỏi về HDFS 2 DataNodes, MLlib ALS (RMSE), Parquet, Airflow DAG $\rightarrow$ **Người 3** trả lời.
4. **Chuẩn bị file tài liệu phòng hờ:**
   * Mỗi máy đều lưu sẵn file PDF báo cáo và link GitHub `https://github.com/Duy35P/Do-An-BigData-Nhom-2` để khi Giảng viên yêu cầu xem mã nguồn file nào là mở ra được ngay lập tức!
