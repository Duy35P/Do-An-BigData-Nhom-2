# HƯỚNG DẪN THAO TÁC DEMO — MÁY 2
## VAI TRÒ TRÊN SƠ ĐỒ: KAFKA CLUSTER & CONSUMER GROUP 1 (Spark Realtime)

---

### I. CÁC BƯỚC CHUẨN BỊ TRƯỚC KHI BẮT ĐẦU (SETUP TRƯỚC 3 PHÚT)

1. **Khởi động Kafka Broker:**
   - Đảm bảo Kafka 3.6.2 (chế độ KRaft) đã bật tại cổng `localhost:9092`.
2. **Chia đôi màn hình thành 2 nửa (Split Screen):**
   - **Nửa màn hình bên trái (Terminal 1 - Kafka Consumer):** Mở sẵn cửa sổ chạy lệnh lắng nghe trực tiếp topic `movie_logs`:
     ```bash
     /home/dell/kafka_2.12-3.6.2/bin/kafka-console-consumer.sh \
       --bootstrap-server localhost:9092 \
       --topic movie_logs
     ```
     *(Cửa sổ này sẽ ở trạng thái chờ sẵn sự kiện).*
   - **Nửa màn hình bên phải (Terminal 2 - Spark Streaming):** Chạy sẵn tiến trình Spark Structured Streaming:
     ```bash
     python3 src/spark_streaming.py
     ```
3. **Mở sẵn trình duyệt Web:** Mở `http://localhost:8000`, chuyển chuột sẵn sang tab **"📡 Streaming & Kafka Logs"**.

---

### II. THỨ TỰ CÁC THAO TÁC THỰC HIỆN TRONG KHI DEMO

#### BƯỚC 1: DEMO TIẾP NHẬN SỰ KIỆN TỨC THỜI TẠI KAFKA BROKER (KAFKA CLUSTER)
* **Thời gian thực hiện:** ~30 giây.
* **Màn hình thao tác:** Cửa sổ Terminal 1 (Kafka Console Consumer ở nửa trái).
* **Các thao tác cụ thể:**
  1. Ngay khi Người 1 (Máy 1) vừa dứt lời hoặc khi trên máy mình có sự kiện phát sinh, chỉ tay ngay vào các dòng JSON đang xuất hiện:
     ```json
     {"timestamp": "2026-10-...", "user_id": 99999, "movie_id": 1, "action": "rate", "rating": 5.0}
     ```
  2. Chỉ vào dòng lệnh: Nhấn mạnh cổng kết nối `localhost:9092` và topic `movie_logs`.
  3. Giải thích ngắn gọn bằng cử chỉ: Kafka hoạt động như một vùng đệm phân tán (Retained event log), giúp tách biệt hoàn toàn giữa phía sinh dữ liệu và phía xử lý dữ liệu.

---

#### BƯỚC 2: DEMO XỬ LÝ DÒNG THỜI GIAN THỰC (SPARK STRUCTURED STREAMING)
* **Thời gian thực hiện:** ~35 giây.
* **Màn hình thao tác:** Cửa sổ Terminal 2 (Spark Streaming ở nửa phải).
* **Các thao tác cụ thể:**
  1. Chỉ vào cửa sổ đang chạy `python3 src/spark_streaming.py`.
  2. Mở nhanh file code `src/spark_streaming.py` trên màn hình và chỉ vào 3 vị trí kỹ thuật:
     * **Đọc dòng:** `spark.readStream.format("kafka").option("subscribe", "movie_logs")`
     * **Chống lệch offset:** `.option("failOnDataLoss", "false")`
     * **Sink dữ liệu:** `df_parsed.writeStream.outputMode("append").path("hdfs://localhost:9000/lakehouse/silver/streaming_logs/")`
  3. Nhấn mạnh: Spark Streaming liên tục đọc các micro-batch, bóc tách JSON và chuyển tiếp một luồng xuống HDFS.

---

#### BƯỚC 3: DEMO BẢNG ĐIỀU KHIỂN STREAMING & CHẾ ĐỘ PHẢN HỒI TỨC THÌ (CONTRAST MODE)
* **Thời gian thực hiện:** ~35 giây.
* **Màn hình thao tác:** Trình duyệt Web (`http://localhost:8000`).
* **Các thao tác cụ thể:**
  1. Trên trình duyệt, bấm vào tab **"📡 Streaming & Kafka Logs"**:
     * Chỉ vào các chỉ số: Số lượng sự kiện tiếp nhận, tỷ lệ hành động (`rate`, `view`, `click`) đang được tổng hợp thời gian thực.
  2. Quay lại tab **"🎬 Kho Phim"**, tìm một phim bất kỳ (ví dụ: *Toy Story* hoặc *Star Wars*).
  3. Bấm đổi điểm đánh giá sang **1★**:
     * Quan sát ngay danh sách phim gợi ý bên dưới đổi 100%: Hệ thống kích hoạt **Contrast Mode (Đổi khẩu vị)**, tự động loại bỏ phim bị chê và đề xuất các kiệt tác thể loại đối lập (*The Shawshank Redemption*, *The Godfather*, *Pulp Fiction*).
  4. Hướng tay về phía **Máy 3** để chuyển giao sang phần demo lưu trữ HDFS và điều phối Airflow.
