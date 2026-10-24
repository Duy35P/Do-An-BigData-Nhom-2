# HƯỚNG DẪN THAO TÁC DEMO ĐỘC LẬP — MÁY 2
## VAI TRÒ TRÊN SƠ ĐỒ: KAFKA CLUSTER & CONSUMER GROUP 1 (Spark Realtime)
> *(Máy này hoạt động hoàn toàn độc lập, có thể demo bất cứ lúc nào giảng viên gọi tên, không phụ thuộc vào Máy 1 hay Máy 3).*

---

### I. CÔNG VIỆC CHUẨN BỊ (TRƯỚC KHI GIẢNG VIÊN GỌI)

1. **Khởi động dịch vụ:**
   * Bật Kafka Broker KRaft (nếu chưa chạy: `bin/kafka-server-start.sh -daemon config/kraft/server.properties`).
   * Bật Web Server trên máy mình: `python3 -m uvicorn src.main:app --host 0.0.0.0 --port 8000`.
2. **Chia màn hình làm 3 cửa sổ sẵn sàng:**
   * **Cửa sổ 1 (Terminal trái - Kafka Consumer):** Đang chạy lệnh chờ sự kiện:
     ```bash
     /home/dell/kafka_2.12-3.6.2/bin/kafka-console-consumer.sh \
       --bootstrap-server localhost:9092 \
       --topic movie_logs
     ```
   * **Cửa sổ 2 (Terminal phải - Spark Streaming):** Chạy sẵn tiến trình Spark Streaming:
     ```bash
     python3 src/spark_streaming.py
     ```
   * **Cửa sổ 3 (Trình duyệt Web):** Mở `http://localhost:8000`, chuyển chuột sẵn sang tab **"📡 Streaming & Kafka Logs"**.

---

### II. CÁC BƯỚC THAO TÁC THỰC HIỆN KHI ĐẾN LƯỢT DEMO

#### BƯỚC 1: TỰ KÍCH HOẠT VÀ DEMO KAFKA CLUSTER BROKER
* **Màn hình:** Mở Web (`http://localhost:8000`) cạnh Cửa sổ Terminal 1 (Kafka Consumer).
* **Thao tác thực hiện:**
  1. Trên Web, tìm một bộ phim bất kỳ (ví dụ: *Toy Story* hoặc *The Matrix*).
  2. Bấm chấm điểm **5★**.
  3. **Lập tức nhìn sang Terminal 1:** Chỉ tay vào dòng JSON vừa nhảy ra tức thì:
     ```json
     {"timestamp": "2026-10-...", "user_id": 99999, "movie_id": 1, "action": "rate", "rating": 5.0}
     ```
* **Chỉ vào màn hình:**
  * Giải thích: Khối trung tâm **Kafka Cluster** chạy chế độ KRaft tại cổng `9092`, quản lý topic `movie_logs`. Mọi bản tin được đưa vào hàng đợi với độ trễ dưới 50ms, đóng vai trò vùng đệm (retained event log) chịu tải cực lớn.

---

#### BƯỚC 2: DEMO TIẾN TRÌNH SPARK STRUCTURED STREAMING (CONSUMER GROUP 1)
* **Màn hình:** Cửa sổ Terminal 2 (Spark Streaming) và file code `src/spark_streaming.py`.
* **Thao tác thực hiện:**
  1. Chỉ vào Terminal 2 đang chạy tiến trình `spark_streaming.py`: Cho thấy Spark liên tục xử lý các micro-batch theo thời gian thực mà không bị gián đoạn.
  2. Mở nhanh file code `src/spark_streaming.py` chỉ vào các khối lệnh then chốt:
     * **Đọc Stream:** `spark.readStream.format("kafka").option("subscribe", "movie_logs")`
     * **Bóc tách Schema JSON:** `from_json(col("value"), schema)`
     * **Ghi Sink xuống HDFS:** `df_parsed.writeStream.outputMode("append").path("hdfs://localhost:9000/lakehouse/silver/streaming_logs/")`
     * **Cấu hình chống lỗi lệch offset:** `.option("failOnDataLoss", "false")`
* **Chỉ vào màn hình:**
  * Giải thích: Spark Streaming đại diện cho **Consumer Group thứ nhất (Spark Realtime)**, vừa bóc tách dữ liệu để cảnh báo/thống kê tức thời, vừa chuyển tiếp một luồng dữ liệu xuống kho HDFS.

---

#### BƯỚC 3: DEMO GIÁM SÁT REALTIME VÀ CHẾ ĐỘ PHẢN HỒI GỢI Ý TỨC THÌ (CONTRAST MODE)
* **Màn hình:** Trình duyệt Web (`http://localhost:8000`).
* **Thao tác thực hiện:**
  1. Chuyển sang tab **"📡 Streaming & Kafka Logs"**:
     * Chỉ vào bảng dữ liệu: Các sự kiện vừa thao tác đã được thống kê trực tiếp lên bảng theo thời gian thực (số lượng event, loại action, rating).
  2. Quay lại tab **"🎬 Kho Phim"**, đổi đánh giá của phim vừa chọn từ 5★ sang **1★**:
     * Quan sát ngay danh sách phim bên dưới: Hệ thống kích hoạt **Contrast Mode (Đổi khẩu vị)**, tự động loại trừ 100% phim cùng thể loại bị chê, đề xuất các kiệt tác đỉnh cao thể loại đối lập (*The Shawshank Redemption*, *The Godfather*).
* **Chỉ vào màn hình:**
  * Giải thích: Đây là tính năng "Cảnh báo và tổng hợp tức thời" (spark-alerts / realtime response) đúng như trong mô hình kiến trúc.

---

### III. CÂU HỎI GIẢNG VIÊN HAY HỎI MÁY 2 & CÁCH TRẢ LỜI
* **Hỏi:** *Tại sao dùng Kafka KRaft mà không dùng ZooKeeper?*  
  $\rightarrow$ **Trả lời:** Dạ Kafka 3.6.2 chế độ KRaft sử dụng thuật toán đồng thuận Raft nội bộ, loại bỏ hoàn toàn cụm ZooKeeper cồng kềnh, giúp giảm tài nguyên tiêu thụ và tăng tốc độ phục hồi khi broker khởi động lại.
* **Hỏi:** *Làm thế nào xử lý khi bị lệch Offset giữa Spark Streaming và Kafka?*  
  $\rightarrow$ **Trả lời:** Dạ trong `spark_streaming.py`, nhóm em thêm cấu hình `.option("failOnDataLoss", "false")` và cơ chế quản lý checkpoint riêng trên HDFS để luồng stream tự động đồng bộ lại từ offset mới nhất mà không bị crash hệ thống.
