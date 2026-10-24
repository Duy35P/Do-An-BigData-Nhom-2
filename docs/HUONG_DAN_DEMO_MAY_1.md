# HƯỚNG DẪN THAO TÁC DEMO — MÁY 1
## VAI TRÒ TRÊN SƠ ĐỒ: DATA PRODUCERS (Website & Mobile App Event Generator)

---

### I. CÁC BƯỚC CHUẨN BỊ TRƯỚC KHI BẮT ĐẦU (SETUP TRƯỚC 3 PHÚT)

1. **Khởi động ứng dụng:**
   - Ra ngoài màn hình Desktop, nhấp đúp vào file `START_SYSTEM.bat` (hoặc mở terminal chạy `python3 -m uvicorn src.main:app --host 0.0.0.0 --port 8000`).
2. **Mở sẵn 2 cửa sổ trên màn hình:**
   - **Cửa sổ 1 (Trình duyệt):** Mở sẵn đường dẫn `http://localhost:8000` (Giao diện Web Netflix Recommender).
   - **Cửa sổ 2 (Terminal WSL):** Mở sẵn 1 cửa sổ dòng lệnh Linux, gõ sẵn lệnh sau nhưng **chưa bấm Enter**:
     ```bash
     python3 producer.py
     ```
   - **Cửa sổ 3 (VS Code / Trình xem code):** Mở sẵn file `src/main.py` tại dòng 50 (hàm `send_kafka_event`).

---

### II. THỨ TỰ CÁC THAO TÁC THỰC HIỆN TRONG KHI DEMO

#### BƯỚC 1: DEMO GIAO DIỆN CLIENT & PHÁT SINH SỰ KIỆN TƯƠNG TÁC (WEBSITE PRODUCER)
* **Thời gian thực hiện:** ~45 giây.
* **Màn hình thao tác:** Cửa sổ Trình duyệt Web (`http://localhost:8000`).
* **Các thao tác cụ thể:**
  1. Trên thanh tìm kiếm ở đầu trang, gõ chữ: `Toy Story`.
  2. Bấm chọn phim **"Toy Story (1995)"**.
  3. Di chuột vào phần đánh giá sao, nhấp chọn **5★**.
     * *Mục đích:* Kích hoạt hàm bắn bản tin đánh giá người dùng vào Kafka.
  4. Nhấp vào nút **"❤️ Yêu thích"** (Add to Watchlist) trên thẻ phim.
     * *Mục đích:* Kích hoạt sự kiện `action: add_to_watchlist` gửi sang Kafka.

---

#### BƯỚC 2: CHỈ VÀO MÃ NGUỒN TẠO BẢN TIN JSON STREAMING
* **Thời gian thực hiện:** ~20 giây.
* **Màn hình thao tác:** Cửa sổ VS Code (file `src/main.py`).
* **Các thao tác cụ thể:**
  1. Mở file `src/main.py` và chỉ tay vào đoạn code hàm `send_kafka_event()`:
     ```python
     kafka_msg = {
         "timestamp": ts,
         "user_id": int(user_id),
         "movie_id": int(movie_id),
         "action": str(action),     # "rate", "add_to_watchlist"
         "rating": rating_val
     }
     producer.send("movie_logs", kafka_msg)
     ```
  2. Chỉ cho Thầy/Cô thấy: Mọi hành vi trên web đều được đóng gói thành chuẩn JSON và bắn trực tiếp vào topic `movie_logs` của Kafka Broker cổng 9092.

---

#### BƯỚC 3: DEMO GIẢ LẬP TẢI TỪ ỨNG DỤNG DI ĐỘNG (MOBILE APP / IOT SIMULATOR)
* **Thời gian thực hiện:** ~25 giây.
* **Màn hình thao tác:** Cửa sổ Terminal WSL đã chuẩn bị sẵn ở Bước I.
* **Các thao tác cụ thể:**
  1. Bấm phím **Enter** để kích hoạt chạy file:
     ```bash
     python3 producer.py
     ```
  2. Để terminal chạy tự động bắn ra 4-5 dòng log JSON tương tự như sau:
     ```text
     Đã gửi: {'timestamp': '2026-10-24 19:40:11', 'user_id': 342, 'movie_id': 1196, 'action': 'view'}
     Đã gửi: {'timestamp': '2026-10-24 19:40:12', 'user_id': 815, 'movie_id': 260, 'action': 'rate', 'rating': 4.5}
     Đã gửi: {'timestamp': '2026-10-24 19:40:13', 'user_id': 102, 'movie_id': 589, 'action': 'click'}
     ```
  3. Nhấn tổ hợp phím **Ctrl + C** trên bàn phím để dừng tiến trình bắn dữ liệu.
  4. Hướng tay về phía **Máy 2** để báo hiệu cho bạn thứ hai bắt đầu phần demo Kafka & Spark Streaming.
