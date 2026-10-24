# HƯỚNG DẪN THAO TÁC DEMO ĐỘC LẬP — MÁY 1
## VAI TRÒ TRÊN SƠ ĐỒ: DATA PRODUCERS (Website & Mobile App Event Generator)
> *(Máy này hoạt động hoàn toàn độc lập, có thể demo bất cứ lúc nào giảng viên gọi tên, không phụ thuộc vào Máy 2 hay Máy 3).*

---

### I. CÔNG VIỆC CHUẨN BỊ (TRƯỚC KHI GIẢNG VIÊN GỌI)

1. **Khởi động ứng dụng Web:**
   * Ra ngoài màn hình Desktop, nhấp đúp vào file `START_SYSTEM.bat` (hoặc mở terminal chạy: `python3 -m uvicorn src.main:app --host 0.0.0.0 --port 8000`).
2. **Mở sẵn 3 cửa sổ trên màn hình:**
   * **Cửa sổ 1 (Trình duyệt):** Mở sẵn đường dẫn `http://localhost:8000` (Giao diện Web Netflix Recommender).
   * **Cửa sổ 2 (Terminal WSL):** Để sẵn lệnh sau nhưng **chưa bấm Enter**:
     ```bash
     python3 producer.py
     ```
   * **Cửa sổ 3 (VS Code / Trình soạn thảo):** Mở sẵn file `src/main.py` (tại dòng 50: hàm `send_kafka_event`).

---

### II. CÁC BƯỚC THAO TÁC THỰC HIỆN KHI ĐẾN LƯỢT DEMO

#### BƯỚC 1: DEMO NGUỒN SỰ KIỆN TỪ GIAO DIỆN WEB (WEBSITE PRODUCER)
* **Màn hình:** Trình duyệt Web (`http://localhost:8000`).
* **Thao tác thực hiện:**
  1. Trên ô tìm kiếm của Web, gõ tên phim: `Toy Story`.
  2. Bấm chọn phim **"Toy Story (1995)"**.
  3. Bấm đánh giá **5★** cho phim.
  4. Bấm nút **"❤️ Yêu thích"** (Add to Watchlist) trên thẻ phim.
* **Chỉ vào màn hình:**
  * Giải thích cho Thầy/Cô: Giao diện Web đóng vai trò là một **Data Producer** trong sơ đồ. Mỗi thao tác click, rate, hay watchlist đều lập tức kích hoạt sự kiện gửi đi.

---

#### BƯỚC 2: CHỨNG MINH CODE BÓC TÁCH & GỬI BẢN TIN JSON VÀO KAFKA
* **Màn hình:** VS Code (file `src/main.py`).
* **Thao tác thực hiện:**
  1. Kéo đến hàm `send_kafka_event()` và chỉ chuột vào cấu trúc bản tin JSON:
     ```python
     kafka_msg = {
         "timestamp": ts,
         "user_id": int(user_id),
         "movie_id": int(movie_id),
         "action": str(action),     # 'rate', 'add_to_watchlist'
         "rating": rating_val
     }
     producer.send("movie_logs", kafka_msg)
     ```
* **Chỉ vào màn hình:**
  * Giải thích: Dữ liệu không lưu trực tiếp vào cơ sở dữ liệu truyền thống mà được đẩy sang hàng đợi phân tán Kafka qua topic `movie_logs` với cổng `localhost:9092`.

---

#### BƯỚC 3: DEMO BỘ SINH DỮ LIỆU TỰ ĐỘNG MÔ PHỎNG CLIENT (MOBILE APP / IOT SIMULATOR)
* **Màn hình:** Cửa sổ Terminal WSL (lệnh `python3 producer.py`).
* **Thao tác thực hiện:**
  1. Nhấn **Enter** để chạy file `producer.py`.
  2. Để terminal chạy tự động bắn ra 4-5 dòng log JSON thực tế:
     ```text
     Đã gửi: {'timestamp': '2026-10-24 19:40:11', 'user_id': 342, 'movie_id': 1196, 'action': 'view'}
     Đã gửi: {'timestamp': '2026-10-24 19:40:12', 'user_id': 815, 'movie_id': 260, 'action': 'rate', 'rating': 4.5}
     Đã gửi: {'timestamp': '2026-10-24 19:40:13', 'user_id': 102, 'movie_id': 589, 'action': 'click'}
     ```
  3. Bấm tổ hợp phím **Ctrl + C** để dừng lại.
* **Chỉ vào màn hình:**
  * Giải thích: Đây là tiến trình mô phỏng các thiết bị di động (Mobile App) hoặc IoT liên tục phát sinh sự kiện người dùng với tần suất cao để kiểm thử khả năng chịu tải của hệ thống.

---

### III. CÂU HỎI GIẢNG VIÊN HAY HỎI MÁY 1 & CÁCH TRẢ LỜI
* **Hỏi:** *Dữ liệu gửi vào Kafka có cấu trúc schema như thế nào?*  
  $\rightarrow$ **Trả lời:** Dữ liệu dạng JSON gồm 5 trường: `timestamp` (thời gian), `user_id` (mã người dùng), `movie_id` (mã phim), `action` (hành động: view, click, rate, watchlist) và `rating` (điểm đánh giá từ 1.0 đến 5.0).
* **Hỏi:** *Nếu Kafka bị tắt thì Web có bị crash không?*  
  $\rightarrow$ **Trả lời:** Dạ không, trong `src/main.py` nhóm em cài đặt Kafka Producer dưới dạng Singleton có khối `try...except`. Nếu Kafka tạm thời không phản hồi, Web vẫn phục vụ người dùng bình thường và ghi nhận cảnh báo mà không bị sập.
