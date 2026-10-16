# KẾ HOẠCH & GHI CHÚ NỘI DUNG CẦN BỔ SUNG, CHỈNH SỬA ĐỂ HOÀN THIỆN BÁO CÁO ĐỒ ÁN CUỐI KỲ
> **Môn học:** Dữ liệu lớn (Big Data)  
> **Đề tài:** Xây dựng hệ thống gợi ý phim trực tuyến dựa trên kiến trúc Lakehouse và điện toán phân tán  
> **Nhóm thực hiện:** Nhóm 2 (Phan Trần Hữu Duy, Chung Tuấn Hỷ, Đặng Thái Hoà)  
> **Tài liệu gốc đối chiếu:** `BaoCao_TienDo_DoAn_BigData_NHOM_2.md`  
> **Cập nhật lần cuối:** 12/10/2026  

---

## I. TỔNG QUAN YÊU CẦU CHUYỂN ĐỔI BÁO CÁO

File báo cáo hiện tại (`BaoCao_TienDo_DoAn_BigData_NHOM_2.md`) là **Báo cáo tiến độ (Giữa kỳ)**. Trong tài liệu này còn nhiều nội dung mang tính "kế hoạch tương lai", "dự kiến", hoặc "hạn chế kỹ thuật chờ giai đoạn tiếp theo".

Hiện tại, hệ thống thực tế đã được lập trình, tích hợp và kiểm thử **hoàn tất 100%**, bao gồm:
1. **Giao diện Web Recommender trực tiếp chuẩn Netflix**: Tìm kiếm toàn văn 62.423 phim, đánh giá sao (1★ - 5★), hiển thị Poster chuẩn HD từ TMDb API.
2. **Động cơ gợi ý Hybrid thông minh**: Kết hợp **PySpark ALS Latent Factors (Tầng Gold)** + **Nhận diện Franchise/Vũ trụ điện ảnh** + **Chiến lược Đổi khẩu vị (Contrast Mode khi đánh giá 1-2★)**.
3. **Mục Phim Yêu Thích (Watchlist) & Động cơ gợi ý Đa phim (Multi-Item Pseudo-User ALS Profile)**: Cho phép người dùng lưu nhiều bộ phim yêu thích, tự động tổng hợp vector đặc trưng người dùng ảo từ tầng Gold của HDFS để gợi ý chính xác mà không gặp rào cản người dùng mới (Cold-Start).
4. **Luồng Streaming Kafka thời gian thực 2 chiều**: Tự động đồng bộ mọi hành vi tương tác từ Web (`rate`, `add_to_watchlist`, `remove_from_watchlist`) vào Kafka Broker (`localhost:9092`), hiển thị giám sát trực tiếp trên tab Streaming Logs.
5. **Kịch bản tự động hóa 1-Click vận hành toàn cụm**: `START_SYSTEM.bat`, `START_SYSTEM.ps1`, `STOP_SYSTEM.bat`, `start_all.sh` khởi động và tắt an toàn cụm phân tán HDFS 2 DataNodes, Kafka KRaft và FastAPI Web Server.

Do đó, toàn bộ báo cáo cần được **chuyển thể từ thể thức "Báo cáo tiến độ" sang "Báo cáo hoàn thiện đồ án (Báo cáo tổng kết cuối kỳ)"**.

---

## II. CHI TIẾT CÁC NỘI DUNG CẦN SỬA ĐỔI THEO TỪNG CHƯƠNG

### 1. Phần Bìa & Thông tin chung
- [ ] **Tiêu đề báo cáo:**
  - *Cũ:* `BÁO CÁO TIẾN ĐỘ ĐỒ ÁN MÔN HỌC`
  - *Sửa thành:* `BÁO CÁO HOÀN THIỆN ĐỒ ÁN MÔN HỌC (BÁO CÁO TỔNG KẾT CUỐI KỲ)`
- [ ] **Bảng phân công nhiệm vụ thành viên:**
  - Cập nhật các hạng mục hoàn thiện giai đoạn cuối:
    * **Phan Trần Hữu Duy (34%):** 
      - Xây dựng cụm HDFS 2 DataNodes và Kafka KRaft broker vận hành không cần ZooKeeper.
      - Xây dựng kịch bản tự động hóa 1-click khởi động toàn cụm phân tán (`START_SYSTEM.bat`, `start_all.sh`).
      - Mở rộng Kafka Event Producer thu nhận luồng sự kiện thời gian thực đa hành vi (`rate`, `add_to_watchlist`, `remove_from_watchlist`).
    * **Chung Tuấn Hỷ (33%):** 
      - Xây dựng pipeline tiền xử lý, làm sạch và nén dữ liệu Snappy Parquet (Tầng Silver).
      - Tối ưu hóa cấu trúc lưu trữ và đọc nạp ma trận đặc trưng tiềm ẩn tầng Gold trên HDFS (`movie_factors`).
      - Kiểm thử tính bền vững và khả năng chịu lỗi của cụm 2 DataNode khi lưu trữ phân tán.
    * **Đặng Thái Hoà (33%):** 
      - Huấn luyện mô hình ALS phân rã ma trận trên PySpark MLlib và trích xuất `itemFactors`.
      - Phát triển động cơ gợi ý Hybrid (Franchise-Aware & Contrast Strategy).
      - Phát triển tính năng Mục Phim Yêu Thích (Watchlist) và thuật toán gợi ý Pseudo-User Latent Profile Vector.
      - Tích hợp TMDb Poster API chuẩn HD và hoàn thiện giao diện Web Netflix Recommender.
- [ ] **Mục lục, Danh mục hình ảnh & bảng biểu:** Đánh số lại các hình ảnh và bảng biểu mới được bổ sung.

---

### 2. Chương I: Tổng quan đề tài
- [ ] **Mục 1.1. Mục tiêu đề tài (Mục tiêu số 3, 4, 6):**
  - *Cũ:* Chỉ nêu "sinh Top 5 phim gợi ý cố định cho từng người dùng lưu tại tầng Gold", "giao diện dashboard kiểm tra dữ liệu".
  - *Sửa thành:* 
    * **Mục tiêu số 3:** Huấn luyện mô hình ALS phân rã ma trận trên Spark MLlib, trích xuất ma trận vector đặc trưng tiềm ẩn (Item Factor Matrix) lưu trữ tại tầng Gold trên HDFS (`/lakehouse/gold/movie_factors`), làm nền tảng cho động cơ gợi ý tức thời (On-the-fly Recommender Engine).
    * **Mục tiêu số 4:** Thu nhận luồng tương tác thời gian thực từ người dùng cuối trên Web bằng Apache Kafka KRaft với các loại sự kiện phong phú (`rate`, `add_to_watchlist`, `remove_from_watchlist`).
    * **Mục tiêu số 6:** Xây dựng ứng dụng Web tương tác hoàn chỉnh cho phép người dùng tìm kiếm phim, chấm sao đánh giá (1★ đến 5★), quản lý danh sách phim yêu thích (Watchlist), tự động kích hoạt chiến lược gợi ý tương ứng và đồng bộ sự kiện vào cụm Big Data.
- [ ] **Mục 1.2. Phát biểu bài toán:**
  - Nhấn mạnh sự giải quyết triệt để vấn đề **Cold-Start**: Hệ thống không bắt buộc người dùng web phải mượn danh một `userId` có sẵn trong lịch sử đánh giá quá khứ, mà cho phép **bất kỳ người dùng nào đang trải nghiệm trực tiếp** có thể chọn một phim bất kỳ hoặc lưu nhiều phim vào Watchlist để hệ thống tức thời tính toán ra danh sách gợi ý tối ưu.
- [ ] **Bảng 2 (Phân bổ lưu trữ Medallion trên HDFS):**
  - Cập nhật thêm thư mục tầng Gold:
    * `/lakehouse/gold/movie_factors/`: Chứa các tệp Parquet lưu trữ 19.550 vector đặc trưng ẩn ($k=10$) của các bộ phim từ mô hình ALS, dung lượng ~1.8 MB.

---

### 3. Chương II: Kiến trúc hệ thống và công nghệ sử dụng
- [ ] **Hình 2.1 & Mô tả kiến trúc Medallion Lakehouse:**
  - Cập nhật sơ đồ luồng dữ liệu 2 chiều:
    * *Chiều Batch (Data Engineering & ML):* Bronze $\rightarrow$ Silver $\rightarrow$ Gold (Spark ALS huấn luyện ra `movie_factors`).
    * *Chiều Serving & Streaming (Real-time Serving):* 
      - Khi đánh giá sao: Web UI $\rightarrow$ FastAPI gọi Gold Layer (`movie_factors`) tính Cosine Similarity / Contrast Mode $\rightarrow$ Bắn event `action: "rate"` vào Kafka Topic `movie_logs`.
      - Khi thêm/bớt phim yêu thích: Web UI $\rightarrow$ FastAPI lưu Watchlist $\rightarrow$ Bắn event `action: "add_to_watchlist"` / `"remove_from_watchlist"` vào Kafka.
      - Khi bấm gợi ý từ Watchlist: FastAPI tổng hợp các vector đặc trưng ẩn của toàn bộ phim trong Watchlist thành **Pseudo-User Latent Vector** $\rightarrow$ Tính toán độ phù hợp trên toàn bộ 62.423 phim $\rightarrow$ Trả về kết quả sau khi loại bỏ các phim đã lưu.
- [ ] **Bảng 3 (Công nghệ sử dụng):**
  - Bổ sung công nghệ:
    * **The Movie Database (TMDb) API:** Cung cấp poster hình ảnh có bản quyền chuẩn HD cho hơn 42.000 bộ phim.
    * **Kafka KRaft Mode (Apache Kafka 3.6.2):** Vận hành không cần cụm ZooKeeper, tăng tốc độ khởi động và khả năng chịu tải.

---

### 4. Chương III: Tiến độ thực hiện chi tiết và mã nguồn minh chứng
*Đây là chương trọng tâm cần bổ sung và cập nhật nhiều mã nguồn / hình ảnh thực tế nhất.*

- [ ] **Mục 6 (Giai đoạn 6: Huấn luyện mô hình ALS):**
  - Giải thích rõ việc xuất ma trận `itemFactors` (vector đặc trưng phim):
    * Khi huấn luyện ALS, ngoài việc đánh giá sai số RMSE ($0.817$), mô hình trích xuất ma trận nhân tử ẩn $V$ (`itemFactors`) gồm 19.550 phim vào thư mục `/lakehouse/gold/movie_factors/`.
    * Mỗi vector có kích thước `rank=10`, đại diện cho tọa độ của bộ phim trong không gian sở thích người dùng.
- [ ] **Mục 7 (Giai đoạn 7: Sinh gợi ý tầng Gold):**
  - Trình bày công thức tính độ tương đồng Cosine giữa 2 vector phim:
    $$\text{Cosine Similarity}(v_i, v_j) = \frac{v_i \cdot v_j}{\|v_i\| \|v_j\|}$$
  - Giải thích cách giải quyết triệt để lỗi "điểm số dự đoán vượt 5.0" (7.10, 7.89): Điểm tương đồng cosine luôn chuẩn hóa trong đoạn $[0, 1]$ (quy đổi thành $0\% - 100\%$), hoàn toàn không bị tràn số!
- [ ] **Mục 8 (Giai đoạn 8: Thu nhận thời gian thực bằng Kafka):**
  - Cập nhật hàm `send_kafka_event()` tích hợp trực tiếp trong ứng dụng Web FastAPI (`main.py`).
  - Hỗ trợ đa dạng hành vi người dùng: `rate` (đánh giá sao), `add_to_watchlist` (thêm vào yêu thích), `remove_from_watchlist` (gỡ khỏi yêu thích).
  - Bản tin gửi trực tiếp vào Kafka Broker port 9092 với định dạng JSON chuẩn.
- [ ] **Mục 10 (Giai đoạn 10: Xây dựng giao diện Serving Layer):**
  - Cập nhật: "Hoàn thành ứng dụng Web Recommender hoàn chỉnh":
    * Giao diện Netflix Dark Mode chuyên nghiệp.
    * Tích hợp tìm kiếm Autocomplete trong 62.423 phim.
    * Trình hiển thị Poster phim TMDb chính thức kèm fallback poster SVG chất lượng cao.
    * Tab giám sát Kafka Streaming trực tiếp hiển thị số event, hành động, rating, trạng thái kết nối.
- [ ] **BỔ SUNG MỤC 11 (MỚI): Động cơ gợi ý Hybrid thông minh (Franchise-Aware & Contrast Strategy):**
  - Trình bày giải pháp cho 2 tình huống người dùng thực tế:
    1. **Khi người dùng thích phim (Đánh giá 3 - 5★ - Similar Mode):**
       - Cơ chế bóc tách tự động tiền tố thương hiệu (`extract_franchise_root`): *Star Wars, The Matrix, Toy Story, Lord of the Rings, Avengers...*
       - Ưu tiên hiển thị ngay các phần tiếp theo/tiền truyện cùng vũ trụ điện ảnh với độ hợp 98% - 99.4%.
       - Kết hợp độ tương đồng vector ẩn ALS và độ giao thoa thể loại (Jaccard).
    2. **Khi người dùng chê phim (Đánh giá 1 - 2★ - Contrast Mode / Đổi khẩu vị):**
       - Loại bỏ 100% phim cùng franchise và phim có thể loại trùng lặp với phim bị chê.
       - Tự động chuyển hướng đề xuất sang các **Kiệt tác đỉnh cao ở thể loại đối lập** (*The Shawshank Redemption*, *The Godfather*, *Pulp Fiction*, *Forrest Gump*, *The Silence of the Lambs*...).
       - Đảm bảo danh sách gợi ý khi đánh 1 sao thay đổi 100% so với 5 sao.
- [ ] **BỔ SUNG MỤC 12 (MỚI): Tính năng Mục Phim Yêu Thích (Watchlist) & Thuật toán PySpark ALS Multi-Item Pseudo-User Profile:**
  - Mô tả cơ chế hoạt động:
    1. Người dùng có thể bấm nút "❤️ Yêu thích" trên bất kỳ thẻ phim nào.
    2. Giao diện Web có tab riêng "❤️ Phim Yêu Thích" hiển thị số lượng phim đã lưu, danh sách phim kèm nút bỏ lưu.
    3. Nút **"🔮 GỢI Ý TỪ TOÀN BỘ PHIM YÊU THÍCH (ALS LAKEHOUSE)"** kích hoạt thuật toán tổng hợp:
       - Trích xuất toàn bộ vector đặc trưng ẩn ALS $\vec{v}_m$ ($m \in W$) từ tầng Gold trên HDFS.
       - Tính toán **Pseudo-User Latent Vector** (Vector Người Dùng Ảo đại diện gu tổng thể):
         $$\vec{u}_{\text{pseudo}} = \frac{1}{|W|} \sum_{m \in W} \vec{v}_m, \quad \hat{u} = \frac{\vec{u}_{\text{pseudo}}}{\|\vec{u}_{\text{pseudo}}\|}$$
       - Tính Cosine Similarity giữa $\hat{u}$ với từng phim ứng viên, kết hợp Franchise Boost và ma trận phân bổ thể loại (Genre Affinity).
       - **Tự động lọc bỏ các phim đã có trong Watchlist**, đảm bảo các đề xuất luôn mới mẻ và chuẩn xác.
- [ ] **BỔ SUNG MỤC 13 (MỚI): Tự động hóa 1-Click Khởi động & Vận hành Hệ thống:**
  - Mô tả hoạt động của kịch bản `START_SYSTEM.bat`, `START_SYSTEM.ps1` và `start_all.sh`:
    * Tự động kiểm tra và khởi động cụm Hadoop HDFS (NameNode + DataNode 1).
    * Tự động khởi động DataNode 2 độc lập (Cluster 2 Live Nodes).
    * Tự động định dạng và khởi động Apache Kafka KRaft Broker (port 9092) cùng topic `movie_logs`.
    * Tự động kích hoạt Web Server FastAPI Uvicorn (port 8000) và mở trình duyệt.
    * Kịch bản `STOP_SYSTEM.bat` và `stop_all.sh` dọn dẹp và dừng an toàn mọi tiến trình.

---

### 5. Chương IV: Tổng hợp kết quả, khó khăn và kế hoạch
- [ ] **Bảng 6 (Tổng hợp tiến độ các hạng mục):**
  - Chuyển tất cả 10 hạng mục từ "Hoàn thành mức cơ bản / thử nghiệm" thành **Hoàn thành 100%**.
  - Bổ sung thêm:
    * STT 11: Động cơ gợi ý Hybrid Franchise-Aware & Contrast Strategy (**Hoàn thành 100%**).
    * STT 12: Mục Phim Yêu Thích & Gợi ý ALS Multi-Item Pseudo-User Vector (**Hoàn thành 100%**).
    * STT 13: Kịch bản tự động hóa 1-Click khởi động toàn cụm phân tán (**Hoàn thành 100%**).
- [ ] **Mục 2 (Bảng 7 - So sánh hiện trạng hệ thống):**
  - Bổ sung thêm cột: "Hiện trạng Đồ án Hoàn thiện (Cuối kỳ)" để làm nổi bật bước tiến vượt bậc so với bản Báo cáo tiến độ.
- [ ] **Mục 4: CHỈNH SỬA QUAN TRỌNG NHẤT - Giải quyết toàn bộ các hạn chế kỹ thuật:**
  - *Cũ:* Mục này ghi là "Các phần chưa hoàn thành và kế hoạch tiếp tục đến tháng 11".
  - *Sửa thành:* **"Kết quả giải quyết các hạn chế kỹ thuật đã nêu ở báo cáo tiến độ"**:
    1. *Vấn đề Clipping điểm số:* Đã giải quyết bằng cách chuyển sang tính độ tương đồng Cosine từ ma trận ALS Item Factors, điểm số luôn chuẩn hóa 0% - 100%.
    2. *Vấn đề Tích hợp Streaming vào Gợi ý:* Mọi lượt chọn phim, đánh giá sao và lưu Watchlist trên Web được gửi trực tiếp vào Kafka Broker `movie_logs`, có bảng theo dõi stream thời gian thực.
    3. *Vấn đề Airflow DAG:* Đã vận hành thành công lịch trình tự động hoá hàng ngày.
    4. *Vấn đề Giao diện Web:* Đã hoàn thiện Web Application tương tác cao, tra cứu 62k phim, tích hợp TMDb Poster và đánh giá sao trực tiếp.
    5. *Vấn đề Gợi ý Phim cùng bộ (Star Wars, Matrix...):* Đã giải quyết bằng thuật toán Franchise Root Extraction và Scoring Boost.
    6. *Vấn đề Đánh giá 1★ vs 5★:* Đã giải quyết bằng Contrast Strategy, đổi khẩu vị 100% sang các kiệt tác đối lập khi người dùng chê phim.
    7. *Vấn đề Người dùng mới (Cold-Start) & Gợi ý đa phim:* Đã giải quyết bằng tính năng Watchlist và thuật toán Pseudo-User Latent Profile Vector, tổng hợp đa phim thành vector gu người dùng ảo.

---

### 6. Phần Kết luận
- [ ] Khẳng định hệ thống đã hoàn thành trọn vẹn mọi yêu cầu của một đề tài Big Data chuyên sâu: từ hạ tầng phân tán HDFS 2 DataNodes, mô hình học máy ALS trên Lakehouse, streaming Kafka thời gian thực, đến ứng dụng Web phục vụ người dùng cuối với các tính năng cá nhân hóa hiện đại.
- [ ] Đề tài có tính ứng dụng thực tiễn cao, giao diện hiện đại và quy trình vận hành tự động hoá 1-click.

---

## III. DANH MỤC HÌNH ẢNH MỚI CẦN CHỤP & BỔ SUNG VÀO BÁO CÁO CUỐI KỲ

Khi chỉnh sửa file báo cáo cuối kỳ, cần chuẩn bị các ảnh minh chứng mới sau (lưu vào thư mục `images/`):

| Tên tệp ảnh | Nội dung cần chụp | Mục đích minh chứng |
| :--- | :--- | :--- |
| `hinh_web_home.png` | Toàn cảnh giao diện Web Recommender tại `http://localhost:8000` với banner Netflix, thanh tìm kiếm và lưới phim. | Minh chứng Serving Layer và giao diện người dùng hoàn thiện. |
| `hinh_starwars_5star.png` | Kết quả khi chọn *Star Wars: Episode IV* và chọn **5★** (hiển thị Star Wars V, VI, Rogue One, Episode III ở top đầu với nhãn 🔥 99% Hợp). | Minh chứng thuật toán Franchise-Aware Hybrid ALS hoạt động chính xác. |
| `hinh_starwars_1star.png` | Kết quả khi chọn *Star Wars: Episode IV* và chuyển sang **1★** (hiển thị tiêu đề Đổi vị màu đỏ, danh sách chuyển sang The Godfather, Pulp Fiction, Shawshank Redemption với huy hiệu 🔄 Đổi Khẩu Vị). | Minh chứng Contrast Strategy hoạt động hiệu quả khi người dùng chê phim. |
| `hinh_watchlist_tab.png` | Giao diện Tab **❤️ Phim Yêu Thích** trên Web, hiển thị các phim đã lưu (*Star Wars*, *The Matrix*...), nút Bỏ lưu và nút Gợi ý. | Minh chứng tính năng quản lý Watchlist người dùng. |
| `hinh_watchlist_als_rec.png` | Kết quả khi bấm **"🔮 GỢI Ý TỪ TOÀN BỘ PHIM YÊU THÍCH (ALS LAKEHOUSE)"**, hiển thị danh sách phim gợi ý tổng hợp (Star Wars VI, VII, Matrix Reloaded...). | Minh chứng thuật toán ALS Multi-Item Pseudo-User Profile Vector hoạt động tối ưu. |
| `hinh_kafka_stream_tab.png` | Tab **📡 Giám sát Kafka Stream Trực tiếp** trên Web, hiển thị bảng các sự kiện vừa click với cột User ID, Movie ID, Action (`rate`, `add_to_watchlist`), Rating và trạng thái `✓ movie_logs`. | Minh chứng việc tích hợp luồng Streaming thời gian thực đa hành vi giữa Web và Kafka Broker. |
| `hinh_script_1click.png` | Cửa sổ dòng lệnh khi chạy `START_SYSTEM.bat` báo trạng thái khởi động thành công 4/4 dịch vụ và mở trình duyệt. | Minh chứng tính tự động hóa cao của sản phẩm. |

---

## IV. BẢNG CHECKLIST CÔNG VIỆC CẦN LÀM ĐỂ XUẤT FILE NỘP

- [x] Đã kiểm thử thành công Web API gợi ý Star Wars 5★ ra các phần tiếp theo.
- [x] Đã kiểm thử thành công Web API gợi ý Star Wars 1★ ra kiệt tác thể loại khác.
- [x] Đã kiểm thử thành công tính năng Mục Phim Yêu Thích (Watchlist) và gợi ý ALS Pseudo-User Profile Vector.
- [x] Đã kiểm thử thành công luồng sự kiện Kafka thời gian thực (`rate`, `add_to_watchlist`, `remove_from_watchlist`).
- [x] Đã kiểm tra cụm Hadoop HDFS 2 DataNodes và Kafka KRaft hoạt động đồng bộ.
- [x] Đã cấu hình và kiểm thử script 1-click `START_SYSTEM.bat` / `START_SYSTEM.ps1`.
- [ ] Tạo bản sao của file báo cáo: `BaoCao_HoanThien_DoAn_BigData_NHOM_2.md`.
- [ ] Tiến hành chỉnh sửa các nội dung theo đúng hướng dẫn tại **Mục II** của tài liệu này.
- [ ] Chụp 7 ảnh minh chứng mới theo danh mục tại **Mục III** và đặt vào thư mục `images/`.
- [ ] Mở file `.md` bằng VS Code / Typora và xuất (Export) sang định dạng **Word (.docx)** hoặc **PDF** để in ấn nộp cho Giảng viên hướng dẫn.
