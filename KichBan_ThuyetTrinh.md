# Kịch bản thuyết trình – Hệ thống gợi ý phim (Big Data, Nhóm 2)

Thời lượng gợi ý: khoảng 20–25 phút (1–1,5 phút mỗi slide).

## Slide 1: Trang bìa

Xin chào quý thầy cô và các bạn. Nhóm em là nhóm 2, xin phép báo cáo tiến độ đồ án môn Dữ liệu lớn với đề tài: Xây dựng hệ thống gợi ý phim trực tuyến dựa trên kiến trúc Lakehouse và điện toán phân tán. Nhóm gồm ba thành viên: Phan Trần Hữu Duy, Chung Tuấn Hỷ và Đặng Thái Hoà, dưới sự hướng dẫn của thầy Huỳnh Thái Học.

## Slide 2: Nội dung trình bày

Bài báo cáo gồm bảy phần. Đầu tiên là tổng quan đề tài và kiến trúc hệ thống. Tiếp theo là tiến độ thực hiện chi tiết, đi từ lưu trữ HDFS, xử lý bằng PySpark, huấn luyện mô hình ALS đến streaming Kafka, Airflow và giao diện FastAPI. Cuối cùng là tổng hợp tiến độ, khó khăn và kết luận.

## Slide 3: Mục tiêu đề tài

Mục tiêu của nhóm là xây dựng một pipeline dữ liệu lớn hoàn chỉnh, mô phỏng nền tảng phim trực tuyến, từ lưu trữ dữ liệu thô đến trình bày kết quả. Cụ thể có sáu mục tiêu: lưu trữ phân tán theo mô hình Medallion trên HDFS; xử lý theo lô bằng PySpark; huấn luyện mô hình gợi ý ALS bằng Spark MLlib; thu nhận dữ liệu thời gian thực bằng Kafka; điều phối tự động bằng Airflow; và phục vụ dữ liệu qua giao diện web FastAPI.

## Slide 4: Bộ dữ liệu sử dụng

Nhóm dùng hai bộ dữ liệu công khai. Thứ nhất là MovieLens 25M của GroupLens, gồm 25 triệu lượt đánh giá, 1 triệu tag trên hơn 62 nghìn bộ phim của hơn 162 nghìn người dùng, với ba tệp chính là movies, ratings và links. Thứ hai là The Movies Dataset trên Kaggle, bổ sung mô tả nội dung, điểm TMDB, kinh phí và ngày phát hành qua hai tệp movies_metadata và credits.

## Slide 5: Môi trường thực nghiệm

Hệ thống chạy trên Ubuntu 26.04 trong WSL2, với Python 3.14, OpenJDK 8, Hadoop 3.3.6, PySpark 3.5.1, Kafka 3.6.2 chế độ KRaft và Airflow 3.3.2. Điểm cần lưu ý là máy ảo WSL chỉ có khoảng 1,4 GB RAM. Đây là ràng buộc lớn, ảnh hưởng đến nhiều quyết định tối ưu mà nhóm sẽ trình bày ở các slide sau.

## Slide 6: Kiến trúc Lakehouse – Medallion

Hệ thống tổ chức theo kiến trúc Lakehouse ba tầng. Nguồn dữ liệu gồm các tệp CSV và log do Kafka Producer sinh ra. Tầng Bronze lưu dữ liệu thô. Tầng Silver lưu dữ liệu đã làm sạch dạng Snappy Parquet, cùng log streaming. Tầng Gold lưu kết quả phân tích và gợi ý phim. Có hai luồng xử lý: luồng batch dùng PySpark, Spark SQL và MLlib; luồng streaming dùng Kafka và Spark Structured Streaming. Airflow lập lịch, còn FastAPI đọc HDFS qua WebHDFS để hiển thị.

## Slide 7: Giai đoạn 1: Cụm HDFS 2 DataNode

Giai đoạn đầu là chuẩn bị môi trường. Nhóm nạp Ubuntu vào WSL2, cài Java, SSH không mật khẩu, rồi cài Hadoop 3.3.6 và khởi động HDFS. Để mô phỏng kiến trúc phân tán thật trên một máy, nhóm mở rộng lên hai DataNode bằng cách tạo bản sao cấu hình riêng, đổi cổng mạng, tệp PID và thư mục log để tránh xung đột. Kết quả là giao diện NameNode hiển thị 2 Live Nodes. Nhóm cũng tạo cấu trúc thư mục Medallion và đặt hệ số nhân bản tầng Bronze bằng 2.

## Slide 8: Giai đoạn 2–3: Bronze và PySpark

Ở giai đoạn hai, nhóm tải dữ liệu từ Kaggle, giải nén rồi đẩy lên tầng Bronze bằng lệnh hdfs dfs -put. Sang giai đoạn ba, chương trình process_movies.py dùng PySpark đọc trực tiếp CSV từ HDFS. Riêng tệp movies_metadata có ký tự xuống dòng nên phải đọc với các tùy chọn quote, escape và multiLine. Sau đó nhóm ghép ba bảng movies, links và metadata theo movieId và tmdbId, rồi chọn các cột cần dùng.

## Slide 9: Giai đoạn 4: Làm sạch dữ liệu, tầng Silver

Giai đoạn bốn là làm sạch dữ liệu bằng clean_data.py. Nhóm đổi tên cột để tránh lỗi AMBIGUOUS_REFERENCE khi join, trích năm phát hành từ tiêu đề bằng biểu thức chính quy, ép kiểu điểm TMDB về thang 0 đến 10, loại kinh phí không hợp lệ, điền giá trị thiếu và đổi timestamp sang dạng thời gian đọc được. Về tài nguyên, nhóm chuyển thư mục tạm của Spark sang đĩa thực và bỏ dropDuplicates để tránh shuffle. Kết quả: 62.423 phim và 25 triệu đánh giá được ghi dưới dạng Snappy Parquet.

## Slide 10: Giai đoạn 5: Truy vấn Spark SQL

Ở giai đoạn năm, nhóm dùng Spark SQL để phân tích. Các DataFrame được đăng ký thành bảng tạm, sau đó chạy truy vấn chuẩn ANSI ghép ba bảng, lọc phim có điểm TMDB từ 8.0 trở lên và kinh phí trên 10 triệu đô la. Kết quả là Top 5 tác phẩm kinh điển có mức đầu tư lớn, như trong hình.

## Slide 11: Giai đoạn 6: Mô hình ALS

Giai đoạn sáu là huấn luyện mô hình gợi ý bằng thuật toán ALS, một phương pháp phân rã ma trận cho lọc cộng tác. Vì chỉ có khoảng 1,4 GB RAM, nhóm lấy mẫu 3% dữ liệu, khoảng 750 nghìn bản ghi, chia 80% để huấn luyện và 20% để kiểm tra, với rank bằng 10, 5 vòng lặp và regParam 0.1. Mô hình đạt RMSE 1.1870 trên thang điểm 1 đến 5, thời gian huấn luyện và sinh gợi ý là 92,3 giây.

## Slide 12: Giai đoạn 7: Gợi ý lưu vào tầng Gold

Sau khi có mô hình, nhóm dùng hàm recommendForUserSubset để sinh Top 5 phim cho một tập người dùng. Kết quả được tách mảng bằng explode, ghép với bảng phim tầng Silver để có tên phim, thể loại và điểm dự đoán, rồi ghi Parquet vào tầng Gold. Bảng này là một số kết quả mẫu, ví dụ người dùng 95 được gợi ý Kizumonogatari Part 1 với điểm dự đoán 7,10.

## Slide 13: Giai đoạn 8: Kafka thời gian thực

Giai đoạn tám xử lý dữ liệu thời gian thực. Producer sinh log tương tác giả lập gồm user, phim và hành động như click, view, add_to_watchlist, rate, mỗi giây một bản tin, gửi vào topic movie_logs của Kafka chạy chế độ KRaft. Spark Structured Streaming đọc topic này, giải mã JSON và ghi liên tục xuống tầng Silver trên HDFS, có checkpoint để đảm bảo an toàn. Nhóm đã kiểm chứng dữ liệu xuất hiện trên HDFS.

## Slide 14: Giai đoạn 9: Airflow

Giai đoạn chín là tự động hóa bằng Airflow 3.3.2. Nhóm phải nâng cấp từ bản 2.9 vì Python 3.14 không còn được hỗ trợ, và viết một CLI tùy chỉnh vì pip không sinh tệp lệnh airflow. DAG movie_data_pipeline chạy hàng ngày, dùng BashOperator để kích hoạt tác vụ Spark batch, trạng thái và log được theo dõi trên giao diện web. Hạng mục này đạt mức cơ bản và sẽ được hoàn thiện thêm.

## Slide 15: Giao diện web FastAPI

Để trực quan hóa, nhóm xây dựng giao diện web bằng FastAPI, chạy bằng uvicorn tại cổng 8000, kết nối HDFS qua WebHDFS REST API và hiển thị dữ liệu của ba tầng Bronze, Silver, Gold. Đây hiện là bản kiểm thử nhanh, sẽ được hoàn thiện ở giai đoạn sau.

## Slide 16: Tổng hợp tiến độ

Tổng hợp lại, mười hạng mục đều đã có kết quả. Tám hạng mục đã hoàn thành đầy đủ, từ cụm HDFS, tầng Bronze, PySpark, Silver, Spark SQL, mô hình ALS, tầng Gold đến Kafka và Streaming. Airflow hoàn thành ở mức cơ bản và giao diện FastAPI mới ở bản kiểm thử.

## Slide 17: Kết quả đạt được

So với lúc đầu, dữ liệu trên HDFS chuyển từ CSV thô sang Parquet nén Snappy, giảm khoảng 70% kích thước và đọc nhanh hơn nhiều lần. Dữ liệu đã được làm sạch và chuẩn hóa. Hệ thống có mô hình ALS với RMSE 1.1870, kết quả gợi ý được lưu vào tầng Gold thay vì chỉ hiển thị tạm, đồng thời đã có luồng Kafka Streaming và DAG Airflow thay cho việc chạy thủ công.

## Slide 18: Khó khăn và cách giải quyết

Nhóm gặp bảy khó khăn chính. Mỗi máy chỉ chạy một DataNode, nhóm giải quyết bằng cách nhân bản cấu hình và đổi cổng. Tràn đĩa tạm khi xử lý 25 triệu dòng được xử lý bằng cách chuyển thư mục tạm sang đĩa thực. Thiếu RAM khi huấn luyện ALS thì lấy mẫu 3% dữ liệu. Ngoài ra còn lỗi join, lỗi đọc CSV có xuống dòng, vấn đề tương thích phiên bản, và việc mất dữ liệu trong /tmp khi khởi động lại WSL, nhóm đã xử lý từng vấn đề như trên slide.

## Slide 19: Kết luận và hướng tiếp theo

Đến thời điểm này, nhóm đã hoàn thành các thành phần cốt lõi: cụm HDFS hai DataNode, kho Medallion với 25 triệu đánh giá ở định dạng Parquet, pipeline PySpark, mô hình ALS với kết quả ở tầng Gold, luồng Kafka và Spark Streaming, Airflow và giao diện FastAPI. Hệ thống vận hành ổn định trên tài nguyên hạn chế. Các bước tiếp theo là nâng cao chất lượng mô hình, hoàn thiện chuỗi tự động hóa và giao diện trình bày để hoàn tất đồ án.

## Slide 20: Cảm ơn, Q&A

Phần báo cáo của nhóm đến đây là hết. Xin chân thành cảm ơn quý thầy cô và các bạn đã lắng nghe. Nhóm em xin sẵn sàng tiếp thu góp ý và giải đáp các câu hỏi.
