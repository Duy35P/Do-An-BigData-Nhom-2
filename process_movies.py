from pyspark.sql import SparkSession
import pyspark.sql.functions as F
import time
spark = SparkSession.builder \
    .appName("MovieData_Join") \
    .master("local[*]") \
    .getOrCreate()
spark.sparkContext.setLogLevel("ERROR")
print("Đang đọc và xử lý dữ liệu từ HDFS, vui lòng đợi...")
# Đọc file với đường dẫn cụ thể
df_movies = spark.read.csv("hdfs://localhost:9000/lakehouse/bronze/movielens/movies.csv", header=True, inferSchema=True)
df_links  = spark.read.csv("hdfs://localhost:9000/lakehouse/bronze/movielens/links.csv", header=True, inferSchema=True)
df_meta   = spark.read.csv("hdfs://localhost:9000/lakehouse/bronze/movielens/movies_metadata.csv", 
                           header=True, inferSchema=True, quote='"', escape='"', multiLine=True)
# Join dữ liệu
df_step1 = df_movies.join(df_links, on="movieId", how="inner")
df_meta = df_meta.withColumn("id", F.col("id").cast("integer"))
df_joined = df_step1.join(df_meta, df_step1["tmdbId"] == df_meta["id"], how="inner")
# CHỈ ĐỊNH RÕ RÀNG cột lấy từ bảng nào để tránh lỗi trùng tên (Ambiguous)
df_final = df_joined.select(
    df_movies["movieId"],
    df_movies["title"].alias("TenPhim"),
    df_movies["genres"].alias("TheLoai"),
    df_meta["overview"].alias("TomTatNoiDung"),
    df_meta["vote_average"].alias("DiemTMDB"),
    df_meta["budget"].alias("KinhPhi")
)
print("\n--- KẾT QUẢ SAU KHI XỬ LÝ (5 BỘ PHIM ĐẦU TIÊN) ---")
df_final.show(5, truncate=60)
print("Truy cập http://localhost:4040 trên trình duyệt để xem giao diện Spark UI...")
# time.sleep(600) # Dừng 10 phút
spark.stop()
