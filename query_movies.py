from pyspark.sql import SparkSession

# Khởi tạo Spark
spark = SparkSession.builder.appName("SparkSQL_Demo").master("local[*]").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

print("Đang nạp dữ liệu từ HDFS vào các bảng SQL ảo...")
# Đọc file và đăng ký thành các bảng SQL (Temporary View)
spark.read.csv("hdfs://localhost:9000/lakehouse/bronze/movielens/movies.csv", header=True, inferSchema=True).createOrReplaceTempView("movies")
spark.read.csv("hdfs://localhost:9000/lakehouse/bronze/movielens/links.csv", header=True, inferSchema=True).createOrReplaceTempView("links")
spark.read.csv("hdfs://localhost:9000/lakehouse/bronze/movielens/movies_metadata.csv", header=True, inferSchema=True, quote='"', escape='"', multiLine=True).createOrReplaceTempView("metadata")

print("\n--- BẮT ĐẦU CHẠY CÂU TRUY VẤN SQL ---")
# Dùng thuần SQL để JOIN 3 bảng và lấy ra Top 5 phim hay nhất có kinh phí cao
ket_qua = spark.sql("""
    SELECT 
        m.title AS TenPhim, 
        meta.vote_average AS DiemSo, 
        meta.budget AS KinhPhi
    FROM movies m
    JOIN links l ON m.movieId = l.movieId
    JOIN metadata meta ON l.tmdbId = CAST(meta.id AS INT)
    WHERE meta.vote_average >= 8.0 AND meta.budget > 10000000
    ORDER BY meta.budget DESC
    LIMIT 5
""")

ket_qua.show(truncate=False)

spark.stop()
