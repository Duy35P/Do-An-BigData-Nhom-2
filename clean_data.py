"""
Pipeline làm sạch dữ liệu (Data Cleaning & Silver Layer) bằng PySpark
- Nguồn: Tầng Bronze (CSV thô trên HDFS)
- Đích: Tầng Silver (Parquet chuẩn hoá trên HDFS)
"""

import os
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.types import IntegerType, DoubleType, FloatType

def create_spark_session():
    # Sử dụng thư mục tạm trên ổ cứng (940GB trống) thay vì /tmp (ramdisk chỉ có 700MB)
    temp_dir = "/home/dell/spark_temp"
    os.makedirs(temp_dir, exist_ok=True)

    return SparkSession.builder \
        .appName("MovieData_Cleaning_Silver") \
        .master("local[*]") \
        .config("spark.local.dir", temp_dir) \
        .config("spark.sql.parquet.compression.codec", "snappy") \
        .config("spark.sql.shuffle.partitions", "20") \
        .getOrCreate()

def clean_movies(spark):
    print("\n" + "="*50)
    print("1. BẮT ĐẦU LÀM SẠCH DỮ LIỆU PHIM (MOVIES & METADATA)")
    print("="*50)

    # 1. Đọc dữ liệu thô từ HDFS
    movies_path = "hdfs://localhost:9000/lakehouse/bronze/movielens/movies.csv"
    links_path  = "hdfs://localhost:9000/lakehouse/bronze/movielens/links.csv"
    meta_path   = "hdfs://localhost:9000/lakehouse/bronze/movielens/movies_metadata.csv"

    print("Đang đọc dữ liệu từ Bronze Layer...")
    df_movies_raw = spark.read.csv(movies_path, header=True, inferSchema=True)
    df_links_raw  = spark.read.csv(links_path, header=True, inferSchema=True)
    df_meta_raw   = spark.read.csv(meta_path, header=True, inferSchema=True, quote='"', escape='"', multiLine=True)

    count_raw = df_movies_raw.count()
    print(f"- Số lượng phim thô ban đầu: {count_raw:,} bản ghi")

    # 2. Tiền xử lý chọn lọc các cột cần thiết để TRÁNH XUNG ĐỘT TÊN CỘT (title, genres, id)
    df_movies = df_movies_raw.select(
        F.col("movieId").cast(IntegerType()).alias("movieId"),
        F.trim(F.col("title")).alias("TenPhim"),
        F.col("genres").alias("genres_raw")
    ).filter(F.col("movieId").isNotNull())

    df_links = df_links_raw.select(
        F.col("movieId").cast(IntegerType()).alias("link_movieId"),
        F.col("tmdbId").cast(IntegerType()).alias("tmdbId")
    ).filter(F.col("link_movieId").isNotNull() & F.col("tmdbId").isNotNull())

    df_meta = df_meta_raw.select(
        F.col("id").cast(IntegerType()).alias("meta_id"),
        F.col("overview").alias("TomTatNoiDung"),
        F.col("poster_path").alias("poster_path"),
        F.col("vote_average").cast(DoubleType()).alias("vote_raw"),
        F.col("budget").cast(DoubleType()).alias("budget_raw"),
        F.to_date(F.col("release_date"), "yyyy-MM-dd").alias("release_date")
    ).filter(F.col("meta_id").isNotNull())

    # 3. Join các DataFrame đã được đổi tên rõ ràng
    df_joined = df_movies \
        .join(df_links, df_movies["movieId"] == df_links["link_movieId"], how="left") \
        .join(df_meta, df_links["tmdbId"] == df_meta["meta_id"], how="left")

    # 4. Chuẩn hóa và làm sạch logic
    # - Trích xuất năm phát hành từ TenPhim: ví dụ "Toy Story (1995)"
    extracted_year = F.regexp_extract(F.col("TenPhim"), r"\((\d{4})\)$", 1).cast(IntegerType())
    meta_year = F.year(F.col("release_date"))
    final_year = F.coalesce(extracted_year, meta_year)

    # - Làm sạch điểm TMDB và kinh phí
    clean_vote = F.when(
        (F.col("vote_raw") >= 0) & (F.col("vote_raw") <= 10),
        F.col("vote_raw")
    ).otherwise(F.lit(None))

    clean_budget = F.when(
        F.col("budget_raw") > 0,
        F.col("budget_raw")
    ).otherwise(F.lit(None))

    # - Chuẩn hóa thể loại và tóm tắt
    clean_genres = F.when(
        (F.col("genres_raw").isNull()) | (F.col("genres_raw") == "(no genres listed)"),
        F.lit("Chưa rõ")
    ).otherwise(F.col("genres_raw"))

    clean_overview = F.when(
        F.col("TomTatNoiDung").isNull() | (F.trim(F.col("TomTatNoiDung")) == ""),
        F.lit("Không có tóm tắt")
    ).otherwise(F.col("TomTatNoiDung"))

    df_movies_cleaned = df_joined.select(
        F.col("movieId"),
        F.col("TenPhim"),
        final_year.alias("NamPhatHanh"),
        clean_genres.alias("TheLoai"),
        clean_vote.alias("DiemTMDB"),
        clean_budget.alias("KinhPhi"),
        clean_overview.alias("TomTatNoiDung"),
        F.col("poster_path")
    ).filter(F.col("movieId").isNotNull() & F.col("TenPhim").isNotNull()) \
     .dropDuplicates(["movieId"])

    count_cleaned = df_movies_cleaned.count()
    print(f"- Số lượng phim sau khi làm sạch: {count_cleaned:,} bản ghi")

    # 5. Ghi Parquet vào HDFS (Silver Layer)
    output_movies_path = "hdfs://localhost:9000/lakehouse/silver/movies"
    print(f"-> Đang ghi file Parquet vào: {output_movies_path} ...")
    df_movies_cleaned.write.mode("overwrite").parquet(output_movies_path)
    print("✓ Đã ghi thành công dữ liệu Phim vào HDFS (Silver/Parquet)!")

    print("\n--- MẪU DỮ LIỆU PHIM ĐÃ LÀM SẠCH (5 BẢN GHI ĐẦU) ---")
    df_movies_cleaned.show(5, truncate=50)
    return df_movies_cleaned


def clean_ratings(spark):
    print("\n" + "="*50)
    print("2. BẮT ĐẦU LÀM SẠCH DỮ LIỆU ĐÁNH GIÁ (RATINGS)")
    print("="*50)

    ratings_path = "hdfs://localhost:9000/lakehouse/bronze/movielens/ratings.csv"
    print("Đang đọc dữ liệu Ratings từ Bronze Layer...")
    df_ratings = spark.read.csv(ratings_path, header=True, inferSchema=True)

    # 1. Ép kiểu và lọc các giá trị không hợp lệ
    # - rating phải nằm trong [0.5, 5.0]
    # - chuyển timestamp sang định dạng Datetime dễ truy vấn
    # Lưu ý: Bộ dữ liệu MovieLens 25M đã đảm bảo mỗi cặp (userId, movieId) là duy nhất.
    # Sử dụng narrow transformation (select + filter) giúp Spark stream trực tiếp sang Parquet
    # mà không cần shuffle tốn RAM / disk spill.
    df_ratings_cleaned = df_ratings \
        .select(
            F.col("userId").cast(IntegerType()).alias("userId"),
            F.col("movieId").cast(IntegerType()).alias("movieId"),
            F.col("rating").cast(FloatType()).alias("rating"),
            F.to_timestamp(F.from_unixtime(F.col("timestamp"))).alias("thoiGianDanhGia")
        ) \
        .filter(
            F.col("userId").isNotNull() &
            F.col("movieId").isNotNull() &
            (F.col("rating") >= 0.5) & (F.col("rating") <= 5.0)
        )

    # 2. Ghi Parquet vào HDFS (Silver Layer)
    output_ratings_path = "hdfs://localhost:9000/lakehouse/silver/ratings"
    print(f"-> Đang ghi file Parquet vào: {output_ratings_path} ...")
    df_ratings_cleaned.write.mode("overwrite").parquet(output_ratings_path)
    print("✓ Đã ghi thành công dữ liệu Ratings vào HDFS (Silver/Parquet)!")

    print("\n--- MẪU DỮ LIỆU RATINGS ĐÃ LÀM SẠCH (5 BẢN GHI ĐẦU) ---")
    df_ratings_cleaned.show(5)
    return df_ratings_cleaned


if __name__ == "__main__":
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("ERROR")

    try:
        clean_movies(spark)
        clean_ratings(spark)
        print("\n" + "="*50)
        print("HOÀN THÀNH TOÀN BỘ QUY TRÌNH LÀM SẠCH & LƯU HDFS!")
        print("="*50)
    finally:
        spark.stop()
