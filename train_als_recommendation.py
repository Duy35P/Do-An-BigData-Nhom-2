"""
Pipeline Huấn luyện Mô hình Gợi ý Phim (ALS - Collaborative Filtering) bằng PySpark MLlib
- Nguồn: Tầng Silver (Parquet trên HDFS)
- Tối ưu bộ nhớ: Tránh tràn RAM (1.4GB) bằng kỹ thuật lấy mẫu phân tầng, cấu hình rank nhẹ, checkpoint và subset recommendation
- Đích: Tầng Gold (Lưu kết quả Top 5 gợi ý cho người dùng vào HDFS)
"""

import os
import time
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.ml.recommendation import ALS
from pyspark.ml.evaluation import RegressionEvaluator

def create_spark_session():
    temp_dir = "/home/dell/spark_temp"
    os.makedirs(temp_dir, exist_ok=True)
    os.makedirs(f"{temp_dir}/checkpoint", exist_ok=True)

    spark = SparkSession.builder \
        .appName("MovieRecommendation_ALS_Gold") \
        .master("local[*]") \
        .config("spark.local.dir", temp_dir) \
        .config("spark.sql.parquet.compression.codec", "snappy") \
        .config("spark.sql.shuffle.partitions", "20") \
        .config("spark.driver.memory", "1g") \
        .getOrCreate()

    spark.sparkContext.setCheckpointDir(f"{temp_dir}/checkpoint")
    spark.sparkContext.setLogLevel("ERROR")
    return spark

def run_als_pipeline():
    start_time = time.time()
    spark = create_spark_session()

    print("\n" + "="*60)
    print("BƯỚC 1: ĐỌC DỮ LIỆU TỪ SILVER LAYER (PARQUET TRÊN HDFS)")
    print("="*60)

    silver_ratings_path = "hdfs://localhost:9000/lakehouse/silver/ratings"
    silver_movies_path  = "hdfs://localhost:9000/lakehouse/silver/movies"

    df_ratings = spark.read.parquet(silver_ratings_path)
    df_movies  = spark.read.parquet(silver_movies_path)

    # 1. TỐI ƯU TRÁNH TRÀN RAM: Lấy mẫu đại diện ~500.000 dòng đánh giá
    # Đủ đại diện để học ma trận tiềm ẩn (latent factor matrix) mà không tốn RAM
    print("-> Đang lấy mẫu tối ưu bộ nhớ (3% dữ liệu ~ 750,000 đánh giá)...")
    df_sample = df_ratings.sample(withReplacement=False, fraction=0.03, seed=42)

    # Chia tập Train (80%) và Test (20%)
    (train_data, test_data) = df_sample.randomSplit([0.8, 0.2], seed=42)
    print(f"- Tập huấn luyện (Train): {train_data.count():,} dòng")
    print(f"- Tập kiểm thử (Test):   {test_data.count():,} dòng")

    print("\n" + "="*60)
    print("BƯỚC 2: KHỞI TẠO VÀ HUẤN LUYỆN MÔ HÌNH ALS (SPARK MLLIB)")
    print("="*60)
    # Cấu hình siêu tham số siêu nhẹ cho RAM 1.4GB:
    # - rank=10: Kích thước vector đặc trưng ẩn gọn gàng
    # - maxIter=5: 5 vòng lặp tối ưu Alternating Least Squares (nhanh, hội tụ tốt)
    # - coldStartStrategy='drop': Loại bỏ NaN khi gặp người dùng/phim mới
    als = ALS(
        userCol="userId",
        itemCol="movieId",
        ratingCol="rating",
        rank=10,
        maxIter=5,
        regParam=0.1,
        coldStartStrategy="drop",
        nonnegative=True
    )

    print("-> Đang huấn luyện mô hình ALS, vui lòng đợi trong giây lát...")
    model = als.fit(train_data)
    print("✓ Huấn luyện mô hình ALS thành công!")

    print("\n" + "="*60)
    print("BƯỚC 3: ĐÁNH GIÁ ĐỘ CHÍNH XÁC (ĐO CHỈ SỐ RMSE)")
    print("="*60)
    predictions = model.transform(test_data)
    evaluator = RegressionEvaluator(
        metricName="rmse",
        labelCol="rating",
        predictionCol="prediction"
    )
    rmse = evaluator.evaluate(predictions)
    print(f"✓ Sai số trung bình (Root Mean Squared Error - RMSE): {rmse:.4f}")

    print("\n" + "="*60)
    print("BƯỚC 4: SINH GỢI Ý PHIM CHO TẬP NGƯỜI DÙNG (TOP 5 PHIM / USER)")
    print("="*60)
    # TỐI ƯU RAM: Thay vì sinh cho toàn bộ 162,000 users (gây OOM),
    # ta sinh gợi ý cho tập mẫu 500 người dùng tích cực
    sample_users = df_ratings.select("userId").distinct().limit(500)
    print(f"-> Đang tính toán Top 5 phim gợi ý cho 500 người dùng...")
    user_recs = model.recommendForUserSubset(sample_users, 5)

    # Bung mảng gợi ý (explode array) và chuẩn hóa điểm dự đoán về thang [0.5, 5.0]
    recs_flat = user_recs.select(
        F.col("userId"),
        F.explode(F.col("recommendations")).alias("rec")
    ).select(
        F.col("userId"),
        F.col("rec.movieId").alias("movieId"),
        F.round(
            F.when(F.col("rec.rating") > 5.0, 5.0)
             .when(F.col("rec.rating") < 0.5, 0.5)
             .otherwise(F.col("rec.rating")),
            2
        ).alias("DiemDuDoan")
    )

    # Join với bảng phim để hiển thị tên và thể loại phim
    recs_final = recs_flat.join(df_movies, on="movieId", how="inner") \
        .select(
            F.col("userId"),
            F.col("movieId"),
            F.col("TenPhim"),
            F.col("TheLoai"),
            F.col("DiemDuDoan")
        ) \
        .orderBy(F.col("userId"), F.col("DiemDuDoan").desc())

    print("\n--- MẪU KẾT QUẢ GỢI Ý (CHO 2 NGƯỜI DÙNG ĐẦU TIÊN) ---")
    recs_final.show(10, truncate=50)

    print("\n" + "="*60)
    print("BƯỚC 5: LƯU KẾT QUẢ GỢI Ý & MA TRẬN TIỀM ẨN ALS (ITEM FACTORS) VÀO HDFS (TẦNG GOLD)")
    print("="*60)
    output_gold_path = "hdfs://localhost:9000/lakehouse/gold/movie_recommendations"
    print(f"-> Đang ghi kết quả gợi ý theo người dùng vào: {output_gold_path} ...")
    recs_final.write.mode("overwrite").parquet(output_gold_path)
    print("✓ Đã lưu thành công movie_recommendations lên HDFS!")

    output_factors_path = "hdfs://localhost:9000/lakehouse/gold/movie_factors"
    print(f"-> Đang trích xuất ma trận Item Factors của mô hình ALS vào: {output_factors_path} ...")
    df_factors = model.itemFactors.select(
        F.col("id").alias("movieId"),
        F.col("features")
    )
    df_factors.write.mode("overwrite").parquet(output_factors_path)
    print("✓ Đã lưu thành công movie_factors (Item Latent Vectors) lên HDFS Tầng Gold!")

    elapsed = time.time() - start_time
    print("\n" + "="*60)
    print(f"HOÀN THÀNH TOÀN BỘ TIẾN TRÌNH TRONG: {elapsed:.1f} GIÂY")
    print("="*60)

    spark.stop()

if __name__ == "__main__":
    run_als_pipeline()
