from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType
from pyspark.sql.functions import col, from_json

# Khởi tạo Spark
spark = SparkSession.builder \
    .appName("MovieLogStreaming_Production") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1") \
    .master("local[*]") \
    .getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

# Định nghĩa Schema
schema = StructType([
    StructField("timestamp", StringType(), True),
    StructField("user_id", IntegerType(), True),
    StructField("movie_id", IntegerType(), True),
    StructField("action", StringType(), True),
    StructField("rating", DoubleType(), True)
])

# Đọc từ Kafka (thêm failOnDataLoss=false để tránh lỗi lệch offset khi khởi động lại Kafka)
df_kafka = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "movie_logs") \
    .option("startingOffsets", "latest") \
    .option("failOnDataLoss", "false") \
    .load()

# Bóc tách JSON
df_parsed = df_kafka.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")) \
    .select("data.*")

print("🚀 Hệ thống đang hứng dữ liệu từ Kafka và LƯU TRỮ XUỐNG HDFS...")

# Ghi dữ liệu thực tế xuống HDFS
query = df_parsed.writeStream \
    .outputMode("append") \
    .format("csv") \
    .option("header", "true") \
    .option("path", "hdfs://localhost:9000/lakehouse/silver/streaming_logs/") \
    .option("checkpointLocation", "hdfs://localhost:9000/lakehouse/checkpoints/streaming_logs/") \
    .start()

query.awaitTermination()
