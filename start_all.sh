#!/bin/bash
# ==============================================================================
# SCRIPT KHỞI ĐỘNG TỔNG THỂ TOÀN BỘ CỤM HỆ THỐNG BIG DATA LAKEHOUSE
# - Hadoop HDFS (NameNode + SecondaryNameNode + DataNode 1)
# - Hadoop DataNode 2 (Cụm 2 DataNodes phân tán)
# - Apache Kafka KRaft Broker (Port 9092, Topic: movie_logs)
# - FastAPI Web Application (Port 8000, Uvicorn)
# ==============================================================================

echo "=========================================================="
echo "🚀 BẮT ĐẦU KHỞI ĐỘNG HỆ THỐNG LAKEHOUSE BIG DATA"
echo "=========================================================="

export JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64
export HADOOP_HOME=/usr/local/hadoop
export PATH=$JAVA_HOME/bin:$HADOOP_HOME/bin:$HADOOP_HOME/sbin:$PATH

# 1. Bật Hadoop HDFS
echo "[1/4] 🐘 Đang khởi động Hadoop HDFS (NameNode + DataNode 1)..."
$HADOOP_HOME/sbin/start-dfs.sh
sleep 3
$HADOOP_HOME/bin/hdfs dfsadmin -safemode leave > /dev/null 2>&1 || true

# 2. Bật DataNode 2 (Mô phỏng 2 DataNodes)
echo "[2/4] 🐘 Đang khởi động DataNode 2 (Cluster 2 Live Nodes)..."
mkdir -p /tmp/hadoop2-pids
mkdir -p /usr/local/hadoop/logs2
$HADOOP_HOME/bin/hdfs --config /usr/local/hadoop/etc/hadoop2 --daemon start datanode
sleep 2

# 3. Bật Apache Kafka KRaft
echo "[3/4] 📡 Đang khởi động Apache Kafka KRaft Broker (9092)..."
cd /home/dell/kafka_2.12-3.6.2
if ! pgrep -f "kafka.Kafka" > /dev/null; then
    mkdir -p /tmp/kraft-combined-logs
    KAFKA_CLUSTER_ID="$(bin/kafka-storage.sh random-uuid)"
    bin/kafka-storage.sh format -t $KAFKA_CLUSTER_ID -c config/kraft/server.properties > /dev/null 2>&1 || true
    bin/kafka-server-start.sh -daemon config/kraft/server.properties
    sleep 3
    bin/kafka-topics.sh --create --topic movie_logs --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1 > /dev/null 2>&1 || true
    echo "✓ Kafka KRaft đã sẵn sàng tại port 9092!"
else
    echo "✓ Kafka KRaft đang chạy!"
fi
cd /home/dell

# 4. Kiểm tra và khởi động FastAPI Web Server
echo "[4/4] 🎬 Khởi động Web Recommender (FastAPI Uvicorn port 8000)..."
pkill -f "uvicorn main:app" > /dev/null 2>&1 || true
cd /home/dell
source /home/dell/movie_env/bin/activate
nohup uvicorn main:app --host 0.0.0.0 --port 8000 > /home/dell/uvicorn.log 2>&1 &
sleep 2

echo "=========================================================="
echo "🎉 HỆ THỐNG ĐÃ KHỞI ĐỘNG HOÀN TẤT 100%!"
echo "----------------------------------------------------------"
echo "🌐 1. Web Gợi Ý Phim:       http://localhost:8000"
echo "🐘 2. Hadoop NameNode UI:   http://localhost:9870 (Tab Datanodes: 2 Live Nodes)"
echo "📡 3. Kafka Broker:         localhost:9092 (Topic: movie_logs)"
echo "=========================================================="
jps
