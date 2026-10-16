#!/bin/bash
# ==============================================================================
# SCRIPT DỪNG TOÀN BỘ CỤM HỆ THỐNG BIG DATA LAKEHOUSE
# ==============================================================================

echo "=========================================================="
echo "🛑 ĐANG DỪNG HỆ THỐNG LAKEHOUSE BIG DATA..."
echo "=========================================================="

export JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64
export HADOOP_HOME=/usr/local/hadoop
export PATH=$JAVA_HOME/bin:$HADOOP_HOME/bin:$HADOOP_HOME/sbin:$PATH

# 1. Dừng FastAPI Web Server
echo "[1/4] 🎬 Dừng FastAPI Uvicorn Server..."
pkill -f "uvicorn main:app" > /dev/null 2>&1 || true

# 2. Dừng Kafka
echo "[2/4] 📡 Dừng Kafka Broker..."
/home/dell/kafka_2.12-3.6.2/bin/kafka-server-stop.sh > /dev/null 2>&1 || true
pkill -f "kafka.Kafka" > /dev/null 2>&1 || true

# 3. Dừng DataNode 2
echo "[3/4] 🐘 Dừng DataNode 2..."
$HADOOP_HOME/bin/hdfs --config /usr/local/hadoop/etc/hadoop2 --daemon stop datanode > /dev/null 2>&1 || true

# 4. Dừng Hadoop HDFS
echo "[4/4] 🐘 Dừng Hadoop HDFS (NameNode + DataNode 1)..."
$HADOOP_HOME/sbin/stop-dfs.sh > /dev/null 2>&1 || true

sleep 2
echo "📋 Tiến trình còn lại:"
jps

echo "=========================================================="
echo "✓ Đã dừng toàn bộ hệ thống an toàn!"
echo "=========================================================="
