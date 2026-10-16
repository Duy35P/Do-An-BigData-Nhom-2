import time
import json
import random
from datetime import datetime
from kafka import KafkaProducer

# Khởi tạo Producer kết nối vào cổng 9092 của Kafka
producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

actions = ['view', 'click', 'rate', 'add_to_watchlist']

print("🚀 Đang bắn dữ liệu giả lập vào Kafka (Bấm Ctrl+C để dừng)...")
try:
    while True:
        # Sinh ngẫu nhiên dữ liệu người dùng tương tác với Web Phim
        action = random.choice(actions)
        log_data = {
            "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "user_id": random.randint(1, 1000),     # User ID từ 1 đến 1000
            "movie_id": random.randint(1, 10000),   # Movie ID từ 1 đến 10000
            "action": action
        }
        
        # Nếu là hành động đánh giá phim thì sinh thêm điểm số (1.0 đến 5.0)
        if action == 'rate':
            log_data["rating"] = round(random.uniform(1.0, 5.0), 1)

        # Gửi cục dữ liệu (JSON) này vào topic 'movie_logs'
        producer.send('movie_logs', value=log_data)
        print(f"Đã gửi: {log_data}")
        
        # Đợi 1 giây rồi bắn tiếp
        time.sleep(1)
        
except KeyboardInterrupt:
    print("\n🛑 Đã dừng hệ thống bắn dữ liệu.")
finally:
    producer.close()
