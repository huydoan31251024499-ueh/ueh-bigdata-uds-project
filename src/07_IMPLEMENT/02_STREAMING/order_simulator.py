import json
import time
import random
from datetime import datetime

print("=== [SIMULATOR] BẮT ĐẦU GIẢ LẬP ĐƠN HÀNG ĐA BỐI CẢNH (2026) ===")
order_id = 5000

# Tạo sẵn thư mục input stream
import os
os.makedirs("/home/dntt/Desktop/stream_input", exist_ok=True)

while True:
    order_id += 1
    current_time = datetime.now()
    
    # 1. Cơ chế Scenario-based: Ép bối cảnh cực đoan "Mưa bão - Ngập lụt" sau mỗi 4 đơn hàng
    if order_id % 4 == 0:
        context = "rain_flood"
        distance = random.uniform(5.0, 15.0)                  # Quãng đường dài hơn
        traffic_index = random.uniform(75.0, 98.0)             # Tắc đường nghiêm trọng
        prcp_mm = random.uniform(15.0, 45.0)                   # Mưa rất to
        flood_depth = random.uniform(20.0, 50.0)               # Ngập sâu nặng
        condition_label = "Ngập lụt - Bão lớn"
    else:
        context = "normal"
        distance = random.uniform(1.0, 6.0)
        traffic_index = random.uniform(15.0, 45.0)
        prcp_mm = random.uniform(0.0, 2.0)
        flood_depth = 0.0
        condition_label = "Bình thường"

    # 2. Đóng gói payload Đơn hàng
    order_payload = {
        "id": str(order_id),
        "createdAt": current_time.strftime("%Y-%m-%d %H:%M:%S"),
        "distance_km": round(distance, 2),
        "traffic_congestion_index": round(traffic_index, 2),
        "order_hour": current_time.hour
    }

    # 3. Đóng gói payload Thời tiết (Gắn tag temporal theo khung giờ)
    weather_payload = {
        "hour_timestamp": current_time.strftime("%Y-%m-%d %H:00:00"),
        "prcp_mm": round(prcp_mm, 2),
        "avg_flood_depth_cm": round(flood_depth, 2),
        "condition_label": condition_label
    }

    # Xuất file JSON giả lập vào phân vùng hệ thống để Spark quét
    with open(f"/home/dntt/Desktop/stream_input/order_{order_id}.json", "w") as f:
        json.dump({**order_payload, **weather_payload}, f) # Hợp nhất dữ liệu luồng

    print(f"[Sinh Đơn] ID: {order_id} | Bối cảnh: {condition_label} | {distance} km")
    time.sleep(2) # Cứ 2 giây phát sinh một sự kiện mới
