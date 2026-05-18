import requests
import json
import random
from kafka import KafkaProducer
from time import sleep
from datetime import datetime
from json import dumps

# ==========================================
# CẤU HÌNH HẠ TẦNG KAFKA & TỌA ĐỘ TP.HCM
# ==========================================
KAFKA_SERVER = 'localhost:9092' 
KAFKA_TOPIC = 'weather_realtime'

CITY_LAT = 10.7626   # Vĩ độ trung tâm TP.HCM
CITY_LON = 106.6602  # Kinh độ trung tâm TP.HCM

# Endpoint Open-Meteo: Lấy nhiệt độ (2m), lượng mưa (rain), độ ẩm (relative_humidity_2m) thời gian thực
OPEN_METEO_URL = f"https://api.open-meteo.com/v1/forecast?latitude={CITY_LAT}&longitude={CITY_LON}&current=temperature_2m,relative_humidity_2m,rain,weather_code,surface_pressure,wind_speed_10m"

print("🛰️  [HỆ THỐNG XE DÙ] Đang kết nối tới trục Kafka Broker nội bộ...")

# Khởi tạo Kafka Producer
try:
    producer = KafkaProducer(
        bootstrap_servers=[KAFKA_SERVER],
        value_serializer=lambda x: dumps(x).encode('utf-8'),
        acks='all'
    )
    print("✅ Kết nối hạ tầng Kafka thành công!")
except Exception as e:
    print(f"❌ Lỗi kết nối Kafka: {e}. Vui lòng kiểm tra docker-compose ps!")
    exit(1)

def get_weather_data():
    """Truy vấn dữ liệu real-time từ Open-Meteo (Không cần Key) và map sang UDS Schema"""
    try:
        # Gọi API trực tiếp (Open-Meteo có SSL chuẩn, không lo lỗi Certificate)
        response = requests.get(OPEN_METEO_URL, timeout=5)
        if response.status_code == 200:
            data = response.json()
            current_data = data["current"]
            
            # Bóc tách các chỉ số khí tượng thực tế
            temp_celsius = current_data.get("temperature_2m")
            humidity = current_data.get("relative_humidity_2m")
            rain_mm = current_data.get("rain", 0.0)
            weather_code = current_data.get("weather_code", 0) # Mã trạng thái khí tượng (WMO code)
            
            # Logic xử lý tương tác bối cảnh phi tuyến của hệ thống Xe Dù:
            # Nếu API báo có mưa (rain_mm > 0) hoặc mã thời tiết báo mưa/dông (WMO codes 51-67, 80-86, 95-99)
            is_raining = rain_mm > 0 or weather_code >= 51
            
            if is_raining or humidity > 85:
                # Nếu trời mưa ngập thực tế hoặc độ ẩm quá cao, kích hoạt kịch bản ngập cục bộ
                prcp_mm = rain_mm if rain_mm > 0 else round(random.uniform(5.0, 15.0), 2)
                has_flood = 1
                flood_severity = round(random.uniform(2.5, 4.5), 1) # Ngưỡng cản trở dòng chảy
            else:
                prcp_mm = 0.0
                has_flood = 0
                flood_severity = 0.0

            # Khớp 100% với Schema quy trình PROCESS & CLEAN của đồ án UDS
            weather_payload = {
                "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "location": "TP.HCM City Center",
                "temp": temp_celsius,
                "prcp": prcp_mm,
                "rhum": humidity,
                "pres": current_data.get("surface_pressure"),
                "wspd": current_data.get("wind_speed_10m"),
                "coco": weather_code, # Map trực tiếp WMO Code làm mã trạng thái thời tiết
                "has_flood_event": has_flood,
                "flood_severity_score": flood_severity
            }
            return weather_payload
    except Exception as e:
        print(f"⚠️  [API WARNING] Lỗi kết nối Open-Meteo: {e}")
    return None

# ==========================================
# VÒNG LẶP STREAMING DỮ LIỆU LIVE-DEMO
# ==========================================
print(f"🔥 KÍCH HOẠT PIPELINE: Đang nạp dữ liệu thực tế từ Open-Meteo vào Kafka topic '{KAFKA_TOPIC}'...")
print("⏱️  Tần suất đẩy: 3 giây/bản tin để phục vụ Hội đồng nghiệm thu trực quan.")
print("-" * 80)

try:
    while True:
        weather = get_weather_data()
        if weather:
            # Đẩy dữ liệu vào Kafka Broker
            producer.send(KAFKA_TOPIC, value=weather)
            print(f"🚀 [REAL-TIME -> KAFKA SUCCESS] {weather}")
            producer.flush() 

        # Đẩy liên tục mỗi 3 giây để tạo nhịp nhảy dữ liệu đẹp mắt trên terminal
        sleep(3)
except KeyboardInterrupt:
    print("\n🛑 Dừng nạp dữ liệu do Tech Lead ngắt lệnh.")
finally:
    producer.close()