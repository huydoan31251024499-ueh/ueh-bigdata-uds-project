"""
kafka_producers.py

Hệ thống nạp dữ liệu đa luồng (Multi-threaded Ingestion) cho UDS Big Data.

Luồng 1 — weather_realtime : Open-Meteo API mỗi 5 phút → topic weather_realtime
Luồng 2 — order_stream     : Giả lập đơn UDS liên tục   → topic order_stream

Payload weather : {"timestamp": ISO-8601, "temp": float, "prcp_mm": float, "coco_code": int}
Payload order   : {"order_id": str, "createdAt": ISO-8601, "weight": float,
                   "sender_lat": float, "sender_lng": float,
                   "receiver_lat": float, "receiver_lng": float, "serviceType": str}

Cài thư viện: pip install kafka-python requests
Chạy        : python kafka_producers.py
Dừng        : Ctrl+C

"""

import json
import logging
import random
import threading
import time
import uuid
from datetime import datetime, timezone

import requests
from kafka import KafkaProducer
from kafka.errors import KafkaError

from config import (
    KAFKA_BOOTSTRAP_SERVERS,
    TOPIC_WEATHER, TOPIC_ORDERS,
    OPEN_METEO_URL, HCMC_LAT, HCMC_LNG,
    WEATHER_INTERVAL_SEC, ORDER_INTERVAL_SEC,
    LOG_FORMAT, LOG_LEVEL,
)

#  Logging
logging.basicConfig(level=getattr(logging, LOG_LEVEL), format=LOG_FORMAT)
log = logging.getLogger(__name__)


# Tạo Kafka Producer (thread-safe) 
def make_producer() -> KafkaProducer:
    return KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8"),
        acks="all",
        retries=5,
        retry_backoff_ms=500,
        request_timeout_ms=15000,
    )


#  LUỒNG 1 — WEATHER REALTIME (Open-Meteo API)

def fetch_weather_payload() -> dict | None:
    """
    Gọi Open-Meteo API, đóng gói thành JSON payload sạch.
    Bbox: 10.4–11.2 Lat, 106.3–107.1 Lng (TP.HCM).
    """
    params = {
        "latitude":        HCMC_LAT,
        "longitude":       HCMC_LNG,
        "current_weather": "true",
        "hourly":          "precipitation",
        "forecast_days":   1,
        "timezone":        "Asia/Ho_Chi_Minh",
    }
    try:
        resp = requests.get(OPEN_METEO_URL, params=params, timeout=10)
        resp.raise_for_status()
        data = resp.json()

        cw       = data["current_weather"]
        now_hour = datetime.now().strftime("%Y-%m-%dT%H:00")
        times    = data["hourly"]["time"]
        prcp_arr = data["hourly"]["precipitation"]
        prcp_mm  = float(prcp_arr[times.index(now_hour)]) if now_hour in times else 0.0

        return {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "temp":      round(float(cw["temperature"]), 1),
            "prcp_mm":   round(prcp_mm, 2),
            "coco_code": int(cw["weathercode"]),
        }
    except Exception as e:
        log.error("[WeatherAPI] Lỗi Open-Meteo: %s", e)
        return None


def weather_producer_thread(producer: KafkaProducer, stop_event: threading.Event):
    log.info("[WeatherProducer] Khởi động — gửi mỗi %ds → topic '%s'",
             WEATHER_INTERVAL_SEC, TOPIC_WEATHER)
    while not stop_event.is_set():
        payload = fetch_weather_payload()
        if payload:
            try:
                producer.send(TOPIC_WEATHER, value=payload).get(timeout=10)
                log.info("[SUCCESS][WeatherProducer] → %s | %s",
                         TOPIC_WEATHER, json.dumps(payload))
            except KafkaError as e:
                log.error("[WeatherProducer] Kafka error: %s", e)
        else:
            log.warning("[WeatherProducer] Không lấy được data, bỏ qua lần này.")

        for _ in range(WEATHER_INTERVAL_SEC):
            if stop_event.is_set():
                break
            time.sleep(1)

    log.info("[WeatherProducer] Đã dừng.")


#  LUỒNG 2 — ORDER STREAM (giả lập đơn hàng UDS)

# Bounding box từng quận — khớp với uds_orders_raw.csv
DISTRICT_BOXES = {
    "Binh Thanh": ((10.786, 10.820), (106.695, 106.723)),
    "Thu Duc":    ((10.831, 10.869), (106.736, 106.792)),
    "Quan 7":     ((10.716, 10.755), (106.682, 106.724)),
    "Quan 12":    ((10.853, 10.880), (106.629, 106.665)),
    "Binh Tan":   ((10.761, 10.798), (106.580, 106.620)),
    "Go Vap":     ((10.826, 10.840), (106.662, 106.695)),
    "Quan 1":     ((10.760, 10.778), (106.693, 106.708)),
    "Quan 3":     ((10.776, 10.790), (106.674, 106.690)),
    "Quan 8":     ((10.725, 10.762), (106.662, 106.688)),
    "Tan Phu":    ((10.785, 10.807), (106.614, 106.634)),
    "Nha Be":     ((10.680, 10.715), (106.700, 106.728)),
}

# serviceType và weight khớp đúng với uds_orders_raw.csv
SERVICE_CONFIG = {
    "3h":           {"weight_range": (0.5, 5.0),  "prob": 0.35},
    "5h":           {"weight_range": (0.5, 10.0), "prob": 0.30},
    "ban_tai_nhanh":{"weight_range": (5.0, 20.0), "prob": 0.15},
    "ban_tai_4h":   {"weight_range": (5.0, 30.0), "prob": 0.12},
    "luu_kho":      {"weight_range": (1.0, 15.0), "prob": 0.08},
}

def simulate_order() -> dict:
    """
    Sinh đơn hàng UDS giả lập, bảo toàn phân phối thuộc tính lịch sử.
    Payload đúng theo yêu cầu nhóm trưởng.
    """
    districts = list(DISTRICT_BOXES.keys())
    sender_d  = random.choice(districts)
    recv_d    = random.choice(districts)

    slat_r, slng_r = DISTRICT_BOXES[sender_d]
    rlat_r, rlng_r = DISTRICT_BOXES[recv_d]

    service = random.choices(
        list(SERVICE_CONFIG.keys()),
        weights=[v["prob"] for v in SERVICE_CONFIG.values()],
        k=1
    )[0]
    lo, hi = SERVICE_CONFIG[service]["weight_range"]

    return {
        "order_id":     f"UDS-{uuid.uuid4().hex[:10].upper()}",
        "createdAt":    datetime.now(timezone.utc).isoformat(),
        "weight":       round(random.uniform(lo, hi), 2),
        "sender_lat":   round(random.uniform(*slat_r), 6),
        "sender_lng":   round(random.uniform(*slng_r), 6),
        "receiver_lat": round(random.uniform(*rlat_r), 6),
        "receiver_lng": round(random.uniform(*rlng_r), 6),
        "serviceType":  service,
    }


def order_producer_thread(producer: KafkaProducer, stop_event: threading.Event):
    log.info("[OrderProducer] Khởi động — gửi mỗi %ds → topic '%s'",
             ORDER_INTERVAL_SEC, TOPIC_ORDERS)
    while not stop_event.is_set():
        payload = simulate_order()
        try:
            producer.send(TOPIC_ORDERS, value=payload).get(timeout=10)
            log.info("[SUCCESS][OrderProducer] → %s | order_id: %s | service: %-14s | %.2fkg",
                     TOPIC_ORDERS, payload["order_id"],
                     payload["serviceType"], payload["weight"])
        except KafkaError as e:
            log.error("[OrderProducer] Kafka error: %s", e)
        time.sleep(ORDER_INTERVAL_SEC)

    log.info("[OrderProducer] Đã dừng.")


#  MAIN

def main():
    log.info("=" * 62)
    log.info("  UDS Kafka Multi-threaded Producer")
    log.info("  Broker  : %s", KAFKA_BOOTSTRAP_SERVERS)
    log.info("  Topics  : %s | %s", TOPIC_WEATHER, TOPIC_ORDERS)
    log.info("  Weather : mỗi %d giây (Open-Meteo API)", WEATHER_INTERVAL_SEC)
    log.info("  Orders  : mỗi %d giây (giả lập UDS)", ORDER_INTERVAL_SEC)
    log.info("=" * 62)

    # Kết nối Kafka Broker
    try:
        producer = make_producer()
        log.info("[INIT] Kết nối Kafka broker thành công → %s",
                 KAFKA_BOOTSTRAP_SERVERS)
    except Exception as e:
        log.critical("[INIT] Không kết nối được Kafka: %s", e)
        log.critical("       Kiểm tra: docker-compose up -d kafka zookeeper")
        return

    stop_event = threading.Event()

    # Khởi chạy 2 thread song song (đa luồng không đồng bộ)
    t_weather = threading.Thread(
        target=weather_producer_thread,
        args=(producer, stop_event),
        name="WeatherProducer",
        daemon=True,
    )
    t_orders = threading.Thread(
        target=order_producer_thread,
        args=(producer, stop_event),
        name="OrderProducer",
        daemon=True,
    )

    t_weather.start()
    t_orders.start()
    log.info("[INIT] 2 luồng đã khởi động song song. Nhấn Ctrl+C để dừng.")

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        log.info("\n[SHUTDOWN] Nhận tín hiệu dừng — đang flush message…")
        stop_event.set()
        t_weather.join(timeout=10)
        t_orders.join(timeout=10)
        producer.flush()
        producer.close()
        log.info("[SHUTDOWN] Hoàn tất. Tất cả message đã được ghi vào Kafka.")


if __name__ == "__main__":
    main()
