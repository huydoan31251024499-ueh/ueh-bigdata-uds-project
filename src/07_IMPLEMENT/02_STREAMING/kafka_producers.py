"""
Kafka Producers - UDS Streaming System

2 Streams:
- weather_realtime → Kafka topic: weather_realtime
- order_stream     → Kafka topic: order_stream

Compatible with:
- Docker Kafka (kafka:9092)
- Local Python (localhost:29092)
"""

import glob
import json
import logging
import os
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
    TOPIC_WEATHER,
    TOPIC_ORDERS,
    OPEN_METEO_URL,
    HCMC_LAT,
    HCMC_LNG,
    WEATHER_INTERVAL_SEC,
    ORDER_INTERVAL_SEC,
    LOG_FORMAT,
    LOG_LEVEL
)


# Topics
TOPIC_WEATHER = "weather_realtime"
TOPIC_ORDERS = "order_stream"

# =============================
# WEATHER API
# =============================
OPEN_METEO_URL = "https://api.open-meteo.com/v1/forecast"
HCMC_LAT = 10.82
HCMC_LNG = 106.63

# =============================
# INTERVAL
# =============================
WEATHER_INTERVAL_SEC = 300   # 5 phút
ORDER_INTERVAL_SEC = 2       # 2 giây

# =============================
# LOGGING
# =============================
LOG_FORMAT = "%(asctime)s | %(levelname)s | %(message)s"
LOG_LEVEL = "INFO"


# =============================
# LOGGING
# =============================
logging.basicConfig(level=getattr(logging, LOG_LEVEL), format=LOG_FORMAT)
log = logging.getLogger(__name__)

# =============================
# PATH CONFIG (DOCKER SAFE)
# =============================
if os.path.exists("./stream_input"):
    STREAM_INPUT_DIR = "./stream_input"
else:
    STREAM_INPUT_DIR = "/app/stream_input"
    
# =============================
# KAFKA PRODUCER
# =============================
def make_producer() -> KafkaProducer:
    return KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v, default=str).encode("utf-8"),
        acks="all",
        retries=3
    )

# =============================
# WEATHER STREAM
# =============================
def fetch_weather_payload():
    try:
        params = {
            "latitude": HCMC_LAT,
            "longitude": HCMC_LNG,
            "current_weather": "true",
            "hourly": "precipitation",
            "forecast_days": 1,
            "timezone": "Asia/Ho_Chi_Minh",
        }

        resp = requests.get(OPEN_METEO_URL, params=params, timeout=8)
        resp.raise_for_status()
        data = resp.json()

        cw = data["current_weather"]
        now_hour = datetime.now().strftime("%Y-%m-%dT%H:00")

        times = data["hourly"]["time"]
        prcp_arr = data["hourly"]["precipitation"]

        prcp_mm = float(prcp_arr[times.index(now_hour)]) if now_hour in times else 0.0

        return {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "temp": round(float(cw["temperature"]), 1),
            "prcp_mm": round(prcp_mm, 2),
            "coco_code": int(cw["weathercode"])
        }

    except Exception as e:
        log.error("[WeatherAPI Error] %s", e)
        return None


def weather_thread(producer, stop_event):
    log.info("[Weather] Started → topic: %s", TOPIC_WEATHER)

    while not stop_event.is_set():
        payload = fetch_weather_payload()

        if payload:
            try:
                producer.send(TOPIC_WEATHER, value=payload)
                log.info("[Weather] Sent → %s", payload)
            except KafkaError as e:
                log.error("[Weather Kafka Error] %s", e)

        time.sleep(WEATHER_INTERVAL_SEC)

    log.info("[Weather] Stopped")


# =============================
# ORDER STREAM
# =============================

SERVICE_CONFIG = {
    "3h": {"weight_range": (0.5, 5.0), "prob": 0.6},
    "5h": {"weight_range": (0.5, 10.0), "prob": 0.4},
}

DISTRICT_BOXES = {
    "Binh Thanh": ((10.78, 10.82), (106.69, 106.72)),
    "Thu Duc":    ((10.83, 10.87), (106.73, 106.79)),
    "Quan 7":     ((10.71, 10.75), (106.68, 106.72)),
}

def random_coords():
    d = random.choice(list(DISTRICT_BOXES.keys()))
    lat_r, lng_r = DISTRICT_BOXES[d]
    return (
        round(random.uniform(*lat_r), 6),
        round(random.uniform(*lng_r), 6)
    )


def enrich_order(raw):
    service = random.choices(
        list(SERVICE_CONFIG.keys()),
        weights=[v["prob"] for v in SERVICE_CONFIG.values()],
        k=1
    )[0]

    weight_range = SERVICE_CONFIG[service]["weight_range"]

    slat, slng = random_coords()
    rlat, rlng = random_coords()

    return {
        "order_id": raw.get("order_id", f"UDS-{uuid.uuid4().hex[:6]}"),
        "createdAt": raw.get("createdAt", datetime.now(timezone.utc).isoformat()),

        "distance_km": raw.get("distance_km", 0.0),
        "traffic_congestion_index": raw.get("traffic_congestion_index", 0.0),

        "weight": round(random.uniform(*weight_range), 2),

        "sender_lat": slat,
        "sender_lng": slng,
        "receiver_lat": rlat,
        "receiver_lng": rlng,

        "serviceType": service
    }


def order_thread(producer, stop_event):
    log.info("[Order] Started → scanning %s", STREAM_INPUT_DIR)

    processed = set()
    os.makedirs(STREAM_INPUT_DIR, exist_ok=True)

    while not stop_event.is_set():
        files = sorted(glob.glob(os.path.join(STREAM_INPUT_DIR, "order_*.json")))

        for path in files:
            if path in processed:
                continue

            try:
                with open(path, "r", encoding="utf-8") as f:
                    raw = json.load(f)

                payload = enrich_order(raw)

                producer.send(TOPIC_ORDERS, value=payload)

                log.info(
                    "[Order] Sent → %s | svc=%s | %.2fkg",
                    payload["order_id"],
                    payload["serviceType"],
                    payload["weight"]
                )

                processed.add(path)

                try:
                    os.remove(path)
                    processed.discard(path)
                except:
                    pass

            except Exception as e:
                log.warning("[Order Error] %s | file=%s", e, path)
                processed.add(path)

        time.sleep(ORDER_INTERVAL_SEC)

    log.info("[Order] Stopped")


# =============================
# MAIN
# =============================
def main():
    log.info("=== UDS Kafka Producer START ===")

    try:
        producer = make_producer()
        log.info("[INIT] Kafka connected → %s", KAFKA_BOOTSTRAP_SERVERS)
    except Exception as e:
        log.critical("[INIT FAIL] %s", e)
        return

    stop_event = threading.Event()

    t1 = threading.Thread(target=weather_thread, args=(producer, stop_event), daemon=True)
    t2 = threading.Thread(target=order_thread, args=(producer, stop_event), daemon=True)

    t1.start()
    t2.start()

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        log.info("Shutting down...")
        stop_event.set()

        t1.join(5)
        t2.join(5)

        producer.flush()
        producer.close()

        log.info("Shutdown complete")


if __name__ == "__main__":
    main()