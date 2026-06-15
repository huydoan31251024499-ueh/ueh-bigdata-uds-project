import json
import time
import random
import os
from datetime import datetime, timezone

# =============================
# CONFIG
# =============================
STREAM_DIR = os.getenv("STREAM_INPUT_DIR", "./stream_input")
os.makedirs(STREAM_DIR, exist_ok=True)

print("=== [SIMULATOR] UDS ORDER STREAM STARTED ===")

order_id = 5000

# =============================
# HELPER FUNCTIONS
# =============================

def random_coords():
    """
    Sinh tọa độ ngẫu nhiên trong TP.HCM (approx bounding box)
    """
    lat = random.uniform(10.70, 10.85)
    lng = random.uniform(106.60, 106.75)
    return round(lat, 6), round(lng, 6)


def choose_service():
    return random.choices(
        ["3h", "5h"],
        weights=[0.6, 0.4],
        k=1
    )[0]


def choose_scenario():
    """
    Scenario realistic distribution
    """
    return random.choices(
        ["normal", "rain", "flood", "rain_flood"],
        weights=[0.6, 0.2, 0.1, 0.1],
        k=1
    )[0]


def generate_order(context):
    """
    Generate order data aligned with ML feature expectations
    """

    if context == "rain_flood":
        distance = random.uniform(5.0, 15.0)
        traffic = random.uniform(7.0, 9.5)

    elif context == "flood":
        distance = random.uniform(3.0, 10.0)
        traffic = random.uniform(6.0, 9.0)

    elif context == "rain":
        distance = random.uniform(2.0, 8.0)
        traffic = random.uniform(4.0, 7.0)

    else:  # normal
        distance = random.uniform(1.0, 6.0)
        traffic = random.uniform(2.0, 5.0)

    slat, slng = random_coords()
    rlat, rlng = random_coords()

    return {
        "distance_km": round(distance, 2),
        "traffic_congestion_index": round(traffic, 2),
        "sender_lat": slat,
        "sender_lng": slng,
        "receiver_lat": rlat,
        "receiver_lng": rlng,
        "serviceType": choose_service(),
        "weight": round(random.uniform(0.5, 10.0), 2)
    }


# =============================
# MAIN LOOP
# =============================

while True:
    order_id += 1
    now = datetime.now(timezone.utc)
    context = choose_scenario()

    order = generate_order(context)

    payload = {
        "order_id": f"UDS-{order_id}",
        "createdAt": now.isoformat(),
        **order
    }

    # ✅ Lưu file JSON riêng cho từng event
    filepath = os.path.join(STREAM_DIR, f"order_{order_id}.json")

    try:
        with open(filepath, "w", encoding="utf-8") as f:
            json.dump(payload, f)

        print(
            f"[ORDER] {payload['order_id']} | "
            f"svc={payload['serviceType']} | "
            f"{payload['distance_km']}km | "
            f"traffic={payload['traffic_congestion_index']} | "
            f"context={context}"
        )

    except Exception as e:
        print(f"[ERROR] Cannot write file: {e}")

    time.sleep(2)  # 2s per event
