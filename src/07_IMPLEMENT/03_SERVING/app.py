from fastapi import FastAPI
from pydantic import BaseModel
import json
import os

app = FastAPI()

# Định nghĩa cấu trúc dữ liệu chuẩn
class OrderData(BaseModel):
    order_id: str
    distance: float
    original_eta: float
    weather_adaptive_eta: float
    current_context: str

DB_FILE = "live_orders.json"

# Hàm phụ trợ để lưu dữ liệu vào file json làm kho chứa tạm thời
def save_to_db(data):
    orders = []
    if os.path.exists(DB_FILE):
        try:
            with open(DB_FILE, "r", encoding="utf-8") as f:
                orders = json.load(f)
        except:
            orders = []
    orders.insert(0, data) # Đơn mới nhất lên đầu
    with open(DB_FILE, "w", encoding="utf-8") as f:
        json.dump(orders, f, ensure_ascii=False, indent=4)

@app.post("/api/eta")
async def receive_order_stream(order: OrderData):
    new_order = {
        "Mã đơn hàng": order.order_id,
        "Quãng đường (km)": order.distance,
        "ETA Gốc (Phút)": round(order.original_eta, 1),
        "ETA Co giãn (Phút)": round(order.weather_adaptive_eta, 1),
        "Biến động trễ (Phút)": round(order.weather_adaptive_eta - order.original_eta, 1),
        "Bối cảnh thời tiết": order.current_context
    }
    save_to_db(new_order)
    return {"status": "success", "message": "Đã nhận luồng dữ liệu thành công!"}

if __name__ == "__main__":
    import uvicorn
    # Chạy server tại cổng 8000
    uvicorn.run(app, host="127.0.0.1", port=8000)
