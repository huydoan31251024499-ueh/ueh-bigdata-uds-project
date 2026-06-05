### **CHƯƠNG 2 & 3: KỸ THUẬT STREAMING**

**Thành viên thực hiện:** Lê Ngọc Anh Hùng

---

#### **1. Kiến trúc phân vùng lưu trữ trên GitHub (`src/07_IMPLEMENT/`)**

Toàn bộ mã nguồn do Hùng xây dựng sẽ được quản lý tập trung tại hai thư mục con nhằm tách biệt rõ ràng vai trò Thu thập mở rộng (Batch/Cào dữ liệu) và Luồng thời gian thực (Streaming):

```
└── src
    └── 07_IMPLEMENT
        ├── 01_PREPARE_EXT
        │   ├── get_flood_points.py     # Script trích xuất danh sách 159 điểm ngập chính thống
        │   └── get_market_traffic.py   # Script xử lý dữ liệu mật độ giao thông khu vực chợ
        └── 02_STREAMING
            ├── config.py               # Quản lý tập trung biến môi trường và thông số Kafka Broker
            └── kafka_producers.py      # Mã nguồn nạp dữ liệu đa luồng (Multi-threaded Ingestion)

```

---

#### **2. Chi tiết công việc kỹ thuật cụ thể (Nhiệm vụ của Hùng)**

##### **Bước 1: Làm giàu dữ liệu tĩnh ngoại cảnh (`01_PREPARE_EXT`)**

* **Nhiệm vụ:** Viết script `get_flood_points.py` thực hiện cấu trúc hóa danh mục 159 điểm đen ngập lụt tại TP.HCM từ dữ liệu chính thống (Sở Xây dựng / Trung tâm Quản lý Hạ tầng Kỹ thuật / Ứng dụng UDI Maps).
* **Yêu cầu kỹ thuật:** Định dạng đầu ra bắt buộc là file `hcmc_flood_points_raw.csv` lưu trữ trên HDFS với Schema nghiêm ngặt: `flood_point_id`, `street_name`, `district`, `latitude`, `longitude`, và `nominal_severity` (mức độ nghiêm trọng danh nghĩa của điểm ngập). Tập dữ liệu này làm bệ đỡ không gian (Spatial Context) để Spark đối chiếu tọa độ đơn hàng sau này.

##### **Bước 2: Phát triển trục nạp đa luồng thời gian thực (`kafka_producers.py`)**

Hùng lập trình script hệ thống `kafka_producers.py` sử dụng thư viện `kafka-python` kết hợp cơ chế đa luồng (`threading`) để vận hành song song hai tiến trình nạp sự kiện không đồng bộ với thông lượng cao, không gây nghẽn mạch (`Network Latency`):

**Luồng 1 - Sự kiện khí tượng thực (`weather_realtime`):**
* Lập trình hàm kết nối, tự động gửi HTTP Request định kỳ 5 phút/lần đến API (OpenWeatherMap hoặc Open-Meteo) truy vấn trạng thái thực của khí tượng TP.HCM năm 2026 (Giới hạn không gian: 10.4–11.2 Lat, 106.3–107.1 Lng).
* Đóng gói dữ liệu trả về thành chuỗi JSON bán cấu trúc sạch (Semi-structured JSON Payload) đẩy vào Kafka với định dạng:
`{"timestamp": "ISO-8601", "temp": float, "prcp_mm": float, "coco_code": int}`.


**Luồng 2 - Sự kiện dòng đơn hàng (`order_stream`):**
* Thiết lập cổng tiếp nhận cấu trúc để bắt luồng dữ liệu sự kiện từ module giả lập `order_simulator.py` của Tú.
* Liên tục đẩy các bản tin JSON đơn hàng mới phát sinh vào Kafka Topic. Bản tin phải bảo toàn các phân phối thuộc tính lịch sử để mô hình không bị sai lệch đặc trưng:
`{"order_id": "string", "createdAt": "ISO-8601", "weight": float, "sender_lat": float, "sender_lng": float, "receiver_lat": float, "receiver_lng": float, "serviceType": "string"}`.



##### **Bước 3: Tối ưu hóa hạ tầng cấu hình Kafka trên Docker (`infrastructure/`)**

Hùng chịu trách nhiệm cập nhật `docker-compose.yml` để phân vùng mạng hệ thống liên thông mượt mà giữa các Container:

* Cấu hình chính xác hai cổng giao tiếp mạng (`KAFKA_ADVERTISED_LISTENERS`): Cổng nội bộ `kafka:9092` dành cho công cụ Spark Structured Streaming tiêu thụ dòng trên RAM, và cổng ngoại vi `localhost:29092` mở thông suốt cho script `kafka_producers.py` chạy từ máy vật lý cục bộ.
* Thiết lập biến môi trường `KAFKA_AUTO_CREATE_TOPICS_ENABLE: "true"` để tự động khởi tạo và định tuyến chính xác cho 3 topic hệ thống: `weather_realtime`, `order_stream`, và topic đầu ra `uds-predicted-eta`.

---

#### **3. Minh chứng thực thi bắt buộc bàn giao (Phục vụ viết báo cáo và Slide thuyết trình)**

Để chứng minh tính xác thực và khả năng vận hành thực tế (Veracity & Velocity) của hệ thống trước Hội đồng phản biện, Hùng cần cung cấp đầy đủ các ảnh chụp màn hình kỹ thuật sau vào ngày **13/06**:

1. **Màn hình Terminal log vận hành của Producer:** Chụp rõ câu lệnh thực thi `python kafka_producers.py` và các dòng log trả về trạng thái `[SUCCESS]` kèm chuỗi JSON mẫu đang được nạp liên tục vào Kafka Broker thành công.
2. **Log kiểm tra hệ thống Docker Container:** Chụp lại kết quả lệnh `docker logs <kafka_container_id>` chứng minh cụm Kafka đang nhận diện đúng hai cổng listeners nội-ngoại vi và không gặp lỗi phân vùng sự kiện (`Topic Partitioning`).