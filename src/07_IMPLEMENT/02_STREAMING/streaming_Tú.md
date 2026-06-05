### **CHƯƠNG 2 & 4: ĐẶC TẢ KỸ THUẬT PHÂN VÙNG XỬ LÝ VÀ SUY LUẬN DÒNG**

**Thành viên thực hiện:** Đinh Nguyễn Tuấn Tú

---

#### **1. Kiến trúc phân vùng lưu trữ trên GitHub (`src/07_IMPLEMENT/`)**

Toàn bộ mã nguồn và cấu trúc thuật toán do Tú xây dựng sẽ được quản lý tập trung tại phân vùng `02_STREAMING` và module độc lập phục vụ kiểm thử:

```
└── src
    └── 07_IMPLEMENT
        └── 02_STREAMING
            ├── order_simulator.py      # Bộ giả lập đơn hàng đa bối cảnh năm 2026
            ├── spark_streaming.py      # Lõi xử lý dòng và điều hướng Model Gating trên RAM
            └── schemas.py              # Định nghĩa cấu trúc nghiêm ngặt Schema-on-read

```

---

#### **2. Chi tiết công việc kỹ thuật cụ thể (Nhiệm vụ của Tú)**

##### **Bước 1: Phát triển bộ giả lập đơn hàng đa bối cảnh (`order_simulator.py`)**

Để phục vụ việc kiểm thử khả năng chịu tải và tính toán của hệ thống xử lý dòng năm 2026, Tú lập trình script Python sinh dữ liệu đơn hàng tự động theo cơ chế **Scenario-based Generation**:

* **Xây dựng kịch bản cực đoan:** Thiết lập các cấu hình sinh dữ liệu ngẫu nhiên nhưng có chủ đích (ví dụ: bối cảnh `rain_flood` sẽ tự động tăng mật độ đơn hàng có tọa độ GPS trùng khớp hoặc nằm trong bán kính ảnh hưởng của 159 điểm đen ngập lụt do Hùng cung cấp; đồng thời tự động hạ tốc độ nền `avg_vehicle_speed_kmh` xuống mức tối thiểu).
* **Định dạng đầu ra:** Đảm bảo cấu trúc bản tin JSON xuất ra hoàn toàn đồng nhất với phân phối thuộc tính của tập dữ liệu lịch sử để tránh làm lệch vector đặc trưng đầu vào của mô hình.
* **Bàn giao kết nối:** Đóng gói module này và phối hợp cấu hình để luồng thứ hai của Kafka Producer (`kafka_producers.py` của Hùng) có thể gọi liên tục nhằm bắn sự kiện vào topic `order_stream`.

##### **Bước 2: Xây dựng cấu trúc Schema-on-read (`schemas.py`)**

Để Spark Structured Streaming có thể đọc hiểu dữ liệu không cấu trúc từ Kafka, Tú viết tệp `schemas.py` sử dụng thư viện `pyspark.sql.types`:

* Định nghĩa chính xác cấu trúc kiểu dữ liệu (`StructType`, `StructField`) cho cả 2 luồng: luồng đơn hàng (`order_stream`) và luồng thời tiết (`weather_realtime`). Bước này đóng vai trò ép kiểu nghiêm ngặt từ dạng chuỗi (`string/value`) nhận từ Kafka sang DataFrame có cấu trúc trường rõ ràng ngay khi chạm vào RAM.

##### **Bước 3: Lập trình lõi xử lý dòng và suy luận động (`spark_streaming.py`)**

Sử dụng **Spark Structured Streaming** để xử lý bảng không giới hạn (*Unbounded Table*) trên RAM:

* **Khớp nối không-thời gian (Temporal & Spatial Joining):**
* *Khóa thời gian (Temporal):* Sử dụng hàm `date_trunc("hour", col("timestamp"))` để tạo khóa đồng bộ `hour_timestamp`, tiến hành Join luồng thời tiết thực từ Kafka với luồng đơn hàng.
* *Khóa không gian (Spatial):* Đọc tập dữ liệu tĩnh 159 điểm ngập từ HDFS lên bộ nhớ đệm, dùng tọa độ GPS (`lat`, `lng`) của đơn hàng động từ Kafka đối chiếu tức thời nhằm tự động tính toán ra các đặc trưng: `has_flood`, `flood_avg_severity`.


* **Tính toán đặc trưng trực tiếp (On-the-fly Feature Engineering):** * Tính toán các biến tương tác và biến phi tuyến tính ngay trên dòng chảy dữ liệu trước khi đưa vào mô hình: Bình phương chỉ số tắc nghẽn để tạo ra `traffic_penalty`, tính chỉ số rủi ro tổng hợp $WIS$ và tương tác giờ cao điểm `flood_peak_interaction`.
* **Cơ chế điều hướng mô hình (Model Gating & Inference):**
* Viết đoạn code nạp trực tiếp tập tham số (Model Artifact) của mô hình *Segmented Linear Regression* từ thư mục lưu trữ trên HDFS lên RAM.
* Dựa vào bối cảnh thời gian thực vừa phân loại (`normal`, `rain_only`, `flood_only`, `rain_flood`), điều hướng luồng dữ liệu vào đúng nhánh trọng số mô hình đã tối ưu để chạy hàm `.transform()`, tính toán ra giá trị đầu ra (thời gian di chuyển thực tế - `actual_duration_min`) với độ trễ tính bằng mili-giây.



##### **Bước 4: Phối hợp đẩy dữ liệu đầu ra và kết nối Dashboard**

* Phối hợp với Hùng sử dụng cấu hình `.writeStream.format("kafka")` để đẩy toàn bộ DataFrame kết quả ETA đã co giãn động ngược trở lại Kafka Broker vào topic sạch `uds-predicted-eta`.
* Thống nhất cấu trúc payload đầu ra (`order_id`, `original_eta`, `weather_adaptive_eta`, `current_context`) và bàn giao cho Tiên để Tiên cấu hình Web Server kết nối hiển thị trực quan lên Dashboard.

---

#### **3. Minh chứng thực thi bắt buộc bàn giao (Phục vụ viết báo cáo và Slide thuyết trình)**

1. **Màn hình kiểm soát Spark Web UI (Cổng 4040):** Chụp lại biểu đồ trực quan **DAG Visualization** hiển thị cấu trúc xử lý song song của tiến trình Streaming. Chụp lại màn hình quản lý **Micro-batches** chứng minh tốc độ xử lý dòng (Processing Rate) đạt tiêu chuẩn thời gian thực và không xảy ra hiện tượng thắt nút cổ chai (Bottleneck).
2. **Log suy luận hệ thống (Streaming Inference Logs):** Chụp lại màn hình Console/Terminal của Spark hiển thị rõ DataFrame kết quả sau lệnh `.transform()`, chứng minh giá trị ETA gốc đã được chỉnh sửa một cách logic khi bối cảnh thời tiết chuyển sang cực đoan (`rain_flood`).

---

### Sản phẩm bàn giao

1. **Mã nguồn:** Các tệp tin `order_simulator.py`, `schemas.py`, và `spark_streaming.py` vận hành mượt mà, không xung đột kiểu dữ liệu. **Hạn chót: 09/06**.
2. **Tài liệu viết tiểu luận:** Bản thảo **Chương 4 (Đặc tả thuật toán phân đoạn và cơ chế Model Gating trên RAM)**. Nội dung viết theo văn phong học thuật, giải thích rõ cơ chế chuyển dịch từ mô hình Offline sang Online Inference. **Hạn chót phần viết: 11/06**.