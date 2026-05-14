# GIAI ĐOẠN PROCESS – XỬ LÝ DỮ LIỆU

## I. Mục tiêu giai đoạn PROCESS

Giai đoạn **PROCESS** có nhiệm vụ làm sạch, chuẩn hóa và tích hợp các nguồn dữ liệu thô đã thu thập ở giai đoạn PREPARE nhằm tạo ra tập dữ liệu cuối cùng (`final_features`) sẵn sàng cho giai đoạn **ANALYZE**.

Quy trình xử lý được thiết kế theo kiến trúc **RAW → PROCESSED → FINAL**, triển khai bằng **Apache Spark (PySpark)** trên HDFS.

***

## II. Xử lý dữ liệu Thời tiết

**File:** `01_process_weather_spark.py`

### Dữ liệu đầu vào

*   `hcmc_weather_raw.csv`
*   Snapshot thời tiết theo giờ tại TP.HCM

### Các bước xử lý chính

1.  Đọc dữ liệu từ HDFS với **schema-on-read** (`StructType`).
2.  Chuyển `timestamp` từ **UTC → UTC+7 (Asia/Ho\_Chi\_Minh)**.
3.  Ánh xạ mã thời tiết (`coco`) sang nhãn chữ (`condition_label`) bằng `create_map`.
4.  Điền giá trị thiếu:
    *   `prcp_mm = 0`, `cldc = 0`, `wdir = 0`, `wspd = 0`.
5.  Chuẩn hóa kiểu dữ liệu số (`DoubleType`).
6.  Đổi tên cột kèm đơn vị đo:
    *   `temp → temp_c`, `prcp → prcp_mm`, …
7.  Tạo khóa thời gian dùng để join:
    *   `hour_timestamp = date_trunc("hour", timestamp)`.

### Output

```text
/uds-project/data/processed/weather
```

***

## III. Xử lý dữ liệu Ngập lụt

**File:** `02_process_flood.py`

### Dữ liệu đầu vào

*   `hcmc_flood_points_raw.csv`
*   Các sự kiện ngập tại TP.HCM (2023–2024)

### Các bước xử lý chính

1.  Đọc CSV từ HDFS với schema định nghĩa sẵn.
2.  Parse `timestamp` và chuyển **UTC → UTC+7**.
3.  Lọc tọa độ GPS hợp lệ trong phạm vi TP.HCM.
4.  Lọc nguồn tin cậy:
    *   Giữ `verified = true`
    *   Hoặc nguồn `IoT_sensor`, `monitoring_station`.
5.  Chuẩn hóa kiểu dữ liệu cho các cột số.
6.  Tính **điểm mức độ ngập**:
    *   `severity_score = depth_cm × duration_min / 100`.
7.  Tạo `hour_timestamp` để đồng bộ theo giờ.
8.  **Tổng hợp theo giờ** (`groupBy hour_timestamp`):
    *   `flood_avg_depth_cm`
    *   `flood_avg_severity`
    *   `flood_avg_duration`

### Output

```text
/uds-project/data/processed/flood_hourly
```

***

## IV. Xử lý dữ liệu Thị trường & Giao thông

**File:** `03_process_market.py`

### Dữ liệu đầu vào

*   `hcmc_market_traffic_raw.csv`
*   Snapshot thị trường – giao thông theo giờ

### Các bước xử lý chính

1.  Đọc CSV từ HDFS với schema-on-read.
2.  Parse `timestamp` và chuyển **UTC → UTC+7**.
3.  Chuẩn hóa kiểu dữ liệu số.
4.  Phân loại mức độ ùn tắc (`congestion_level`) từ `traffic_congestion_index`.
5.  Tính hệ số điều chỉnh phí theo ùn tắc:
    *   `fee_multiplier`.
6.  Tính phí giao hàng điều chỉnh:
    *   `adjusted_fee = delivery_fee_avg_vnd × fee_multiplier`.
7.  Tạo `hour_timestamp` làm khóa join.

### Output

```text
/uds-project/data/processed/market
```

***

## V. Tích hợp & Feature Engineering

**File:** `process_full_pipeline.py`

### Bước 1: Load & Join dữ liệu

*   Đọc các bảng Parquet từ HDFS:
    *   `orders`
    *   `weather`
    *   `flood_hourly`
    *   `market`
*   Thực hiện **LEFT JOIN** theo `hour_timestamp`:
        orders
          ⟵ weather
          ⟵ flood
          ⟵ market
*   Giữ nguyên toàn bộ đơn hàng (LEFT JOIN).
*   Điền `NULL` cho dữ liệu flood ở các giờ không có ngập.

***

### Bước 2: Feature Engineering – Độ trễ & Thời gian

*   `actual_duration_min`
*   `expected_duration_min`
*   `delay_min = actual − expected`
*   `is_late` (1 nếu trễ)

***

### Bước 3: Feature Engineering – Thời gian & Thời tiết

*   `order_hour`, `order_dow`, `order_month`
*   Phân loại mưa:
    *   `rain_level = no_rain | light | moderate | heavy`
*   `is_extreme_weather` (mưa lớn hoặc thời tiết nguy hiểm)
*   `is_flooded` (có ngập trong giờ)
*   `is_high_congestion`

***

### Bước 4: Lưu dữ liệu cuối

*   Ghi dữ liệu ra HDFS dưới dạng Parquet

### Output

```text
/uds-project/data/processed/final_features
```

***

## VI. Kết quả giai đoạn PROCESS

*   Tất cả dữ liệu được:
    *   Chuẩn hóa timezone
    *   Đồng bộ theo `hour_timestamp`
    *   Làm sạch và tích hợp đa nguồn
*   Dataset cuối cùng:
    *   Đủ thông tin cho các phân tích **ETA, thu nhập tài xế, ảnh hưởng mưa – ngập – giao thông**
    *   Sẵn sàng cho giai đoạn **ANALYZE** (Spark SQL, Spark MLlib)