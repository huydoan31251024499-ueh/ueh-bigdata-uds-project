# GIAI ĐOẠN PREPARE – CHUẨN BỊ DỮ LIỆU

## I. Mục tiêu giai đoạn PREPARE

Mục tiêu của giai đoạn **PREPARE** là thu thập, tổng hợp và mô tả các nguồn dữ liệu cần thiết nhằm xây dựng một tập dữ liệu đầu vào đầy đủ, nhất quán và đáng tin cậy cho dự án **“Hệ thống phân tích logistics dựa trên dữ liệu lớn thời tiết và vận tải tại TP.HCM cho startup UDS”**.

Dữ liệu thu thập được phục vụ cho việc trả lời **03 SMART Questions** đã được xác định ở giai đoạn ASK, tập trung vào:

*   Dự báo độ trễ giao hàng và tối ưu ETA
*   Phân tích tác động của mưa – ngập – giao thông đến thu nhập tài xế
*   Phân tích khả năng giảm quãng đường và thời gian giao hàng thông qua tránh điểm ngập lụt

***

## II. Các nguồn dữ liệu đã thu thập

### 1. Dữ liệu nội bộ

| Thuộc tính       | Mô tả                                         |
| ---------------- | --------------------------------------------- |
| **Tên file**     | `uds_orders.csv`                              |
| **Nguồn**        | Hệ thống vận hành nội bộ của UDS              |
| **Loại dữ liệu** | Structured data (CSV)                         |
| **Vai trò**      | Dữ liệu nền (fact table) của toàn bộ pipeline |

**Các trường dữ liệu chính:**

*   `id`: mã đơn hàng
*   `createdAt`, `deliveredAt`, `expectedDeliveryTime`: mốc thời gian đơn hàng
*   `senderLat`, `senderLng`, `receiverLat`, `receiverLng`: tọa độ giao – nhận
*   `shippingDistance`: quãng đường ước tính (mét)
*   `weight`: trọng lượng đơn
*   `orderStatus`: trạng thái đơn hàng (hiện tại chủ yếu là `success`).

***

### 2. Dữ liệu bên ngoài

#### 2.1. Dữ liệu thời tiết

| Thuộc tính               | Mô tả                                          |
| ------------------------ | ---------------------------------------------- |
| **Nguồn**                | Visual Crossing / OpenWeatherMap               |
| **Phương thức thu thập** | API Request (Python `requests`)                |
| **Tần suất**             | Theo giờ (Hourly)                              |
| **Mục đích**             | Đánh giá tác động của mưa đến độ trễ giao hàng |

**Các trường dữ liệu sử dụng:**

*   `timestamp`
*   `prcp_mm` (lượng mưa, mm)
*   `temp_c`, `rhum_pct`
*   `condition_label`

***

#### 2.2. Dữ liệu ngập lụt

| Thuộc tính               | Mô tả                                                  |
| ------------------------ | ------------------------------------------------------ |
| **Nguồn**                | UDI Maps / Cổng dữ liệu mở TP.HCM                      |
| **Phương thức thu thập** | Crawl dữ liệu & tổng hợp lịch sử                       |
| **Loại dữ liệu**         | Semi-structured                                        |
| **Mục đích**             | Xác định khu vực và mức độ ngập ảnh hưởng đến giao vận |

**Các trường dữ liệu chính:**

*   `hour_timestamp`
*   `flood_avg_depth_cm`
*   `flood_avg_severity`
*   `flood_avg_duration`

Dữ liệu ngập lụt được **chuẩn hóa theo khung giờ** để đồng bộ với dữ liệu đơn hàng.

***

#### 2.3. Dữ liệu giao thông – thị trường

| Thuộc tính               | Mô tả                                                 |
| ------------------------ | ----------------------------------------------------- |
| **Nguồn**                | Cổng GTVT TP.HCM / tổng hợp thị trường                |
| **Phương thức thu thập** | Crawl + chuẩn hóa batch                               |
| **Mục đích**             | Phân tích ùn tắc và mô phỏng chính sách giá linh động |

**Các trường dữ liệu chính:**

*   `traffic_congestion_index`
*   `avg_vehicle_speed_kmh`
*   `fuel_price_vnd_liter`
*   `delivery_fee_avg_vnd`
*   `fee_multiplier`, `adjusted_fee`

***

## III. Chuẩn bị và tổ chức dữ liệu

### Bước 1: Thu thập dữ liệu

*   Sử dụng Python (`requests`, `BeautifulSoup`) để:
    *   Gọi API thời tiết
    *   Thu thập dữ liệu ngập và giao thông theo batch
*   Tất cả dữ liệu được lưu **nguyên bản** vào thư mục:

```text
data/raw/
```

***

### Bước 2: Kiểm tra chất lượng dữ liệu (Data Validation)

*   Kiểm tra:
    *   Trùng lặp đơn hàng
    *   Giá trị null ở các trường quan trọng (timestamp, tọa độ)
*   Chuẩn hóa:
    *   Đơn vị đo (mm, km, phút)
    *   Timezone về `Asia/Ho_Chi_Minh`
*   Đảm bảo tất cả dữ liệu có thể **join theo `hour_timestamp`**

***

### Bước 3: Sẵn sàng cho giai đoạn PROCESS

*   Mỗi nguồn dữ liệu được mô tả rõ:
    *   Nguồn gốc
    *   Ý nghĩa business
    *   Trường dùng cho join / feature engineering

***

## IV. Sản phẩm bàn giao của giai đoạn PREPARE

Sau khi hoàn thành giai đoạn PREPARE, hệ thống có:

1.  **Dữ liệu thô (Raw Data)**
    *   `uds_orders.csv`
    *   `weather_raw.csv`
    *   `flood_raw.csv`
    *   `market_raw.csv`

2.  **Script thu thập dữ liệu**
    *   Các file `.py` gọi API / crawl dữ liệu

3.  **Tài liệu mô tả dữ liệu**
    *   File `prepare.md` (tài liệu này)
    *   Làm cơ sở cho giai đoạn **PROCESS & ANALYZE**

***

## V. Kết luận

Giai đoạn **PREPARE** đã xây dựng được một tập dữ liệu đa nguồn, phù hợp với bối cảnh vận hành logistics tại TP.HCM và đủ năng lực để:

*   Dự báo độ trễ giao hàng trong điều kiện thời tiết xấu
*   Phân tích tác động của mưa, ngập, giao thông đến thu nhập tài xế
*   Đánh giá khả năng tối ưu quãng đường và thời gian giao hàng