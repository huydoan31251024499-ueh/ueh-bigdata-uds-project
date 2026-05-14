# GIAI ĐOẠN SHARE – TRỰC QUAN HOÁ DỮ LIỆU

Giai đoạn **SHARE** là bước chuyển hóa các kết quả từ công cụ tính toán phân tán (Spark) thành các câu chuyện bằng hình ảnh (Data Storytelling) để Xe Dù (UDS) ra quyết định chiến lược. Dựa trên cấu trúc thư mục kết quả phân tích và các chỉ số khoa học đã đạt được, dưới đây là định hướng trực quan hóa chi tiết:

### 1. Trực quan hóa SMART Q1: Xác định và Dự báo nguy cơ trễ đơn
Dựa trên các tệp trong `data/analysis/q1/`, mục tiêu của trực quan hóa là **chứng minh ngập lụt (flood) mới là yếu tố quyết định rủi ro trễ đơn, không phải mưa đơn thuần**.

***

#### 1.1. Biểu đồ phân đoạn rủi ro trễ đơn theo bối cảnh (Compound Risk)

**Nguồn dữ liệu:** `compound_risk.csv`

**Loại biểu đồ:** Bar chart (cột đơn, không stacked)

**Cấu hình đề xuất:**

*   Dimension: `risk_segment`
*   Metric: `late_probability_pct`
*   Metric phụ (tooltip): `avg_traffic_index`, `order_volume`

**Insight làm rõ:**

*   Nhóm `STRESS_Flood_Only` có **xác suất trễ đơn cao nhất (\~24.23%)**
*   Cao hơn đáng kể so với `HIGH_Rain_Only` (\~4.88%)
*   Chứng minh rằng **ngập lụt độc lập với mưa vẫn tạo rủi ro trễ cao**

***

#### 1.2. Biểu đồ ảnh hưởng của mưa đến tỷ lệ trễ theo loại dịch vụ (Rain Impact)

**Nguồn dữ liệu:** `rain_impact.csv`

**Loại biểu đồ:** Grouped bar chart

**Cấu hình đề xuất:**

*   Dimension: `rain_level`
*   Breakdown dimension: `serviceType` (3h, 5h)
*   Metric: `late_rate_percentage`

**Insight làm rõ:**

*   Mưa lớn **không làm tỷ lệ trễ tăng đột biến**
*   Dịch vụ 3h nhạy cảm hơn 5h, nhưng **rain\_level không giải thích được các trường hợp trễ nghiêm trọng**
*   Loại bỏ giả định “mưa lớn ⇒ trễ đơn”

***

#### 1.3. Biểu đồ điểm nóng trễ đơn gắn với mức độ ngập (Flood Hotspots)

**Nguồn dữ liệu:** `hotspots.csv`

**Loại biểu đồ:** Bubble chart (hoặc Geo Map nếu mở rộng lat/lng)

**Cấu hình đề xuất:**

*   Dimension: `rain_level`
*   Metric: `late_order_count`
*   Bubble size / color: `avg_flood_severity_at_delivery`
*   Filter: `serviceType = '3h'`

**Insight làm rõ:**

*   Nhiều đơn trễ xảy ra trong điều kiện **không mưa hoặc mưa nhẹ nhưng mức độ ngập cao**
*   Xác nhận sự **tách biệt giữa rain và flood trong cơ chế gây trễ**

***

### 2. Trực quan hóa SMART Q2: Phân tích tác động kinh tế lên thu nhập tài xế

Dựa trên các tệp trong `data/analysis/q2/`, mục tiêu trực quan hóa là **chứng minh “hình phạt kinh tế” mà tài xế phải chịu trong điều kiện ngập lụt và vai trò của thời gian giao hàng đối với thu nhập hiệu quả**.

***

#### 2.1. Biểu đồ thu nhập hiệu quả theo bối cảnh vận hành (Economic Penalty)

**Nguồn dữ liệu:** `penalty.csv` 

**Loại biểu đồ:** Bar chart (cột đơn)

**Cấu hình đề xuất:**

*   Dimension: `context_segment`
*   Metric: `income_per_min_raw`
*   Metric phụ (tooltip): `avg_time_mins`, `order_volume`, `avg_traffic`

**Insight làm rõ:**

*   `flood_only` có **thu nhập thấp nhất (\~187 VND/phút)**
*   Giảm khoảng **30% so với điều kiện normal (266 VND/phút)**
*   `rain_only` giảm nhẹ hơn (\~229 VND/phút)

#### 2.2. Biểu đồ so sánh thời gian giao hàng và thu nhập hiệu quả

**Nguồn dữ liệu:** `penalty.csv`

**Loại biểu đồ:** Scatter plot

**Cấu hình đề xuất:**

*   X-axis: `avg_time_mins`
*   Y-axis: `income_per_min_raw`
*   Color / Dimension: `context_segment`
*   Size (optional): `order_volume`

**Insight làm rõ:**

*   Các bối cảnh có **thời gian giao hàng cao hơn** đi kèm với **thu nhập/phút thấp hơn**
*   Xác nhận mối quan hệ kinh tế:

        Time ↑  →  Income efficiency ↓

#### 2.3. Biểu đồ mô phỏng thu nhập trước và sau can thiệp (Simulation Overview)

**Nguồn dữ liệu:** `simulation.csv`

**Loại biểu đồ:** Grouped bar chart (Before vs After)

**Cấu hình đề xuất:**

*   Dimension: `context_segment`
*   Metric 1: `old_income_min`
*   Metric 2: `new_income_min`

**Insight làm rõ:**

*   Trong dữ liệu mô phỏng:
    *   `rain_only` và `rain_flood` cho thấy **khả năng phục hồi thu nhập**
    *   `flood_only` **không được cải thiện**, cho thấy đây là **blind spot của hệ thống hiện tại**


### 3. Trực quan hóa SMART Q3: Phân tích không gian – hiệu quả lộ trình

Dựa trên các tệp trong `data/analysis/q3/`, mục tiêu của trực quan hóa là **chứng minh rằng ngập lụt làm giảm hiệu quả vận hành thông qua thời gian, chứ không phải do quãng đường tăng**.

***

#### 3.1. Biểu đồ so sánh quãng đường danh nghĩa theo bối cảnh (Distance Stability)

**Nguồn dữ liệu:** `distance.csv` / `summary.csv`

**Loại biểu đồ:** Bar chart (cột đơn)

**Cấu hình đề xuất:**

*   Dimension: `context_segment`
*   Metric: `avg_distance_km`

**Insight làm rõ:**

*   Quãng đường danh nghĩa **gần như không thay đổi đáng kể** giữa các bối cảnh:
    *   `normal` ≈ 10,219 m
    *   `rain_only` ≈ 10,531 m
    *   `flood_only` ≈ 9,889 m

***

#### 3.2. Biểu đồ so sánh thời gian vận chuyển theo bối cảnh (Time Impact)

**Nguồn dữ liệu:** `duration.csv` / `summary.csv`

**Loại biểu đồ:** Bar chart (cột đơn)

**Cấu hình đề xuất:**

*   Dimension: `context_segment`
*   Metric: `avg_duration_min`

**Insight làm rõ:**

*   Thời gian giao hàng **tăng mạnh** trong điều kiện bất lợi:
    *   `normal`: \~320 phút
    *   `rain_only`: \~444 phút
    *   `flood_only`: \~365 phút

***

#### 3.3. Biểu đồ hiệu quả lộ trình theo bối cảnh (Route Efficiency – Quan trọng nhất)

**Nguồn dữ liệu:** `efficiency.csv` / `summary.csv`

**Loại biểu đồ:** Bar chart (cột đơn, highlight `flood_only`)

**Cấu hình đề xuất:**

*   Dimension: `context_segment`
*   Metric: `avg_efficiency` (distance / time)

**Insight làm rõ:**

*   Hiệu quả vận hành giảm mạnh trong `flood_only`:
    *   `normal`: \~88
    *   `rain_only`: \~88
    *   `flood_only`: **\~62 (giảm \~30%)**


***

#### 3.4. Biểu đồ tương quan mức độ ngập và thời gian (Correlation Evidence – phụ trợ)

**Nguồn dữ liệu:** `correlation.csv`

**Loại biểu đồ:** Scorecard hoặc Scatter plot (nếu mở rộng)

**Cấu hình đề xuất:**

*   Metric: `corr_severity_duration`

**Insight làm rõ:**

*   `corr_severity_duration ≈ 0.003` → **gần như không có tương quan tuyến tính**

---

**Kết luận:** Bước SHARE không chỉ là báo cáo số liệu mà là bằng chứng định lượng để chứng minh rằng bộ 3 tính năng **A1 (Dashboard ETA), A2 (Dynamic Pricing) và A3 (Điều hướng tránh ngập)** là giải pháp cho UDS vào mùa mưa 2026.