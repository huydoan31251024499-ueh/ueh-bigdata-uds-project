Trong giai đoạn **ANALYZE** của dự án UDS (Xe Dù), chúng ta sẽ sử dụng các công cụ phân tán của hệ sinh thái Apache Spark để khai thác tập dữ liệu đã được làm sạch và tích hợp từ bước PROCESS (như tệp `income_route_features`) nhằm giải quyết các bài toán vận hành logistics.

### 1. Phân tích quy luật trễ đơn (Sử dụng Spark SQL)
Công cụ này được dùng để tìm kiếm các **Patterns** và xác định các "điểm đen" vận hành.
*   **Truy vấn Hotspots:** Sử dụng Spark SQL để thực hiện các lệnh `GROUP BY` theo `time_slot` (khung giờ) và `condition_label` (trạng thái thời tiết) nhằm xác định các khoảng thời gian có tỷ lệ trễ đơn (`is_late`) cao nhất.
*   **Phân tích tương quan:** Thực hiện các phép tính thống kê mô tả để thấy mối liên hệ giữa lượng mưa (`prcp_mm`) và độ lệch thời gian giao hàng thực tế so với cam kết.
*   **Hiệu quả kinh tế:** Truy vấn sự biến động của `income_per_km` theo các mức độ mưa (`rain_level`) để đánh giá tác động của thời tiết đến thu nhập tài xế.

### 2. Dự báo thời gian giao hàng - Dynamic ETA (Sử dụng Spark MLlib)
Để giảm tỷ lệ hủy đơn, chúng ta chuyển đổi bài toán thành mô hình **Hồi quy (Regression)** để cập nhật ETA linh hoạt.
*   **Xây dựng đặc trưng (Featurization):** Sử dụng các công cụ như `VectorAssembler` để gom nhóm các cột đầu vào gồm `shippingDistance_km`, `weight_kg`, `prcp_mm`, và `flood_severity_score` thành một vector đặc trưng.
*   **Huấn luyện mô hình:** Áp dụng thuật toán **Linear Regression** hoặc **Random Forest Regression** để dự báo `actual_duration_min` (thời gian giao thực tế) dựa trên các biến số ngoại cảnh cực đoan.
*   **Đánh giá:** Sử dụng `RegressionEvaluator` với các chỉ số như RMSE để đo lường độ chính xác của dự báo trước khi triển khai tính năng tự động điều chỉnh ETA trên ứng dụng.

### 3. Tối ưu hóa lộ trình tránh điểm ngập (Sử dụng Spark GraphX)
Để thực hiện mục tiêu giảm quãng đường vận chuyển thực tế, dự án sử dụng thư viện xử lý đồ thị phân tán.
*   **Mô hình hóa đồ thị:** Xây dựng mạng lưới giao thông với các **đỉnh (vertices)** là các giao lộ và **cạnh (edges)** là các đoạn đường di chuyển.
*   **Gán trọng số động:** Tích hợp dữ liệu từ `flood_clean` để gán thêm "chi phí" (weight) cao cho các cạnh đi qua vùng có `flood_severity_score` lớn, buộc thuật toán tìm đường tránh các khu vực này.
*   **Smart Routing:** Chạy các thuật toán tìm đường ngắn nhất trên đồ thị để đề xuất lộ trình tối ưu cho shipper trong điều kiện mưa ngập.

### 4. Dự báo xác suất hủy đơn (Sử dụng Spark ML - Classification)
Chúng ta có thể xây dựng mô hình **Phân loại (Classification)** để dự đoán rủi ro đơn hàng bị hủy ngay khi khách đặt đơn.
*   **Thuật toán:** Sử dụng **Logistic Regression** để dự báo khả năng một đơn hàng có trạng thái thành công hay hủy dựa trên các đặc trưng như chỉ số tắc nghẽn (`traffic_congestion_index`) và mức độ mưa tại thời điểm đó.
*   **Hành động:** Kết quả dự báo giúp nhà quản lý đưa ra quyết định về các gói **"Rainy-day Incentives"** (thưởng mưa) kịp thời cho tài xế để duy trì tỷ lệ hoàn thành đơn.

**Tóm lại:** Việc kết hợp khả năng tính toán **In-memory** của Spark SQL để tìm quy luật, Spark MLlib để dự báo độ trễ và GraphX để tối ưu lộ trình sẽ tạo nên một hệ thống **Logistics thích ứng thời tiết** hoàn chỉnh cho UDS.