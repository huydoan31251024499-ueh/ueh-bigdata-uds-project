# Giai đoạn ANALYZE (Phân tích dữ liệu)

### 1. Phân tích Dự báo Độ trễ và Tối ưu ETA (SMART Question 1)
*   **Bài toán:** **Dự báo (Prediction)** và **Hồi quy (Regression)**.
*   **Định hướng Analyze:** Sử dụng **Spark MLlib** để xây dựng mô hình dự báo thời gian giao hàng thực tế (`actual_duration_min`).
    *   **Biến đầu vào (Features):** `shippingDistance_km`, `weight_kg`, `prcp_mm` (lượng mưa), `traffic_congestion_index` và `flood_severity_score`.
    *   **Thuật toán đề xuất:** *Linear Regression* hoặc *Random Forest Regressor*.

### 2. Phân tích Tương quan và Chính sách Giá linh động (SMART Question 2)
*   **Bài toán:** **Nhìn nhận mối liên hệ (Seeing Connections)** và **Tìm kiếm quy luật (Patterns)**.
*   **Định hướng Analyze:** Sử dụng **Spark SQL** để phân tích sự biến động của thu nhập tài xế (`income_per_km`, `income_per_min`) dưới tác động của các hệ số nhân phí (`fee_multiplier`).
    *   **Phân tích Patterns:** Truy vấn các khung giờ "điểm đen" nơi `route_difficulty_score` cao nhưng thu nhập tài xế thấp.
    *   **Hành động:** Xác định ngưỡng tối ưu cho các gói **Rainy-day Incentives** (Ví dụ: Khi `prcp_mm` > 5mm, tăng `weather_fee_multiplier` lên 1.5 để đảm bảo `income_per_min` của tài xế không giảm quá 10% so với trời đẹp).

### 3. Tối ưu hóa Lộ trình tránh điểm ngập (SMART Question 3)
*   **Bài toán:** **Phân tích đồ thị (Graph Analysis)** và **Tối ưu hóa (Optimization)**.
*   **Định hướng Analyze:** Sử dụng **Spark GraphX** (Vượt yêu cầu học phần) để tìm đường đi ngắn nhất "thích ứng thời tiết".
    *   **Xây dựng đồ thị:** Các đỉnh là tọa độ GPS, các cạnh là đoạn đường với trọng số là `effective_distance_km`.
    *   **Gán trọng số động:** Tích hợp `is_flooded` và `flood_avg_severity` vào trọng số của các cạnh. Những đoạn đường đang ngập sẽ có "chi phí" di chuyển cực cao, buộc thuật toán tìm lộ trình thay thế.
*   **Kết quả:** Đo lường sự sụt giảm quãng đường thực tế so với lộ trình cũ khi chưa tích hợp dữ liệu ngập lụt.

### 4. Đề xuất thu thập thêm dữ liệu và Công nghệ phân tán nâng cao
*   **Thu thập thêm dữ liệu:**
    *   **Dữ liệu Giao thông thời gian thực:** Crawl dữ liệu tốc độ xe từ Google Maps API hoặc Cổng GTVT TP.HCM để cập nhật `traffic_congestion_index` liên tục.
    *   **Dữ liệu Đơn hàng bị hủy:** Để trả lời chính xác câu hỏi về tỷ lệ hủy đơn.
*   **Công nghệ phân tán nâng cao:**
    *   **Real-time Streaming (Kafka + Spark Streaming):** Thay vì nạp Batch định kỳ, hãy sử dụng **Kafka** để hứng luồng dữ liệu thời tiết thực từ API và **Spark Streaming** để cập nhật `Dynamic ETA` ngay lập tức khi trời bắt đầu mưa.
    *   **NoSQL (HBase):** Lưu trữ kết quả phân tích đặc trưng lộ trình vào **HBase** để ứng dụng của tài xế có thể truy xuất ngẫu nhiên với độ trễ thấp (miliseconds) khi đang di chuyển trên đường.

**Tóm lại:** Định hướng Analyze tập trung vào việc dùng **Spark MLlib** để dự báo trễ đơn, **Spark SQL** để tối ưu chính sách thưởng mưa, và **GraphX** để dẫn đường tránh ngập, kết hợp phân tích dữ liệu lịch sử sang xử lý dòng (Streaming) qua **Kafka**.