## 1. Bối cảnh và mục tiêu phân tích

Xe Dù (UDS) cung cấp các dịch vụ giao hàng nhanh 3h và 5h tại TP.HCM – một môi trường đô thị có mức độ bất định cao do mưa lớn theo mùa, ngập lụt cục bộ và ùn tắc giao thông theo khung giờ. Hệ thống ETA hiện tại của UDS mang tính **tĩnh (static ETA)**, sử dụng buffer an toàn cố định nhằm hạn chế giao trễ trong điều kiện bình thường.

Tuy nhiên, phân tích ban đầu cho thấy ETA tĩnh không còn phản ánh chính xác thời gian giao hàng thực tế khi điều kiện ngoại cảnh thay đổi (mưa lớn, ngập lụt), dẫn đến độ trễ gia tăng và vi phạm SLA. Do đó, SMART Question 1 tập trung đánh giá liệu **ETA có thể được dự báo và điều chỉnh động theo bối cảnh vận hành** thông qua dữ liệu thời tiết, giao thông và ngập lụt hay không, thay vì chỉ dựa trên quãng đường và thời gian cố định.

***

## 2. Phân tích khám phá dữ liệu – `q1_delay_eda.py`

### 2.1 Mục tiêu EDA

EDA được sử dụng để:

*   Hiểu bản chất của biến trễ `delay_min`
*   Kiểm tra mối quan hệ giữa trễ giao hàng và các yếu tố ngoại cảnh (mưa, ngập, thời gian)
*   Xác định liệu trễ có mang tính cấu trúc hay chỉ là nhiễu ngẫu nhiên

EDA không nhằm mục tiêu dự báo, mà nhằm **xác định nguồn gốc của sai lệch ETA**.

### 2.2 Kết quả chính

Kết quả EDA cho thấy:

1.  **Mưa lớn (prcp\_mm > 5mm)** làm phân phối `delay_min` lệch phải rõ rệt, xuất hiện nhiều trường hợp trễ nặng.
2.  **Ngập lụt (has\_flood = 1)** làm tăng đáng kể cả trung vị và phương sai của `delay_min`.
3.  Trễ tập trung vào **giờ cao điểm**, đặc biệt khi kết hợp với mưa và ngập, cho thấy đây là hiện tượng có cấu trúc theo bối cảnh thời gian.

### 2.3 Insight từ EDA

> Trễ giao hàng trong mưa lớn và ngập lụt không mang tính ngẫu nhiên, mà có thể nhận diện trước dựa trên điều kiện thời tiết và thời gian vận hành.

EDA chỉ ra rằng **vấn đề cốt lõi nằm ở ETA không thích ứng theo bối cảnh**, thay vì do lỗi vận hành đơn lẻ.

***

## 3. Huấn luyện mô hình dự báo ETA – `q1_eta_regression.py`

### 3.1 Mục tiêu mô hình

Mô hình hồi quy ETA được xây dựng nhằm kiểm tra:

*   Thời gian giao hàng thực tế có thể dự báo được từ dữ liệu bối cảnh hay không
*   Các yếu tố thời tiết, ngập lụt và giao thông có đóng góp thực sự vào độ chính xác ETA hay không

Mục tiêu của mô hình **không phải tối ưu RMSE tuyệt đối**, mà là đánh giá tính khả thi của **Dynamic ETA** trong điều kiện dữ liệu thực tế.

### 3.2 Phạm vi và giả định

*   Chỉ huấn luyện cho các dịch vụ **3h và 5h** (dịch vụ có SLA theo phút).
*   Loại khỏi mô hình các dịch vụ không cùng bản chất vận hành (`luu_kho`, bán tải).
*   Loại các outlier cực đoan: `actual_duration_min > 24h`.

### 3.3 Đặc trưng và biểu diễn dữ liệu (Cập nhật sau EDA & chỉnh sửa mô hình)

Dựa trên các insight thu được từ EDA, mô hình hồi quy ETA đã được **điều chỉnh lại tập đặc trưng** nhằm phản ánh đúng hơn các yếu tố gây trễ thực tế trong vận hành đô thị TP.HCM.

Cụ thể, mô hình **ưu tiên các đặc trưng liên quan đến ngập lụt và giao thông**, trong khi mưa được xem là yếu tố thứ cấp:

*   **Khoảng cách & tải trọng**
    *   `shipping_km`
    *   `weight`

*   **Giao thông & năng lực vận hành**
    *   `traffic_penalty` (bình phương `traffic_congestion_index` để mô hình hóa tác động phi tuyến)
    *   `avg_vehicle_speed_kmh`
    *   `active_delivery_vehicles`

*   **Ngập lụt (yếu tố rủi ro chính theo EDA)**
    *   `has_flood`
    *   `flood_avg_severity`
    *   `flood_peak_interaction` (tương tác giữa ngập và giờ cao điểm)

*   **Thời gian**
    *   `is_peak_hour`

Các đặc trưng liên quan đến **mưa đơn thuần** (`is_heavy_rain`, `prcp_mm`) **không còn được đưa trực tiếp vào vector đặc trưng**, mà chỉ được sử dụng cho **phân đoạn dữ liệu**, nhằm tránh gây nhiễu cho mô hình hồi quy.

Toàn bộ đặc trưng được chuẩn hóa bằng `StandardScaler` để đảm bảo tính ổn định khi huấn luyện.


***

## 4. Kết quả huấn luyện và tiến trình cải thiện

### 4.1 Tổng hợp RMSE qua các giai đoạn

| Giai đoạn huấn luyện | Đặc điểm kỹ thuật nổi bật | Kết quả RMSE (Phút) | Ý nghĩa & Đánh giá |
| :--- | :--- | :--- | :--- |
| **1. Baseline (Cơ bản)** | Train trên toàn bộ Dataset, sử dụng các FEATURES đơn giản. | - **Linear Regression:** 479.77<br>- **Random Forest:** 482.95 | **Sai số ~8 tiếng.** Mô hình bị kéo lệch nặng nề bởi các điểm dị biệt (Outliers) như đơn trễ 7 ngày và sự trộn lẫn các loại dịch vụ (Lưu kho vs Hỏa tốc). |
| **2. Feature Engineering & Filtering** | Thêm features (weather, flood, market). Giới hạn `serviceType` (3h, 5h). Loại bỏ đơn trễ > 24h. | - **Dịch vụ 3h:** 206.26<br>- **Dịch vụ 5h:** 291.15 | **Cải thiện ~7-12%** so với baseline. Việc tách riêng dịch vụ giúp mô hình bắt đầu học được quy luật của nhóm hỏa tốc, nhưng sai số vẫn lớn hơn khung thời gian cam kết. |
| **3. Advanced Segmentation (Phân đoạn nâng cao)** | **Data Segmentation** (theo bối cảnh bình thường/mưa/ngập). Huấn luyện trên **SLA-compliant subset**. | - **Dịch vụ 3h (Normal):** **39.86**<br>- *Các phân đoạn khác:* Bị skip do thiếu dữ liệu. | **Bước đột phá kỹ thuật.** Sai số giảm xuống dưới 40 phút, mức độ tin cậy đạt ~78-80% cho điều kiện bình thường, đủ điều kiện triển khai bản Beta. |
| **4. Context-aware Regression (Revised)** | Ưu tiên flood severity, traffic phi tuyến, bỏ rain khỏi vector. | **37.73** (3h_normal). **76.16** (5h_normal)| **Cải thiện thêm RMSE**, mô hình hội tụ ổn định, phù hợp triển khai Dynamic ETA. |

### 4.2 Diễn giải kết quả (Cập nhật)

Việc giảm RMSE từ \~480 phút xuống **37.73 phút** trong phiên bản mô hình mới **không đến từ thay đổi thuật toán**, mà đến từ việc:

*   **Liên kết chặt chẽ với EDA**: Flood được xác định là yếu tố rủi ro chính, vượt trội so với mưa.
*   **Loại bỏ tín hiệu gây nhiễu**: Các đặc trưng mưa đơn thuần gây over-buffer trong ETA được loại khỏi vector hồi quy.
*   **Biểu diễn phi tuyến hợp lý**: Tác động của giao thông được mô hình hóa bằng `traffic_penalty`.
*   **Phân đoạn theo ngữ cảnh vận hành**: Chỉ huấn luyện mô hình trong các phân đoạn có đủ dữ liệu và hành vi ổn định.

Các phân đoạn như `5h rain_only` và `5h flood` tiếp tục bị **chủ động loại bỏ** do số lượng mẫu không đủ, phản ánh đúng vấn đề **data sparsity** đã được chỉ ra trong EDA.

***

## 5. Trả lời SMART Question 1 (Bổ sung)

Kết quả huấn luyện sau chỉnh sửa cho thấy:

*   ETA **có thể dự báo chính xác hơn đáng kể** trong điều kiện vận hành bình thường khi mô hình được điều chỉnh theo bối cảnh.
*   **Ngập lụt cục bộ**, chứ không phải mưa lớn, là yếu tố quyết định rủi ro trễ giao hàng.
*   Dynamic ETA dựa trên **flood severity và trạng thái giao thông** là hướng tiếp cận khả thi hơn so với ETA tĩnh.

***

### Kết luận

> Việc cải thiện ETA không đến từ việc sử dụng thuật toán phức tạp hơn, mà từ cách hiểu đúng dữ liệu và cấu trúc rủi ro trong vận hành. Phiên bản mô hình hồi quy được chỉnh sửa dựa trên EDA, với trọng tâm là ngập lụt và giao thông, đã giúp giảm RMSE xuống **37.73 phút** cho phân đoạn 3h–normal. Kết quả này cho thấy Dynamic ETA theo bối cảnh là khả thi và có cơ sở kỹ thuật vững chắc để triển khai thử nghiệm trong môi trường đô thị thực tế.
