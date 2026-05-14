## 1. Mục tiêu EDA

Mục tiêu của phân tích khám phá dữ liệu (EDA) là:

*   Hiểu **bản chất và cấu trúc của độ trễ giao hàng** (`delay_min`)
*   Xác định **yếu tố bối cảnh** (mưa, ngập, giao thông) ảnh hưởng đến rủi ro trễ
*   Cung cấp **cơ sở định lượng** cho thiết kế đặc trưng (feature engineering) và phân đoạn dữ liệu trong mô hình dự báo ETA

EDA không nhằm mục tiêu tối ưu mô hình, mà nhằm trả lời câu hỏi:  
**“Trễ giao hàng xảy ra khi nào và vì sao?”**

***

## 2. Lưu ý về biến `delay_min`

Trong toàn bộ phân tích, giá trị `delay_min` chủ yếu mang **giá trị âm**, phản ánh việc **đơn hàng được giao sớm hơn ETA cam kết**.  
Do đó:

*   Giá trị trung bình của `delay_min` **không phản ánh rủi ro**
*   **Xác suất trễ** (`late_probability_pct`, `late_rate_percentage`) là chỉ số chính để đánh giá nguy cơ vi phạm SLA

***

## 3. Phân tích theo phân đoạn rủi ro (Risk Segment)

Kết quả EDA theo `risk_segment` cho thấy:

*   **Flood-only** có xác suất trễ cao nhất (\~24%), cao hơn cả điều kiện mưa lớn.
*   **Rain-only** có xác suất trễ thấp (\~5%), dù mức độ giao thông trung bình cao.
*   Các phân đoạn kết hợp mưa + ngập có số lượng quan sát rất thấp, **không đủ ý nghĩa thống kê**.

**Insight chính:**

> Rủi ro trễ giao hàng được quyết định chủ yếu bởi **ngập lụt cục bộ**, không phải cường độ mưa.

Kết quả này trực tiếp biện minh cho việc:

*   Ưu tiên các đặc trưng **flood-related** (`has_flood`, `severity_score`)
*   Không sử dụng mưa đơn thuần làm tín hiệu chính cho ETA

***

## 4. Mối quan hệ giữa mưa, ngập và trễ theo dịch vụ

### 4.1. Theo mức mưa (`rain_level`)

Đối với dịch vụ **3h**:

*   **Light rain** có tỷ lệ trễ cao nhất (\~27%)
*   **Heavy rain** có tỷ lệ trễ thấp (\~5%) và `delay_min` rất âm

Điều này cho thấy:

*   Khi mưa lớn, hệ thống ETA hiện tại **over-buffer**, dẫn đến giao sớm
*   Khi mưa nhẹ, rủi ro trễ cao do **không có chiến lược né ngập rõ ràng**

### 4.2. Theo mức độ ngập

Các đơn trễ tuy ít về số lượng nhưng gắn với **mức độ ngập trung bình cao**, đặc biệt trong nhóm `moderate rain`.  
Điều này cho thấy trễ không phụ thuộc tuyến tính vào mưa, mà phụ thuộc vào **mức độ phơi nhiễm với điểm ngập**.

***

## 5. Tổng hợp Insight từ EDA

Từ các kết quả trên, EDA rút ra các kết luận chính:

1.  Trễ giao hàng **không tỷ lệ thuận với lượng mưa**.
2.  **Ngập lụt cục bộ** là yếu tố rủi ro quan trọng nhất.
3.  ETA hiện tại:
    *   Quá dư (over-buffer) trong mưa lớn
    *   Thiếu thích ứng trong các tình huống ngập nhẹ, phân tán
4.  Nhiều phân đoạn mưa/ngập là **rare events**, không đủ dữ liệu để huấn luyện mô hình thuần ML.

***

## 6. Liên hệ trực tiếp với mô hình ML ETA

Các insight từ EDA dẫn trực tiếp đến các quyết định mô hình hóa sau:

*   **Data Segmentation:** Tách theo bối cảnh vận hành để tránh trộn các phân phối khác nhau.
*   **Feature Engineering:** Ưu tiên đặc trưng định lượng phản ánh ngập (`severity_score`) hơn là nhãn mưa.
*   **Model Gating:** Không huấn luyện mô hình cho các phân đoạn thiếu dữ liệu để đảm bảo tính ổn định thống kê.
*   **Dynamic ETA:** Thay vì tối ưu giao nhanh hơn, mục tiêu là **dự báo ETA sát thực tế hơn theo bối cảnh**.

***

## 7. Kết luận EDA

> EDA cho thấy trễ giao hàng trong đô thị TP.HCM là hiện tượng có cấu trúc, chịu ảnh hưởng mạnh bởi ngập lụt cục bộ hơn là mưa diện rộng. Những kết quả này giải thích vì sao mô hình ETA cần được điều chỉnh theo bối cảnh (Dynamic ETA), và vì sao việc phân đoạn dữ liệu giúp giảm RMSE đáng kể trong giai đoạn huấn luyện mô hình.