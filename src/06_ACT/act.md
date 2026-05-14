# 1. Insights quan trọng từ việc phân tích và trực quan hoá
- EDA cho thấy trễ giao hàng trong đô thị TP.HCM là hiện tượng có cấu trúc, chịu ảnh hưởng mạnh bởi ngập lụt cục bộ hơn là mưa diện rộng. Những kết quả này giải thích vì sao mô hình ETA cần được điều chỉnh theo bối cảnh (Dynamic ETA), và vì sao việc phân đoạn dữ liệu giúp giảm RMSE đáng kể trong giai đoạn huấn luyện mô hình.


- Kết quả huấn luyện mô hình ML sau chỉnh sửa cho thấy:
    *   ETA **có thể dự báo chính xác hơn đáng kể** trong điều kiện vận hành bình thường khi mô hình được điều chỉnh theo bối cảnh (giảm RMSE xuống **37.73 phút** cho phân đoạn 3h–normal).
    *   **Ngập lụt cục bộ**, chứ không phải mưa lớn, là yếu tố quyết định rủi ro trễ giao hàng.
    *   Dynamic ETA dựa trên **flood severity và trạng thái giao thông** là hướng tiếp cận khả thi hơn so với ETA tĩnh.


- Phân tích dữ liệu thực tế cho thấy thu nhập hiệu quả của tài xế giảm mạnh nhất trong các tình huống ngập lụt không đi kèm mưa (flood-only), với mức suy giảm lên tới khoảng 30% so với điều kiện bình thường. Điều này chứng minh rằng hình phạt kinh tế trong vận hành đô thị không xuất phát trực tiếp từ mưa, mà từ trạng thái ngập tích tụ của hạ tầng. Kết quả này cung cấp cơ sở định lượng rõ ràng để thiết kế các cơ chế điều tiết phù hợp trong pha ACT, nhằm ổn định thu nhập tài xế và giảm tỷ lệ hủy đơn trong các điều kiện bất lợi.


# 2. Đề xuất giải pháp

**Các SMART Questions ban đầu:**
- **Q1:** Làm thế nào để xác định và dự báo được nguy cơ giao hàng trễ (delay_min > 0) đối với các dịch vụ giao hàng nhanh 3h và 5h của UDS trong các điều kiện mưa lớn (lượng mưa > 5mm) tại khu vực có nguy cơ ngập lụt tại TP.HCM? giai đoạn 2023-2024?
- **Q2:** Mối quan hệ giữa lượng mưa, mức độ ngập lụt và tình trạng ùn tắc giao thông ảnh hưởng như thế nào đến thu nhập của tài xế?
- **Q3:** Làm thế nào để giảm 10% quãng đường vận chuyển thực tế thông qua việc điều hướng tránh các điểm ngập lụt tại TP.HCM vào mùa mưa 2024?

**Ứng dụng Xe Dù UDS, kết hợp 3 tính năng cốt lõi:**
- **A1:** Hệ thống Dashboard thời gian thực dự đoán ETA bằng mô hình Machine Learning thông qua việc kết hợp phân tích dữ liệu lịch sử và dữ liệu thời gian thực (API) về thời tiết, giao thông và ngập lụt trong giai đoạn vận hành 3 tháng mùa mưa 2026.
- **A2:** Áp dụng chính sách dynamic pricing - giá linh động (thưởng thêm cho tài xế, ưu đãi cho khách hàng) dựa trên ETA thực giúp giảm tỷ lệ huỷ đơn của tài xế và tăng sự hài lòng của khách hàng.
- **A3:** Ứng dụng Google Maps API và dữ liệu các điểm ngập đô thị, dữ liệu thời tiết theo thời gian thực để tối ưu hoá lộ trình di chuyển trong giai đoạn vận hành 3 tháng mùa mưa 2026.


# 3. Định hướng tiếp theo
Để các giải pháp trên đạt hiệu quả tối đa vào mùa mưa 2026, UDS cần tập trung vào:
*   **Tích hợp Kafka & Spark Streaming:** Chuyển đổi từ phân tích dữ liệu lịch sử sang xử lý luồng dữ liệu thời gian thực của thành phố.
*   **Ứng dụng mô hình Design Thinking** (Desgin School, Stanford University) để tiến hành kiểm thử giải pháp: Giai đoạn ACT (Google Data Analytics) hiện tại đang quy chiếu với giai đoạn IDEATE và PROTOTYPE của Design Thinking. Nhóm dự án cần
    - Tạo mẫu thử Low-Fidelity hoặc Mid-Fidelity.
    - Tạo bộ câu hỏi để kiểm thử giả định với người dùng cuối (tài xế, khách hàng).
    - Tiến hành thu thập dữ liệu từ phỏng vấn sâu cùng người dùng và mẫu thử.
    - Lặp lại qui trình Data Analytics nhằm phân tích dữ liệu người dùng đã thu thập.
    - Rút ra insights về người dùng và giải pháp.
    - Tiến hành các bước tiếp: hoàn thiện giải pháp/tính năng, xây dựng mẫu thử High-Fidelity và tiến tới xây dựng MVP hoàn chỉnh.


### 1. Triển khai Hệ thống Dashboard Dự báo ETA Thông minh (Giải quyết Q1)
Dựa trên kết quả huấn luyện mô hình máy học với chỉ số **RMSE đạt mức ~38-40 phút** (giảm đáng kể từ mức 8 tiếng ban đầu), nhóm đề xuất tính năng **A1**:
*   **Cơ chế ETA:** Sử dụng mô hình Spark MLlib để tính toán thời gian giao hàng cơ bản trong điều kiện bình thường. Đối với các bối cảnh rủi ro cao đã xác định (như phân đoạn `flood_only` có xác suất trễ **24.23%**).
*   **Cảnh báo rủi ro thời gian thực:** Dashboard sẽ hiển thị Heatmap các vùng "điểm đen" về trễ đơn dựa trên dữ liệu ngập lụt 2025. Khi hệ thống Kafka nhận tín hiệu mưa lớn (>5mm) hay ngập lụt, App sẽ tự động gửi thông báo điều chỉnh kỳ vọng thời gian đến khách hàng để giảm tỷ lệ hủy đơn.

### 2. Chính sách Giá linh động (Giải quyết Q2)
Phân tích Spark SQL đã định lượng được "Hình phạt kinh tế" (Economic Penalty) khiến thu nhập tài xế sụt giảm **~30%** (từ 266 VND xuống còn **187 VND/phút**) trong các khu vực ngập. Nhóm đề xuất tính năng **A2**:
- Thưởng thêm cho tài xế tại những điều kiện nhất định nhằm hạn chế tình trạng huỷ đơn, tài xế không nhận đơn, tăng tỷ lệ hoàn thành chuyến và mức độ hài lòng của khách hàng. Tác động kỳ vọng: Ổn định `income_per_min`, Giảm từ chối / hủy đơn, Giữ supply trong điều kiện bất lợi


### 3. Tối ưu hóa lộ trình tránh "Điểm đen" ngập lụt (Giải quyết Q3)
Mục tiêu giảm **10% quãng đường thực tế** được thực hiện thông qua tính năng **A3**:
*   **Flood-Aware Routing:** Tích hợp dữ liệu tọa độ của **159 điểm ngập** đã xử lý từ `flood_hourly.csv` vào Google Maps API.
*   **Điều hướng thông minh:** Thay vì đi theo quãng đường ngắn nhất (Shortest Path) nhưng rủi ro cao, hệ thống sẽ đề xuất "Lộ trình an toàn" (Safe Path) tránh các vùng có `flood_avg_severity` cao. Việc tránh các rào cản vật lý này không chỉ giúp giảm quãng đường di chuyển thực tế (do không phải quay đầu khi gặp đường ngập) mà còn bảo vệ phương tiện của tài xế khỏi hư hỏng.
