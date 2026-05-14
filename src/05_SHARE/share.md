# GIAI ĐOẠN SHARE – TRỰC QUAN HOÁ DỮ LIỆU

Giai đoạn **SHARE** là bước chuyển hóa các kết quả từ công cụ tính toán phân tán (Spark) thành các câu chuyện bằng hình ảnh (Data Storytelling) để Xe Dù (UDS) ra quyết định chiến lược. Dựa trên cấu trúc thư mục kết quả phân tích và các chỉ số khoa học đã đạt được, dưới đây là định hướng trực quan hóa chi tiết:

### 1. Trực quan hóa SMART Q1: Xác định và Dự báo nguy cơ trễ đơn
Dựa vào các tệp tin trong `data/analysis/q1/`:
*   **Heatmap "Điểm đen" rủi ro (Hotspots):** Sử dụng dữ liệu từ `hotspots.csv` để hiển thị mật độ các vùng có xác suất trễ đơn cao nhất tại TP.HCM,. Bản đồ này cần nhấn mạnh sự chồng lấp giữa **159 điểm ngập** và các khu vực có `late_rate` vọt lên **24.23%** (đặc biệt là nhóm `flood_only`), .
*   **Biểu đồ phân đoạn rủi ro (Compound Risk):** Dùng biểu đồ cột chồng (Stacked Bar) từ `compound_risk.csv` để so sánh tỷ lệ trễ giữa các phân đoạn: Bình thường, Mưa lớn, và Ngập lụt,. 
*   **Insight cần nhấn mạnh:** Mô hình ML đã đạt **RMSE ~38-40 phút**, cho phép UDS hiển thị cảnh báo "Nguy cơ trễ đơn cao" trên Dashboard vận hành ngay khi nhận tín hiệu từ trạm quan trắc, .

### 2. Trực quan hóa SMART Q2: Mối quan hệ Kinh tế và Thu nhập tài xế
Dựa vào các tệp tin trong `data/analysis/q2/`:
*   **Waterfall Chart "Hình phạt kinh tế" (Penalty):** Trực quan hóa từ `penalty.csv` để cho thấy thu nhập mỗi phút (`income_per_min`) bị sụt giảm từ mức nền **266 VND/phút** xuống còn **187 VND/phút** (giảm ~30%) khi đi vào vùng ngập .
*   **Biểu đồ so sánh Hiệu quả trợ giá (Simulation):** Sử dụng biểu đồ cột đôi từ `simulation.csv` để so sánh thu nhập thực tế và thu nhập sau khi áp dụng **Hệ số nhân phí 1.5x** . Cần nhấn mạnh việc thu nhập tăng lên **385 VND/phút** chính là động lực kinh tế giữ chân tài xế (Risk Premium) .
*   **Insight cần nhấn mạnh:** Chính sách giá linh động không chỉ là tăng giá mà là công cụ **cân bằng cung - cầu** dựa trên rủi ro vật lý của lộ trình, .

### 3. Trực quan hóa SMART Q3: Hiệu quả lộ trình và Quãng đường thực tế
Dựa vào các tệp tin trong `data/analysis/q3/`:
*   **Biểu đồ Radar "Hiệu suất vận hành" (Efficiency):** Sử dụng dữ liệu từ `efficiency.csv` để so sánh chỉ số hiệu quả (Km/phút) giữa các phân đoạn. Nhấn mạnh việc ngập lụt làm tê liệt vận tốc khiến hiệu suất giảm từ **88 xuống 61** (~30%) dù quãng đường lý thuyết có thể ngắn hơn, .
*   **Biểu đồ tán xạ (Scatter Plot) Tương quan:** Trực quan hóa từ `correlation.csv` để chỉ ra rằng độ sâu ngập có mối quan hệ phi tuyến tính với thời gian giao hàng (vượt ngưỡng 20cm là gây tắc nghẽn hoàn toàn), .
*   **Insight cần nhấn mạnh:** Sự lãng phí quãng đường (Detour Index) phát sinh do shipper thiếu thông tin điều hướng, tạo tiền đề cho tính năng **A3 (Flood-Aware Routing)** để giảm 10% quãng đường vận chuyển,, .

### 4. Công nghệ phân tán sử dụng trong bước SHARE
Để đảm bảo tính nhất quán (Veracity) và khả năng mở rộng (Scalability), nhóm nên sử dụng:
*   **Spark SQL làm Query Engine:** Kết nối trực tiếp các tệp Parquet/CSV trên HDFS với công cụ BI (Looker Studio/Power BI) thông qua JDBC/ODBC để thực hiện các truy vấn gộp thời gian thực,,.
*   **Spark Streaming & Kafka (Mô phỏng):** Nếu muốn trình bày Dashboard thời gian thực cho tính năng **A1**, nhóm sử dụng Spark Streaming để đẩy dữ liệu dự báo từ mô hình ETA liên tục lên giao diện Share,,.
*   **HDFS làm lớp lưu trữ bền vững:** Toàn bộ dữ liệu trực quan được truy xuất từ lớp `processed` và `joined` đã được làm sạch, đảm bảo tính xác thực của Insights,.

**Kết luận:** Bước SHARE không chỉ là báo cáo số liệu mà là bằng chứng định lượng để chứng minh rằng bộ 3 tính năng **A1 (Dashboard ETA), A2 (Dynamic Pricing) và A3 (Điều hướng tránh ngập)** là giải pháp sống còn cho UDS vào mùa mưa 2026, .