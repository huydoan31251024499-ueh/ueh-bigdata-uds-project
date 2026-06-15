### PHẦN 1: DẪN NHẬP & BÀI TOÁN KINH DOANH (3 PHÚT)

*   **Slide 1: Tiêu đề & QR Code GitHub**
    *   Tên đề tài: "Hệ Thống Phân Tích Logistics Thích Ứng Thời Tiết Cho Startup UDS".
    *   QR dẫn trực tiếp đến Repository GitHub để giảng viên kiểm tra tính minh bạch.

*   **Slide 2: Business Task & Case Study cụ thể (Phản hồi GV)**
    *   Xác định UDS là startup giao hàng chặng cuối (Last-mile) tại TP.HCM, phục vụ nhóm B2B/B2C hàng nhỏ gọn.
    *   Xác định 3 bài toán Logistics cốt lõi: Đúng hàng, Đúng giá, **Đúng thời gian (Trọng tâm Q1) nhấn mạnh - đào sâu - chia nhỏ để quản lý**.
    *   Thay vì làm dàn trải, nhóm tập trung giải quyết triệt để **Q1: Dự báo ETA động**.
    *   Lấy Q2 (Kinh tế) và Q3 (Không gian) làm bối cảnh: Trễ đơn gây mất 30% thu nhập tài xế và giảm 30% hiệu suất lộ trình.

*   **Slide 3: Xác định Các bên liên quan (Phản hồi GV)**
    *   **Executive Sponsors:** CEO và Nhà sáng lập Molotov (Người phê duyệt chiến lược và ngân sách vận hành).
    *   **End-User*** Đội ngũ Shipper (Đối tượng nhận thông tin điều hướng và các gói ưu đãi thời tiết). Khách hàng cuối (Người dùng nhận được dịch vụ ổn định và thông tin ETA chính xác hơn).


### PHẦN 2: HẠ TẦNG & DATA PIPELINE (3 PHÚT)

*   **Slide 4: Khung công nghệ phân tán (Tech Stack)**
    *   Hình ảnh: Sơ đồ 8 Container (Namenode, Datanode, Spark, Kafka, Zookeeper) đang vận hành.
    *   Nhấn mạnh: Hệ thống chạy thực tế trên cụm ảo hóa, không phải chạy script đơn lẻ.

*   **Slide 5: Lambda-light Pipeline & 5Vs (Execution Proof)**
    *   Sơ đồ luồng: **API (Open-Meteo) $\rightarrow$ Kafka $\rightarrow$ Spark $\rightarrow$ HDFS**.
    *   Phản hồi GV về Input: Phân tách rõ dữ liệu cấu trúc (CSV đơn hàng), bán cấu trúc (JSON thời tiết) và phi cấu trúc (Crawl điểm ngập).

*   **Slide 6: Temporal Joining Key (Kỹ thuật cốt lõi)**
    *   Hình ảnh đoạn code: `date_trunc("hour", col("timestamp"))`.
    *   Giải quyết bài toán đồng bộ luồng: Khớp nối dữ liệu Streaming lệch tần suất (5 phút/lần) với dữ liệu Batch lịch sử.

*   **Slide 7: Biện minh khoa học cho Feature Engineering**
    *   Công thức **Flood Severity Score**: $depth \times 0.7 + duration \times 0.3$.
    *   Lập luận dựa trên rào cản vật lý: Xe máy "đứng bánh" khi ngập > 20cm (Tiêu chuẩn TCVN 4054:2005).

### PHẦN 3: PHÂN TÍCH & MÁY HỌC (ANALYZE - TRỌNG TÂM 5 PHÚT)

*   **Slide 8: EDA - Làm rõ tương quan (Phản hồi nhận xét GV #5)**
    *   Hình ảnh biểu đồ: Phân tích tương quan Mưa $\rightarrow$ Ngập $\rightarrow$ Trễ đơn.
    *   Insight thực thi: Ngập lụt cục bộ (Flood-only) gây rủi ro trễ cao nhất (24.23%), vượt trội so với mưa diện rộng.
*   **Slide 9: Feature Engineering dựa trên Rào cản vật lý**
    *   Công thức WIS và **Flood Severity Score** ($depth \times 0.7 + duration \times 0.3$).
    *   Biện minh kỹ thuật: Ngưỡng ngập > 15cm là rào cản vật lý đối với ống pô xe máy, gây "đứng bánh" cấu trúc.
*   **Slide 10: Huấn luyện Offline & Model Gating (Analyze)**
    *   Hình ảnh: Kết quả RMSE cải thiện từ **~8 tiếng xuống còn 37.73 phút** thông qua phân đoạn dữ liệu (Normal, Rain, Flood).
    *   Giải thích cơ chế **Model Gating**: Điều hướng đơn hàng vào đúng nhánh trọng số mô hình đã tối ưu.
*   **Slide 11: Suy luận dòng Real-time (Execution Proof)**
    *   Hình ảnh đoạn code: `spark.readStream` và cơ chế **In-memory Dynamic Inference**.
    *   Tải trực tiếp Artifact mô hình từ HDFS lên RAM để dự báo tức thời.
### PHẦN 4: TRỰC QUAN & THỰC THI (SHARE/ACT - 3 PHÚT)

*   **Slide 12: Spark Web UI & Kafka UI (Execution Proof)**
    *   Hình ảnh: **DAG Visualization** và biểu đồ **Micro-batches** (Port 4040).
    *   Chứng minh với GV: Hệ thống đạt tiêu chuẩn xử lý phân tán, tốc độ xử lý tính bằng giây.
*   **Slide 13: Terminal & Log kết quả (Execution Proof)**
    *   Hình ảnh: Màn hình Console hiển thị đơn hàng thực tế đang được "kéo giãn" ETA khi gặp vùng ngập.
    *   Ví dụ: Đơn #7218 tự động cộng thêm 12.7 phút buffer do phát hiện bối cảnh cực đoan.
*   **Slide 14: Operational Dashboard (Share)**
    *   Hình ảnh: Dashboard hiển thị Heatmap điểm đen ngập lụt và trạng thái ETA linh động gửi cho quản lý vận hành.
*   **Slide 15: Kiểm chứng Design Thinking (Act)**
    *   Đối tượng End-User: Tài xế và Khách hàng.
    *   Kết quả phỏng vấn 4 người: Giải pháp mang lại giá trị "Win-Win" (An toàn tài xế - Minh bạch thông tin khách hàng).

### PHẦN 6: KẾT LUẬN & VẤN ĐÁP (1 PHÚT + 5 PHÚT)

*   **Slide 16: Định hướng Innovation (Phản hồi nhận xét GV)**
    *   Kinh tế: Đề xuất **Dynamic Pricing** (nhân hệ số 1.5x-2x) dựa trên "hình phạt kinh tế" 30% thu nhập đã phân tích.
    *   Kỹ thuật: Ứng dụng **Spark GraphX** để tìm Safe-Path (Lộ trình an toàn) né điểm ngập sâu.
*   **Slide 17: Q&A**


---

### CÁC ĐIỂM CẦN LƯU Ý:
1.  **Hạn chế lý thuyết suông:** Mỗi slide kỹ thuật (Prepare/Process/Analyze) phải có ít nhất 1 hình ảnh thực tế (Code, Terminal, Spark UI, Dashboard).
2.  **Biện minh hệ số:** Khi trình bày công thức WIS hay Severity Score, hãy nhấn mạnh các con số này được rút ra từ phân tích tương quan Pearson hoặc tiêu chuẩn hạ tầng, không phải con số cảm tính.
3.  **Tập trung vào "Dòng chảy":** Luôn nhắc đến việc dữ liệu di chuyển từ API $\rightarrow$ Kafka $\rightarrow$ Spark $\rightarrow$ HDFS $\rightarrow$ Dashboard để thể hiện nhóm kiểm soát được Data Pipeline.
