# Instruction: Giai đoạn PREPARE (Chuẩn bị dữ liệu)

## I. Mục tiêu
Xây dựng dữ liệu đầu vào chất lượng cho dự án. Nhiệm vụ chính là kết hợp dữ liệu vận hành nội bộ với các nguồn dữ liệu mở (Open Data) của TP.HCM để trả lời 3 câu hỏi SMART về: **Tỷ lệ hủy đơn, Thu nhập tài xế và Quãng đường tối ưu.**

---

## II. Các nguồn dữ liệu cần thu thập

### 1. Dữ liệu Nội bộ (Đã có sẵn)
* **Tên file:** `uds-orders.csv`
* **Đặc điểm:** Dữ liệu có cấu trúc (Structured).
* **Thông tin chính:** ID đơn hàng, tọa độ Người gửi/Người nhận, quãng đường, thời gian tạo đơn.

### 2. Dữ liệu Bên ngoài (Cần bổ sung)
| Nhóm dữ liệu | Nguồn chính (Source) | Phương pháp & Công cụ | Ý nghĩa & Câu hỏi SMART mục tiêu |
| :--- | :--- | :--- | :--- |
| **Thời tiết (Weather)** | OpenWeatherMap | **API Request** (Python `requests`) | Cung cấp lượng mưa (`prcp_mm`) để trả lời câu hỏi **giảm 20% tỷ lệ hủy đơn** qua chỉ số ETA. |
| **Ngập lụt (Flooding)** | UDI Maps / Cổng dữ liệu mở TP.HCM | **Crawl / OSM API** (`BeautifulSoup`, `OSMnx`) | Xác định tọa độ các điểm ngập giao hàng. |
| **Giao thông (Traffic)** | giaothong.hochiminhcity.gov.vn | **Web Scraping / API** (Google Maps API) | Lấy vận tốc trung bình và tình trạng kẹt xe để tối ưu **10% quãng đường vận chuyển**. |
| **Thị trường (Market)** | Petrolimex / Giá cước Grab, Be, Gojek | **API / Manual Collection** (Python `requests`) | Tham chiếu giá xăng và biểu phí Surge Pricing để mô phỏng mô hình **tăng thu nhập tài xế**. |

---

## 🚀 III. Hướng dẫn thực hiện cho thành viên

### **Bước 1: Khai thác API & Crawl**
* Sử dụng Python (`requests`, `BeautifulSoup`) để lấy dữ liệu từ các nguồn trên.
* Ưu tiên lấy các dữ liệu có tọa độ GPS để đồng bộ với dữ liệu đơn hàng.

### **Bước 2: Kiểm tra chất lượng (Data Validation)**
* Đảm bảo dữ liệu không bị trùng lặp.
* Kiểm tra định dạng ngày tháng và đơn vị đo lường (Ví dụ: Lượng mưa phải thống nhất là `mm`).

### **Bước 3: Lưu trữ**
* Đẩy tất cả dữ liệu thô thu thập được vào thư mục: `data/raw/`.
* Cập nhật thông tin về nguồn dữ liệu vào file `data_source.md`.

---

## 📦 IV. Sản phẩm bàn giao (Outputs)
Sau khi kết thúc giai đoạn này, folder `02_PREPARE` phải có:
1.  **Script code:** Các file `.py` dùng để crawl hoặc gọi API.
2.  **Dữ liệu thô:** Các file CSV/JSON mới thu thập được.
3.  **Data Dictionary:** File Markdown mô tả ý nghĩa các cột dữ liệu mới để người làm bước **PROCESS** tiếp quản.

---

**Status:** 🟡 In Progress (Huy đã làm xong mẫu Orders & Weather, các bạn cần bổ sung Điểm ngập & Giá thị trường).




