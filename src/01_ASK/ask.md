# Giai đoạn ASK (Lĩnh vực và Doanh nghiệp)

## 1.1 Lý do chọn đề tài:

Trong bối cảnh thị trường logistics Việt Nam đang bước vào giai đoạn cạnh tranh khốc liệt,
các startup như Xe Dù (Molotov/UDS) phải đối mặt với áp lực kép: vừa duy trì chi phí vận
hành thấp, vừa đảm bảo chất lượng dịch vụ. TP.HCM với mùa mưa kéo dài cùng nạn ngập
cục bộ triền miên thường xuyên phá vỡ hoạt động giao vận cuối cùng. Đáng chú ý, những
doanh nghiệp giao hàng lớn như Ninja Van, Flex Speed lần lượt rời bỏ mảng vận chuyển
chặng cuối, nhường chỗ cho SPX Express và J&T Express - các đơn vị thuộc hệ sinh thái
sàn thương mại điện tử. Cuộc thanh lọc này đòi hỏi những công ty non trẻ phải lấy dữ liệu
và công nghệ làm vũ khí cạnh tranh. 

Molotov chưa xây dựng được công cụ điều phối thông
minh dựa trên thời tiết tức thời, dẫn đến đơn hủy tăng cao, giao hàng kéo dài, chi phí vận
hành phình to và khách hàng bất mãn. Đó là động lực để nhóm triển khai đề tài: “Hệ Thống
Phân Tích Logistics Dựa Trên Dữ Liệu Lớn Thời Tiết Và Vận Tải Tại TP. HCM Cho Startup
UDS”.

---

## 1.2 Business Task

Bài toán kinh doanh: "Xe Dù" (Molotov/UDS) là một startup logistics tại TP.HCM đối mặt với
sự sụt giảm hiệu suất vận hành nghiêm trọng khi thời tiết xấu. Cụ thể, khi lượng mưa tăng cao, tỷ lệ hủy đơn tăng mạnh, thời gian giao hàng thực tế vượt xa ETA cam kết vì Shipper bị kẹt tại các "điểm đen" ngập lụt, gây tốn kém nhiên liệu và chi phí cơ hội, giảm sự hài lòng của khách hàng; và thu nhập của tài xế giảm do không thể hoàn thành đủ số chuyến dẫn đến tình trạng tài xế rời bỏ nền tảng. 

Mục tiêu của dự án là đồng bộ hóa dữ liệu ngoại cảnh (thời tiết, ngập lụt, giao thông) để tối ưu hóa nguồn lực tài
xế hạn chế, từ đó xây dựng một hệ thống logistics có khả năng thích ứng thời tiết (weather-adaptive logistics).

| Bên liên quan | Vai trò | Mối quan tâm chính |
| :--- | :--- | :--- |
| **CEO Molotov** | Phê duyệt chiến lược & ngân sách | Giảm chi phí vận hành, tăng thị phần |
| **Operations Manager** | Điều phối tài xế hàng ngày | Tỷ lệ hoàn thành đơn, hiệu suất đội xe |
| **Đội ngũ Shipper** | Đối tượng nhận điều hướng | Thu nhập ổn định, an toàn khi thời tiết xấu |
| **Khách hàng cuối** | Người dùng dịch vụ | ETA chính xác, giao hàng đúng hẹn |

---
## 1.3 Xác định câu hỏi SMART
Đặt 3 câu hỏi mục tiêu về: Tỷ lệ hủy đơn, Thu nhập tài xế và Quãng đường vận chuyển tối
ưu.

**1. Làm thế nào để dự báo chính xác thời gian giao hàng thực tế và giảm ít nhất 20% độ trễ so với ETA cam kết (delay_min) trong các khung giờ mưa lớn (lượng mưa > 5mm) tại các khu vực có nguy cơ ngập lụt cao ở TP.HCM trong vòng 3 tháng tới, thông qua việc tự động điều chỉnh ETA dựa trên dữ liệu thời tiết, giao thông và ngập lụt?**

- Specific: tập trung vào delay_min, ETA, mưa > 5mm, vùng ngập
- Measurable: giảm 20% độ trễ trung bình
- Achievable: có prcp_mm, traffic_congestion_index, flood_*, shippingDistance
- Relevant: giảm hủy đơn gián tiếp, tăng hài lòng khách hàng
- Time-bound: 3 tháng
  
**2. Mối quan hệ giữa lượng mưa, mức độ ngập lụt và tình trạng ùn tắc giao thông ảnh hưởng như thế nào đến thu nhập của tài xế. Việc áp dụng chính sách giá linh động (tăng thu nhập tài xế, ưu đãi) dựa trên chỉ số thời tiết thời gian thực giúp giảm tỷ lệ huỷ đơn và tăng sự hài lòng của khách hàng thế nào?**
- S (Cụ thể): Kết hợp mô hình giá linh động với ưu đãi tài xế (Rainy-day
Incentives) tại các vùng có chỉ số ngập lụt cao.
- M (Đo lường được): Tỷ lệ hủy đơn, thu nhập trung bình mỗi tài xế trong giờ
mưa, điểm hài lòng của khách hàng.
- A (Khả thi): Sử dụng dữ liệu bản đồ ngập lụt 2025 từ Sở GTVT kết hợp với
dữ liệu thời tiết lịch sử.
- R (Phù hợp): Giải quyết bài toán giữ chân tài xế trong điều kiện thời tiết bất
lợi - yếu tố sống còn với startup logistics.
- T (Thời hạn): Mùa mưa 2026 - khung thời gian thực tế để kiểm định giả
thuyết.

**3. Làm thế nào để giảm 10% quãng đường vận chuyển thực tế
thông qua việc điều hướng tránh các điểm ngập lụt tại TP.HCM vào mùa mưa
2024?**
- S (Cụ thể): Giảm quãng đường vận chuyển thực tế bằng cách tích hợp dữ
liệu điểm ngập vào thuật toán điều hướng.
- M (Đo lường được): So sánh shippingDistance thực tế với khoảng cách tuyến
đường tối ưu (Great-circle distance); tỷ lệ chênh lệch (detour index).
- A (Khả thi): Tận dụng dữ liệu lịch sử đơn hàng Kaggle giai đoạn 2023-
kết hợp bản đồ điểm đen giao thông TP.HCM.
- R (Phù hợp): Trực tiếp giảm chi phí nhiên liệu và tăng số chuyến mỗi tài xế
có thể hoàn thành trong ca.
- T (Thời hạn): Mùa mưa 2024 - sử dụng dữ liệu lịch sử để huấn luyện mô hình
dự báo.
---
## TỔNG HỢP TÀI LIỆU THAM KHẢO

**2.1 Tác động định lượng của thời tiết đến vận tải và logistics**
FHWA - Road Weather Management (Cục Quản lý Đường cao tốc Liên bang Hoa Kỳ):
- 23% tổng số chậm trễ trên đường bộ toàn quốc là do thời tiết bất lợi gây ra.
- Nghiên cứu ban đầu ước tính chi phí chậm trễ do thời tiết gây thiệt hại cho ngành
vận tải đường bộ từ 8 đến 9 tỷ USD mỗi năm.
- Tại các khu vực đô thị, tài xế xe tải thiệt hại khoảng 3,4 tỷ USD (32 triệu giờ) do
    chậm trễ giao thông liên quan đến thời tiết. Một ngày đóng cửa đường cao tốc có thể
    khiến khu vực đô thị thiệt hại tới 76 triệu USD.

**2.2 Tác động của thời tiết đến giao hàng chặng cuối (Last-Mile Delivery)**
OpenWeather - Weather Intelligence for Last-Mile Delivery (04/2025):
- Giao hàng chặng cuối là giai đoạn cuối cùng, quan trọng nhưng rất dễ bị ảnh hưởng
bởi thời tiết.
- Mưa lớn và ngập lụt làm giảm tầm nhìn, tăng khoảng cách phanh, hư hỏng bưu kiện
và khiến nhiều tuyến đường không thể đi qua, gây ra sự chậm trễ lớn. Chi phí vận
hành tăng do nỗ lực giao hàng thất bại, tiêu thụ nhiên liệu tăng, phải trả lương làm
thêm giờ và hao mòn phương tiện.
- 23% tổng số chậm trễ của xe tải là do thời tiết, gây thiệt hại hàng tỷ USD mỗi năm.

**2.3 Ứng dụng Big Data và AI trong logistics thích ứng thời tiết**
GEODIS (Tập đoàn logistics toàn cầu): Các hãng như DHL, Kuehne+Nagel, GEODIS, XPO
Logistics, Flexport, ShipBob đang triển khai các hệ thống hỗ trợ bởi AI, phân tích dữ liệu
thời gian thực về giao thông, thời tiết, giá nhiên liệu để tạo ra các tuyến đường tối ưu một
cách liên tục. Kết quả ban đầu cho thấy mức giảm tiêu thụ nhiên liệu từ 12–28% và cải thiện
hiệu suất giao hàng đúng giờ từ 15-35%.
Visual Crossing - Loading Weather into your Datastores (2026): Tích hợp dữ liệu thời tiết
lịch sử vào kho dữ liệu của doanh nghiệp có thể cách mạng hóa trí tuệ kinh doanh: huấn
luyện các mô hình AI để dự báo những thách thức logistics dựa trên các mẫu thời tiết trong
quá khứ, tối ưu hóa nhân sự và hàng tồn kho.

**2.4 Bối cảnh thị trường logistics Việt Nam**
Tuổi Trẻ - TP.HCM lên lộ trình xử lý 159 điểm ngập (10/04/2026): Toàn TP.HCM hiện có 159
điểm ngập nước, trong đó 57 điểm ngập nặng, thời gian ngập kéo dài trên 1 giờ 30 phút,
gây ùn tắc giao thông, ảnh hưởng đời sống người dân.
VnExpress - Nhiều đường ở TP HCM ngập sâu sau mưa lớn đầu mùa (02/05/2026): Cơn
mưa lớn đầu mùa kéo dài khoảng 30 phút khiến nhiều đường ngập nặng. Đường Trương
Văn Ngư nước ngập hơn nửa bánh xe máy. Nước cuồn cuộn trên đường Dương Văn Cam.
CafeF - Ninja Van Việt Nam thông báo dừng toàn bộ dịch vụ giao nhận nhanh (04/09/2025):
Ninja Van Việt Nam chính thức dừng toàn bộ dịch vụ giao nhận nhanh (B2C và B2B) từ
30/09/2025 sau gần 10 năm hoạt động, trong khuôn khổ kế hoạch tái cấu trúc.
Kênh 14 - Một đơn vị giao hàng online dừng dịch vụ từ 31/3 (06/03/2026): Công ty TNHH
Giao hàng Flex Speed (LEX) chính thức ngừng cung cấp dịch vụ giao hàng chặng cuối từ
ngày 31/03/2026.

## KẾT LUẬN

Dự án này áp dụng quy trình 6 bước của Google Data Analytics (ASK - PREPARE -
PROCESS - ANALYZE - SHARE - ACT), hướng tới mục tiêu xây dựng một hệ thống có khả
năng: 
- (1) tự động điều chỉnh ETA dựa trên dữ liệu thời tiết,
- (2) đề xuất chính sách giá linh
động và ưu đãi tài xế tại các vùng ngập lụt
- (3) tối ưu hóa tuyến đường giao hàng để giảm
quãng đường vận chuyển thực tế.

Ba câu hỏi SMART được xác định trong phần ASK sẽ là
kim chỉ nam cho toàn bộ quá trình phân tích và ra quyết định, đảm bảo mọi hoạt động của
dự án đều hướng đến giá trị kinh doanh cụ thể cho Molotov.

**Problem Statement:** Tại TP.HCM, thời tiết xấu – đặc biệt là mưa lớn và ngập lụt – làm tỷ lệ
hủy đơn tăng vọt, thời gian giao hàng kéo dài và thu nhập tài xế giảm mạnh. Molotov (Xe
Dù) hiện chưa có cơ chế dự báo và điều phối dựa trên dữ liệu thời tiết thực, dẫn đến thất
thoát doanh thu và trải nghiệm khách hàng kém, nhất là khi cạnh tranh trong mảng giao
hàng chặng cuối đang khốc liệt hơn bao giờ hết.

---

## **TÀI LIỆU THAM KHẢO**

1.  **Cục Quản lý Đường cao tốc Liên bang (FHWA):**
    *   [Ước tính ban đầu về hành lang đô thị và nông thôn](https://ops.fhwa.dot.gov/publications/fhwahop16044/chap1.htm#:~:text=The%20initial%20estimate%20indicated%20that,both%20urban%20and%20rural%20corridors.)
    *   [Tổng quan về ảnh hưởng của thời tiết đến giao thông và tai nạn](https://ops.fhwa.dot.gov/weather/overview.htm#:~:text=During%20bad%20weather%2C%20every%20year,and%20over%20450%2C000%20injury%20crashes.)

2.  **Công nghệ và Tối ưu hóa Logistics:**
    *   [OpenWeather: Trí tuệ thời tiết trong giao hàng chặng cuối (Last-mile delivery)](https://openweather.co.uk/blog/post/weather-intelligence-last-mile-delivery)
    *   [Flex Logistik: Các công ty đầu tư vào tối ưu hóa lộ trình nâng cao](https://flexlogistik.de/logistics-firms-invest-in-advanced-route-optimization/)
    *   [Visual Crossing: Tích hợp dữ liệu thời tiết vào kho lưu trữ dữ liệu](https://www.visualcrossing.com/resources/blog/loading-weather-into-your-datastores/)

3.  **Bối cảnh hạ tầng và ngập lụt tại TP.HCM:**
    *   [Tuổi Trẻ: Lộ trình xử lý 159 điểm ngập tại TP.HCM trong 5 năm tới](https://tuoitre.vn/tp-hcm-len-lo-trinh-xu-ly-159-diem-ngap-trong-5-nam-toi-20230531110543913.htm)
    *   [VnExpress: Nhiều tuyến đường ngập sâu sau cơn mưa lớn đầu mùa](https://vnexpress.net/nhieu-duong-o-tp-hcm-ngap-sau-sau-mua-lon-dau-mua-5069161.html)

4.  **Tin tức thị trường giao nhận:**
    *   [CafeF: Ninja Van Việt Nam thông báo dừng dịch vụ giao nhận nhanh từ cuối tháng 9/2025](https://cafef.vn/ninja-van-viet-nam-thong-bao-dung-toan-bo-dich-vu-giao-nhan-nhanh-tu-cuoi-thang-9-2025-188250904075903726.chn)
    *   [Kênh14: Đơn vị giao hàng online thông báo dừng dịch vụ](https://kenh14.vn/mot-don-vi-giao-hang-online-dung-dich-vu-tu-31-3-215260306212841582.chn)


