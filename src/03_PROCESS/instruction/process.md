**Các bước thực hiện**

**process_pipeline_v2.py**

**Weather - các bước xử lý:**

- Đọc CSV từ HDFS với Schema-on-read (StructType định nghĩa sẵn 9 cột: timestamp TimestampType, temp/prcp/wspd/pres DoubleType, rhum/wdir/cldc/coco IntegerType)
- Chuyển timestamp từ UTC sang UTC+7 (Asia/Ho_Chi_Minh) bằng from_utc_timestamp
- Thêm cột condition_label bằng create_map ánh xạ 27 mã coco sang nhãn chữ: 1=Clear, 2=Fair, 3=Cloudy, 4=Overcast, 5=Foggy, 7=Light Rain, 8=Rain, 9=Heavy Rain, 23=Lightning, 25=Thunderstorm, 26=Heavy Thunderstorm, 27=Storm
- fillna bằng coalesce: prcp=0.0, cldc=0, wdir=0, wspd=0.0
- cast tường minh sang DoubleType: temp, rhum, prcp, wdir, wspd, cldc, pres
- Đổi tên 8 cột kèm đơn vị: temp→temp_c, rhum→rhum_pct, prcp→prcp_mm, wdir→wdir_deg, wspd→wspd_kmh, cldc→cldc_pct, pres→pres_hpa, coco→coco_code
- Tạo hour_timestamp = round(unix_timestamp(timestamp)/3600)\*3600 làm Temporal Join Key
- Kết quả: 17.544 dòng → 17.544 dòng (không xóa dòng nào)

**Orders - các bước xử lý:**

- Đọc CSV từ HDFS với Schema-on-read (StructType 18 cột: id/createdAt/deliveredAt/expectedDeliveryTime/mdh/package_name/orderStatus/senderAddress/receiverAddress/shipper/serviceType/image là StringType; senderLat/senderLng/receiverLat/receiverLng/shippingDistance/weight là DoubleType)
- Lọc GPS hợp lệ vùng TP.HCM: senderLat between(10.4, 11.2), senderLng between(106.3, 107.1), receiverLat between(10.4, 11.2), receiverLng between(106.3, 107.1) → loại 0 dòng
- Xóa dòng NULL: shipper isNotNull (loại 1 dòng), deliveredAt isNotNull (loại 8 dòng), expectedDeliveryTime isNotNull (loại 105 dòng) → tổng loại 113 dòng
- regexp_replace pattern \[^\\w\\s,./\] trên cột senderAddress và receiverAddress (giữ lại chữ cái, số, khoảng trắng, dấu phẩy, dấu chấm, dấu gạch chéo; loại ký tự đặc biệt)
- Parse và chuyển 3 cột timestamp từ UTC sang UTC+7: createdAt, deliveredAt, expectedDeliveryTime (format gốc: yyyy-MM-dd'T'HH:mm:ss.SSS'Z')
- Tạo shippingDistance_km = shippingDistance / 1000.0 (đổi m sang km)
- Tạo weight_kg = weight.cast(DoubleType())
- Tạo hour_timestamp = round(unix_timestamp(createdAt)/3600)\*3600 làm Temporal Join Key
- Kết quả: 2.403 dòng → 2.290 dòng (loại 113 dòng NULL)

**Output:** Ghi Parquet vào /uds/data/processed/weather_clean và /uds/data/processed/orders_clean

**process_flood_spark.py**

**Dữ liệu đầu vào:** hcmc_flood_points_raw.csv - 2.400 dòng, mỗi dòng là 1 sự kiện ngập tại 1 địa điểm cụ thể trong TP.HCM giai đoạn 2023-2024, thu thập từ 4 nguồn: IoT_sensor (1.062), user_report (521), monitoring_station (366), traffic_camera (179).

**Các bước xử lý:**

- Đọc CSV từ HDFS với Schema-on-read (StructType 13 cột: flood_id/street/district/source/flood_level/traffic_impact là StringType; depth_cm/rainfall_trigger_mm là DoubleType; duration_min là IntegerType; verified là BooleanType)
- Parse timestamp + chuyển UTC → UTC+7 bằng from_utc_timestamp (format gốc: yyyy-MM-dd HH:mm:ss)
- Lọc GPS hợp lệ vùng TP.HCM: lat between(10.4, 11.2), lng between(106.3, 107.1)
- Lọc nguồn tin cậy: giữ verified=true HOẶC source thuộc IoT_sensor/monitoring_station - loại bỏ user_report và traffic_camera chưa verified
- cast DoubleType tường minh: depth_cm, duration_min, rainfall_trigger_mm
- Tính flood_severity_score = depth_cm × duration_min / 100 - điểm càng cao càng cản trở giao thông nặng
- Tạo hour_timestamp = round(unix_timestamp(timestamp)/3600)\*3600 làm Temporal Join Key
- Aggregate theo giờ: group by hour_timestamp tổng hợp nhiều điểm ngập trong cùng 1 giờ thành: flood_count, avg_flood_depth_cm, avg_flood_severity_score, avg_flood_duration_min, avg_rainfall_trigger_mm, has_flood

**Output:** Ghi Parquet vào /uds/data/processed/flood_clean

**process_market_spark.py**

**Dữ liệu đầu vào:** hcmc_market_traffic_raw.csv - 17.544 dòng, mỗi dòng là snapshot 1 giờ của toàn TP.HCM giai đoạn 2023-2024, bao gồm giá xăng, chỉ số tắc nghẽn, tốc độ xe, phí giao hàng thị trường, giá hàng hoá.

**Các bước xử lý:**

- Đọc CSV từ HDFS với Schema-on-read (StructType 11 cột: fuel_price_vnd_liter/delivery_fee_avg_vnd/rice_price_vnd_kg/veg_price_vnd_kg là LongType; traffic_congestion_index/avg_vehicle_speed_kmh/freight_cost_index là DoubleType; active_delivery_vehicles/rain_flag/road_incidents là IntegerType)
- Parse timestamp + chuyển UTC → UTC+7 bằng from_utc_timestamp (format gốc: yyyy-MM-dd HH:mm:ss)
- cast DoubleType tường minh cho tất cả 9 cột số để tính toán
- Phân loại congestion_level từ traffic_congestion_index: free_flow (0-2), normal (2-4), congested (4-6), heavy (6-8), gridlock (8-10)
- Tính fuel_cost_per_km_vnd = fuel_price_vnd_liter × 0.02 (xe máy tiêu thụ 2L/100km theo chuẩn VEAA)
- Tính market_pressure_index = freight_cost_index×0.4 + traffic_congestion_index×0.4 + road_incidents×0.2
- Tính congestion_fee_multiplier: free_flow/normal=1.0, congested=1.1, heavy=1.3, gridlock=1.5
- Tính rain_fee_multiplier: rain_flag=1 → 1.2, rain_flag=0 → 1.0
- Tính adjusted_delivery_fee_vnd = delivery_fee_avg_vnd × congestion_fee_multiplier × rain_fee_multiplier
- Tạo hour_timestamp = round(unix_timestamp(timestamp)/3600)\*3600 làm Temporal Join Key
- Kết quả: 17.544 dòng → 17.544 dòng (không xóa dòng nào)

**Output:** Ghi Parquet vào /uds/data/processed/market_clean

**join_income_route_spark.py**

**JOIN - 4 nguồn theo hour_timestamp:**

- Đọc 4 file Parquet từ /uds/data/processed/
- Left Join orders × weather theo hour_timestamp
- Left Join + market theo hour_timestamp
- Left Join + flood theo hour_timestamp
- fillna(0) cho các cột flood ở những giờ không có ngập
- Drop cột timestamp trùng lặp từ weather và market sau join
- Kết quả: 2.290 dòng (giữ toàn bộ đơn hàng)

**Feature Engineering - Thu nhập tài xế:**

- actual_duration_min = (unix_timestamp(deliveredAt) - unix_timestamp(createdAt)) / 60 - thời gian giao thực tế (phút)
- expected_duration_min = (unix_timestamp(expectedDeliveryTime) - unix_timestamp(createdAt)) / 60 - thời gian giao kỳ vọng (phút)
- delay_min = actual_duration_min - expected_duration_min - dương=trễ, âm=sớm hơn hứa
- is_late = 1 nếu delay_min > 0, ngược lại = 0
- distance_km = shippingDistance_km làm tròn 3 chữ số, lọc bỏ đơn > 200km
- order_hour, order_dow, order_month, time_slot (sang 6-10h, trua 11-12h, chieu 13-17h, toi 18-21h, dem 22-5h)
- rain_level: no_rain, light (&lt; 2.5mm), moderate (2.5-7.5mm), heavy (&gt;= 7.5mm) theo chuẩn WMO
- is_extreme_weather = 1 nếu prcp_mm > 5 hoặc condition_label thuộc Heavy Rain, Thunderstorm, Heavy Thunderstorm, Storm
- weather_fee_multiplier: heavy=1.5, moderate=1.3, light=1.1, no_rain=1.0
- flood_fee_multiplier: severity>20→1.4, severity>10→1.2, has_flood=1→1.1, else→1.0
- congestion_fee_multiplier: gridlock=1.5, heavy=1.3, congested=1.1, else=1.0
- time_fee_multiplier: dem=1.3, sang=1.1, else=1.0
- estimated_delivery_fee_vnd = delivery_fee_avg_vnd × weather × flood × congestion × time multiplier
- estimated_fuel_cost_vnd = distance_km × 0.02 × fuel_price_vnd_liter
- estimated_driver_income_vnd = estimated_delivery_fee_vnd - estimated_fuel_cost_vnd
- income_per_km = estimated_driver_income_vnd / distance_km
- income_per_min = estimated_driver_income_vnd / actual_duration_min

**Feature Engineering - Quãng đường tối ưu:**

- route_difficulty_score = avg_flood_severity_score×0.4 + traffic_congestion_index×0.4 + prcp_mm×0.2
- route_difficulty_level: easy (&lt; 2.0), moderate (2.0-5.0), hard (5.0-10.0), very_hard (&gt;= 10.0)
- estimated_actual_speed_kmh = avg_vehicle_speed_kmh × flood_penalty(0.6 nếu có ngập) × rain_penalty(0.7 nếu mưa nặng)
- effective_distance_km = distance_km × (1 + route_difficulty_score / 10)
- route_optimization_score = (income_per_km/1000) × difficulty_factor × lateness_factor - easy→1.3, moderate→1.0, hard→0.7, very_hard→0.4; đúng hẹn→1.1, trễ→0.9

**Output:** Ghi Parquet phân vùng theo order_month vào /uds/data/joined/income_route_features

**Số liệu chốt:**

- Tổng đơn hàng hợp lệ sau toàn bộ pipeline: **2.290 dòng**
- Đơn trễ (is_late=1): **463 đơn (20.2%)**
- Thu nhập tài xế theo ca: dem cao nhất (điểm tối ưu 25.68, income/km 18.928 VND), chieu thấp nhất (điểm tối ưu 7.17, income/km 6.159 VND)
- Quãng đường tối ưu: easy route có tốc độ cao nhất (28.61 km/h), very_hard có income/km cao nhất (55.950 VND) nhưng điểm tối ưu thấp hơn do penalty độ khó
- Khoảng cách hiệu quả nhất: **0-3km** (income/km = 7.333 VND, điểm tối ưu 8.74)
- Tỉ lệ trễ theo rain_level: heavy=4.3%, moderate=15%, no_rain=18.8%, light=24.7%
- Tỉ lệ trễ theo condition_label: Heavy Rain=47.5%, Rain=31.4%, Light Rain=27.5%, Clear=17.6%