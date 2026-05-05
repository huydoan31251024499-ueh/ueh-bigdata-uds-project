# HƯỚNG DẪN CÂU LỆNH HAY DÙNG

Tài liệu này tổng hợp toàn bộ các câu lệnh cần thiết để vận hành hệ thống Big Data của dự án UDS trên môi trường Windows/VS Code. 

---

## 1. QUẢN LÝ HỆ THỐNG (DOCKER LIFECYCLE)
Sử dụng các lệnh này để "bật" hoặc "tắt" cụm máy chủ Hadoop/Spark.

| Lệnh | Ý nghĩa |
| :--- | :--- |
| `docker-compose up -d` | **Khởi động:** Chạy toàn bộ cụm server ở chế độ nền (không chiếm terminal). |
| `docker-compose ps` | **Kiểm tra:** Xem các máy ảo (Namenode, Spark...) đã "Up" chưa. |
| `docker-compose logs -f` | **Xem Log:** Theo dõi quá trình vận hành để phát hiện lỗi. |
| `docker-compose down` | **Tắt máy:** Dừng và xóa các container để giải phóng RAM cho máy. |

> **Note:** Luôn chạy `docker-compose up -d` ngay khi bắt đầu làm bài và `down` khi đã làm xong để máy không bị lag.

---

## 2. THAO TÁC DỮ LIỆU TRÊN HDFS (HADOOP STORAGE)
Dữ liệu thô phải được đẩy vào "kho" HDFS thì Spark để xử lý được.

* **Tạo thư mục làm việc trên Hadoop:**
    ```bash
    docker exec -it namenode hdfs dfs -mkdir -p /user/uds/data
    ```
* **Đẩy file từ máy thật (thư mục data/raw) vào Hadoop:**
    ```bash
    docker exec -it namenode hdfs dfs -put /app/data/raw/uds_orders.csv /user/uds/data/
    ```
* **Kiểm tra xem dữ liệu đã vào kho chưa:**
    ```bash
    docker exec -it namenode hdfs dfs -ls /user/uds/data
    ```

---

## 3. THỰC THI SPARK JOB (XỬ LÝ DỮ LIỆU)
Sử dụng `spark-submit` để gửi file Python của bạn vào cụm Spark Cluster để tính toán Big Data.

* **Chạy xử lý dữ liệu Thời tiết & Đơn hàng:**
    ```bash
    docker exec spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077 /app/src/process_weather_spark.py
    ```
* **Chạy file chuẩn bị dữ liệu (Nếu cần):**
    ```bash
    docker exec spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077 /app/src/prepare_get_weather.py
    ```

---

## 4. QUY TRÌNH LÀM VIỆC NHÓM (GIT WORKFLOW)
Để tránh bị ghi đè code và mất dữ liệu, hãy tuân thủ quy trình:

1.  **Cập nhật code mới nhất từ nhóm trưởng (Huy):**
    ```bash
    git pull origin main
    ```
2.  **Lưu kết quả sau khi làm xong:**
    ```bash
    git add .
    git commit -m "Mô tả việc bạn vừa làm (Ví dụ: Update slide, fix bug src/main.py)"
    ```
3.  **Gửi code lên GitHub:**
    ```bash
    git push origin main
    ```
