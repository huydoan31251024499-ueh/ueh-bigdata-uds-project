import pandas as pd
from datetime import datetime
from meteostat import hourly
import ssl

# 1. Bypass lỗi SSL trên macOS
ssl._create_default_https_context = ssl._create_unverified_context

# 2. Cấu hình thông số
STATION = '48900' # Tân Sơn Nhất
START = datetime(2023, 1, 1)
END = datetime(2024, 12, 31, 23, 59)
OUTPUT = 'hcmc_weather_2023_2024.csv'

def main():    
    # 3. Fetch dữ liệu từ Meteostat
    try:
        data = hourly(STATION, START, END).fetch()
        
        if data.empty:
            print("Không có dữ liệu!")
            return

        # 4. Định nghĩa các cột cần thiết
        # temp: nhiệt độ, rhum: độ ẩm, prcp: lượng mưa, wspd: tốc độ gió, coco: mã thời tiết
        cols = ['temp', 'rhum', 'prcp', 'wspd', 'pres', 'coco']
        df = data[cols].copy()

        # 5. Xử lý dữ liệu: Đưa Timestamp từ Index ra thành một cột riêng
        df.reset_index(inplace=True)
        df.rename(columns={'time': 'timestamp'}, inplace=True)

        # 6. Fill các giá trị Null trong cột lượng mưa bằng 0 (giả định không mưa nếu không có dữ liệu)
        df['prcp'] = df['prcp'].fillna(0)

        # 7. Xuất file CSV
        df.to_csv(OUTPUT, index=False)
        
        print("=" * 60)
        print(f"Đã lưu {len(df)} dòng vào: {OUTPUT}")
        print(f"Các cột hiện có: {list(df.columns)}")
        print("=" * 60)

    except Exception as e:
        print(f"Lỗi: {e}")

if __name__ == "__main__":
    main()