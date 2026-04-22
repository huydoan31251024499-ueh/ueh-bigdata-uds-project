import pandas as pd
from datetime import datetime, timedelta
from meteostat import hourly
import ssl

# 1. Bypass lỗi SSL trên macOS
ssl._create_default_https_context = ssl._create_unverified_context

# 2. Cấu hình thông số
STATION = '48900' # Tân Sơn Nhất
START = datetime(2023, 1, 1)
END = datetime(2024, 12, 31, 23, 59)
OUTPUT = 'hcmc_weather_2023_2024.csv'

# 3. Weather Condition Codes - Meteostat Standard
# Reference: https://dev.meteostat.net/formats.html#weather-condition-codes
WEATHER_CONDITION_CODES = {
    1: 'Clear',
    2: 'Fair',
    3: 'Cloudy',
    4: 'Overcast',
    5: 'Foggy',
    6: 'Freezing Fog',
    7: 'Light Rain',
    8: 'Rain',
    9: 'Heavy Rain',
    10: 'Freezing Rain',
    11: 'Heavy Freezing Rain',
    12: 'Sleet',
    13: 'Heavy Sleet',
    14: 'Light Snowfall',
    15: 'Snowfall',
    16: 'Heavy Snowfall',
    17: 'Rain Shower',
    18: 'Heavy Rain Shower',
    19: 'Sleet Shower',
    20: 'Heavy Sleet Shower',
    21: 'Snow Shower',
    22: 'Heavy Snow Shower',
    23: 'Lightning',
    24: 'Hail',
    25: 'Thunderstorm',
    26: 'Heavy Thunderstorm',
    27: 'Storm',
}

def get_condition_label(coco):
    """Map condition code to human-readable label"""
    if pd.isna(coco):
        return 'Unknown'
    coco = int(coco)
    return WEATHER_CONDITION_CODES.get(coco, f'Unknown ({coco})')

def main():    
    # 4. Fetch dữ liệu từ Meteostat
    try:
        data = hourly(STATION, START, END).fetch()
        
        if data.empty:
            print("Không có dữ liệu!")
            return

        # 5. Định nghĩa các cột cần thiết
        # temp: nhiệt độ (°C), rhum: độ ẩm (%), prcp: lượng mưa (mm), 
        # wspd: tốc độ gió (km/h), pres: áp suất (hPa), coco: mã thời tiết
        cols = ['temp', 'rhum', 'prcp', 'wspd', 'pres', 'coco']
        df = data[cols].copy()

        # 6. Xử lý dữ liệu: Đưa Timestamp từ Index ra thành một cột riêng
        df.reset_index(inplace=True)
        df.rename(columns={'time': 'timestamp'}, inplace=True)

        # 7. Chuyển đổi timestamp sang ICT (UTC+7) - Múi giờ Việt Nam
        df['timestamp'] = pd.to_datetime(df['timestamp']) + timedelta(hours=7)

        # 8. Fill các giá trị Null trong cột lượng mưa bằng 0 (giả định không mưa nếu không có dữ liệu)
        df['prcp'] = df['prcp'].fillna(0)

        # 9. Thêm condition_label dựa trên coco codes
        df['condition_label'] = df['coco'].apply(get_condition_label)

        # 10. Rename các cột để thêm units cho rõ ràng
        # temp_c: Temperature in Celsius
        # rhum_pct: Relative Humidity in Percent
        # prcp_mm: Precipitation in Millimeters
        # wspd_kmh: Wind Speed in Kilometers per Hour
        # pres_hpa: Pressure in Hectopascals
        df = df.rename(columns={
            'temp': 'temp_c',
            'rhum': 'rhum_pct',
            'prcp': 'prcp_mm',
            'wspd': 'wspd_kmh',
            'pres': 'pres_hpa',
            'coco': 'coco_code'
        })

        # 11. Sắp xếp lại thứ tự các cột cho rõ ràng
        column_order = ['timestamp', 'temp_c', 'rhum_pct', 'prcp_mm', 'wspd_kmh', 'pres_hpa', 'coco_code', 'condition_label']
        df = df[column_order]

        # 12. Xuất file CSV
        df.to_csv(OUTPUT, index=False)
        
        print("=" * 70)
        print(f"Đã lưu {len(df)} dòng vào: {OUTPUT}\n")
  
        print(f"Mẫu dữ liệu (5 dòng đầu tiên):")
        print(df.head().to_string(index=False))
        
        print(f"\nThống kê dữ liệu:")
        print(f"\tThời gian: {df['timestamp'].min()} đến {df['timestamp'].max()}")
        print(f"\tNhiệt độ: {df['temp_c'].min():.1f}°C - {df['temp_c'].max():.1f}°C")
        print(f"\tĐộ ẩm: {df['rhum_pct'].min()}% - {df['rhum_pct'].max()}%")
        print(f"\tTốc độ gió: {df['wspd_kmh'].min():.1f} - {df['wspd_kmh'].max():.1f} km/h")
        print(f"\tÁp suất: {df['pres_hpa'].min():.1f} - {df['pres_hpa'].max():.1f} hPa")
        print(f"\tLượng mưa tổng cộng: {df['prcp_mm'].sum():.1f} mm")
        print("=" * 70)

    except Exception as e:
        print(f"Lỗi: {e}")

if __name__ == "__main__":
    main()