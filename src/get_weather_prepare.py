import pandas as pd
from datetime import datetime
from meteostat import hourly
import ssl

# Bypass SSL error on macOS
ssl._create_default_https_context = ssl._create_unverified_context

# Configuration
STATION = '48900'  # Tân Sơn Nhất
START = datetime(2023, 1, 1)
END = datetime(2024, 12, 31, 23, 59)
OUTPUT = '/Users/doanquochuy/bigdata-ueh/data/raw/hcmc_weather_raw.csv'

def main():
    try:
        # Fetch raw hourly data from Meteostat
        data = hourly(STATION, START, END).fetch()
        
        if data.empty:
            print("No data available!")
            return

        # Select columns
        cols = ['temp', 'rhum', 'prcp', 'wdir', 'wspd','cldc', 'pres', 'coco']
        df = data[cols].copy()

        # Convert index (timestamp) to first column
        df.reset_index(inplace=True)
        df.rename(columns={'time': 'timestamp'}, inplace=True)

        # Save raw data
        df.to_csv(OUTPUT, index=False)
        
        print(f"Raw data ingestion complete!")
        print(f"Records saved: {len(df)}")
        print(f"Output: {OUTPUT}")
        print(f"\nFirst 5 rows:")
        print(df.head())

    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    main()