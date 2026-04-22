#!/usr/bin/env python3
"""
Script to fetch hourly historical weather data for Tan Son Nhut station (ID: 48900)
using the meteostat library.

Time range: 2023-01-01 to 2024-12-31
Columns: temperature, precipitation, wind speed, weather condition code
Output: hcmc_weather_hourly.csv with timestamp as first column
"""

import pandas as pd
from datetime import datetime
from meteostat import hourly
import ssl
import meteostat as ms


# Disable SSL verification (temporary fix for certificate issues)
ssl._create_default_https_context = ssl._create_unverified_context

def main(): 
    # Define the station ID for Ho Chi Minh City (Tan Son Nhut Airport)
    station_id = '48900'
    station = ms.stations.meta(f'{station_id}')
    print(station)

    # Define the time range
    start_date = datetime(2023, 1, 1)
    end_date = datetime(2024, 12, 31)

    print(f"Fetching hourly weather data for station {station_id} from {start_date.date()} to {end_date.date()}...")

    # Fetch hourly data
    data = hourly(station_id, start_date, end_date)
    data = data.fetch()

    if data.empty:
        print("No data found for the specified station and time range.")
        return

    # Select required columns
    # meteostat columns: temp (temperature), prcp (precipitation), wspd (wind speed), wpgt (wind peak gust), pres (pressure), etc.
    # Weather condition code is typically 'coco' (condition code)
    columns_to_keep = ['temp', 'prcp', 'wspd', 'coco']

    # Check if all required columns exist
    available_columns = [col for col in columns_to_keep if col in data.columns]
    if len(available_columns) != len(columns_to_keep):
        missing = [col for col in columns_to_keep if col not in data.columns]
        print(f"Warning: Missing columns: {missing}")
        print(f"Available columns: {list(data.columns)}")

    # Select available columns
    selected_data = data[available_columns].copy()

    # Rename columns for clarity
    column_names = {
        'temp': 'temperature',
        'prcp': 'precipitation',
        'wspd': 'wind_speed',
        'coco': 'weather_condition_code'
    }

    # Only rename columns that exist
    rename_dict = {k: v for k, v in column_names.items() if k in selected_data.columns}
    selected_data = selected_data.rename(columns=rename_dict)

    print(f"Data shape: {selected_data.shape}")
    print(f"Date range: {selected_data.index.min()} to {selected_data.index.max()}")
    print(f"Columns: {list(selected_data.columns)}")

    # Export to CSV with timestamp as first column
    output_file = 'hcmc_weather_hourly.csv'
    selected_data.to_csv(output_file, index=True)

    print(f"Data exported to {output_file}")
    print(f"Total records: {len(selected_data)}")

if __name__ == "__main__":
    main()