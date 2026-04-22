#!/usr/bin/env python3
"""
Script to fetch hourly historical weather data for Tan Son Nhut station (ID: 48900)
using the meteostat library.

Time range: 2023-01-01 to 2024-12-31
Columns: temp, dwpt (dew point), rhum (humidity), prcp (precipitation), 
         wspd (wind speed), pres (pressure), coco (condition code)
Output: hcmc_weather_uds_optimized.csv with timestamp as first column
"""

import pandas as pd
from datetime import datetime
from meteostat import hourly
import ssl
import urllib3
import logging
import sys

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Disable SSL certificate verification warnings and handle SSL issues
ssl._create_default_https_context = ssl._create_unverified_context
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


def setup_ssl_context():
    """Configure SSL context for secure but lenient connections."""
    try:
        import ssl
        context = ssl.create_default_context()
        context.check_hostname = False
        context.verify_mode = ssl.CERT_NONE
        return context
    except Exception as e:
        logger.warning(f"Failed to setup SSL context: {e}")
        return None


def fetch_weather_data(station_id, start_date, end_date):
    """
    Fetch hourly weather data from meteostat.
    
    Args:
        station_id (str): Station ID (e.g., '48900' for Tan Son Nhut)
        start_date (datetime): Start date
        end_date (datetime): End date
        
    Returns:
        pd.DataFrame: Weather data or None if fetch fails
    """
    try:
        logger.info(f"Fetching hourly weather data for station {station_id}")
        logger.info(f"Date range: {start_date.date()} to {end_date.date()}")
        
        # Fetch hourly data using station ID
        data = hourly(station_id, start_date, end_date).fetch()
        
        if data is None or data.empty:
            logger.warning("No data retrieved from meteostat API")
            return None
            
        logger.info(f"Successfully fetched {len(data)} records")
        return data
        
    except ssl.SSLError as e:
        logger.error(f"SSL Certificate verification failed: {e}")
        logger.info("Retrying with SSL verification disabled...")
        try:
            setup_ssl_context()
            data = hourly(station_id, start_date, end_date).fetch()
            if data is not None and not data.empty:
                logger.info(f"Successfully fetched {len(data)} records after SSL retry")
                return data
        except Exception as retry_e:
            logger.error(f"Retry failed: {retry_e}")
            return None
            
    except Exception as e:
        logger.error(f"Error fetching weather data: {type(e).__name__}: {e}")
        return None


def process_weather_data(data):
    """
    Process and select required weather columns.
    
    Args:
        data (pd.DataFrame): Raw weather data from meteostat
        
    Returns:
        pd.DataFrame: Processed data with selected columns
    """
    # Required columns from meteostat
    # Note: dwpt (dew point) may not be available with station ID approach
    required_columns = ['temp', 'dwpt', 'rhum', 'prcp', 'wspd', 'pres', 'coco']
    
    # Convert column names to strings for comparison
    data.columns = data.columns.astype(str)
    
    # Check available columns
    available_columns = [col for col in required_columns if col in data.columns]
    missing_columns = [col for col in required_columns if col not in data.columns]
    
    if missing_columns:
        logger.warning(f"Missing columns: {missing_columns}")
    
    logger.info(f"Available required columns: {available_columns}")
    logger.info(f"All columns in dataset: {list(data.columns)}")
    
    # Select available columns
    selected_data = data[available_columns].copy()
    
    # Create a new dataframe with timestamp as first column
    result_data = pd.DataFrame()
    result_data['timestamp'] = selected_data.index
    
    # Add all weather columns
    for col in available_columns:
        result_data[col] = selected_data[col].values
    
    return result_data


def main():
    """Main function to orchestrate weather data fetching and processing."""
    try:
        # Configuration
        station_id = '48900'
        start_date = datetime(2023, 1, 1)
        end_date = datetime(2024, 12, 31)
        output_file = 'hcmc_weather_uds_optimized.csv'
        
        logger.info("=" * 60)
        logger.info("WEATHER DATA FETCHING PIPELINE")
        logger.info("=" * 60)
        
        # Fetch data
        data = fetch_weather_data(station_id, start_date, end_date)
        
        if data is None:
            logger.error("Failed to fetch weather data. Exiting.")
            sys.exit(1)
        
        # Process data
        logger.info("Processing weather data...")
        processed_data = process_weather_data(data)
        
        # Data quality checks
        logger.info(f"Processed data shape: {processed_data.shape}")
        logger.info(f"Date range: {processed_data['timestamp'].min()} to {processed_data['timestamp'].max()}")
        logger.info(f"Columns: {list(processed_data.columns)}")
        logger.info(f"Missing values per column:\n{processed_data.isnull().sum()}")
        
        # Export to CSV
        logger.info(f"Exporting data to {output_file}...")
        processed_data.to_csv(output_file, index=False)
        
        logger.info(f"✓ Successfully exported {len(processed_data)} records to {output_file}")
        logger.info("=" * 60)
        
    except Exception as e:
        logger.error(f"Unexpected error in main: {type(e).__name__}: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()