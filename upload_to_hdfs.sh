#!/bin/bash

# Upload weather data to HDFS
SOURCE_FILE="/Users/doanquochuy/hadoop-ueh/data/raw/hcmc_weather_uds_optimized.csv"
HDFS_PATH="/user/uds_project/data/raw/"

echo "Starting HDFS upload..."
echo "Source: $SOURCE_FILE"
echo "Destination: hdfs://$HDFS_PATH"

# Check if source file exists
if [ ! -f "$SOURCE_FILE" ]; then
    echo "ERROR: Source file not found: $SOURCE_FILE"
    exit 1
fi

# Create HDFS directory if it doesn't exist
hdfs dfs -mkdir -p "$HDFS_PATH" 2>/dev/null

# Upload file to HDFS
hdfs dfs -put -f "$SOURCE_FILE" "$HDFS_PATH"

# Check if upload was successful
if [ $? -eq 0 ]; then
    echo "✓ Upload successful!"
    echo "Verifying file in HDFS..."
    hdfs dfs -ls "$HDFS_PATH"
else
    echo "✗ Upload failed!"
    exit 1
fi
