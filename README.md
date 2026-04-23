# Project "Xe Dù" (UDS): Big Data Analytics for Last-Mile Delivery in HCMC

This project analyzes the impact of **weather, tidal floods, and infrastructure** on delivery performance for the "Xe Dù" (Umbrella Delivery Service) startup in Ho Chi Minh City during the 2023-2024 period.

## 1. Business Problem
Last-mile delivery efficiency in HCMC drops significantly during extreme weather events. This project builds a Big Data correlation model between operational logistics and meteorological data to optimize driver allocation, dynamic pricing, and routing to maintain high completion rates.

## 2. Data Ecosystem (The 5Vs)
The project represents a comprehensive Big Data ecosystem by integrating structured, semi-structured, and unstructured sources:

*   **Logistics (Structured):** ~2,404 successful order records including `senderLat/Lng`, `receiverLat/Lng`, item categories, etc; ~17,544 rows of historical data from `meteostat` library.
*   **Weather (Semi-structured):** Real-time updates from OpenWeatherMap API and Kaggle datasets; traffic conditions and real-time ETAs from Google APIs.
*   **Local Infrastructure (Unstructured/Crawl):** Flood points and traffic congestion logs from the HCMC Portal of Transportation and the National Center for Hydro-Meteorological Forecasting (NCHMF).

**Big Data Characteristics:**
*   **Volume:** Millions of potential logs when scaled across the HCMC transport network.
*   **Velocity:** Real-time processing of weather and traffic streams.
*   **Variety:** Integration of CSV, JSON (APIs), and web-crawled data.
*   **Veracity:** High-reliability GPS sensor data and official station metrics.
*   **Value:** Directly reduces order cancellation and compensates for urban infrastructure gaps.

## 3. Technical Architecture
The system is built on a distributed framework optimized for **Apple Silicon (Mac M3)** using **Docker** to ensure environment parity among collaborators:

*   **Storage:** Hadoop Distributed File System (HDFS) for reliable, fault-tolerant storage.
*   **Processing Engine:** **PySpark** (Spark SQL & DataFrames) for high-speed in-memory computation and distributed joins.
*   **Machine Learning:** **Spark MLlib** for regression-based delivery time prediction and **K-Means Clustering** for identifying "logistics black holes".
*   **Ingestion:** **Apache Flume** for log streams and **Sqoop** for RDBMS data transfer.

## 4. Project Roadmap
Aligned with the **Google Data Analytics Framework**, this project is structured into six phases (**Ask, Prepare, Process, Analyze, Share, Act**) executed over a **four-week** duration. The initiative commences with the formulation of **SMART** questions to define stakeholder requirements and organizational objectives, followed by an evaluation of data integrity, ethics, and infrastructure.
The implementation schedule is detailed as follows:
1.   **Data Engineering (Prepare & Process)**: Setup a **Docker** cluster and ingest **UDS** and meteorological datasets into **HDFS**. Execute data cleaning protocols, including **UTC timezone normalization** and structural validation.
2.   **Analytics Framework (Analyze)**: Perform **Spatial Joins** between delivery coordinates and flood zone mapping. Engineer features such as `is_extreme_weather` and establish correlation baselines for weather-impacted logistics.
3.   **Visualization & Storytelling (Share)**: Map delivery heatmaps with tidal charts to visualize logistical bottlenecks. Articulate the **UDS 2025 divestment** through data-driven insights and trend analysis.
4.   **Optimization & Strategic Proposal (Act)**: Fine-tune **PySpark** jobs for computational efficiency. Finalize the GitHub repository and documentation, providing actionable recommendations based on identified root causes.

## 5. Setup Instructions
To maintain a consistent environment, run the provided Docker Compose configuration:

```bash
# Clone the repository
git clone https://github.com/huydoan31251024499-ueh/uds-bigdata-project

# Spin up Spark/HDFS Cluster
docker-compose up -d

# Install required python libraries
pip install -r requirements.txt
```

## 6. Key Insights
The project highlights that while traditional retail datasets are static, UDS logistics for fresh goods (e.g., Kombucha) introduces a **"Urgency Factor."** When rainfall exceeds 10mm, delivery duration spikes by 300% in "low elevation" districts, proving that Big Data is essential for survival in HCMC's complex urban landscape.

---
**Authors:** Huy Doan et al.
**Course:** Big Data and Applications, UEH University.
