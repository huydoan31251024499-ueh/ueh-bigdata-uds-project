# Project “Xe Dù” (UDS):

## Big Data Analytics for Urban Last‑Mile Delivery in Ho Chi Minh City

This project applies **Big Data analytics and distributed data processing** to analyze how **weather, urban flooding, and traffic conditions** affect last‑mile delivery performance for the “Xe Dù” (Umbrella Delivery Service – UDS) startup in **Ho Chi Minh City** during the **2023–2024 rainy seasons**.

The goal is not only to analyze delays, but to **understand the operational, economic, and spatial mechanisms** behind delivery inefficiency and to propose **data‑driven system designs** for real‑world deployment.

***

## 1. Business Problem

Last‑mile delivery in Ho Chi Minh City is highly vulnerable to **urban flooding and adverse weather**. During the rainy season, delivery services face:

*   Increased delivery time and ETA uncertainty
*   Reduced driver income efficiency
*   Higher order cancellation risk
*   Operational instability caused by localized infrastructure disruption

Traditional logistics systems often assume **shortest‑distance routing** and **static pricing**, which fail to capture the **spatial and temporal complexity** of urban flooding.

This project addresses the following question:

> *What actually causes delivery inefficiency during the rainy season, and how can a data‑driven system respond to it?*

***

## 2. Data Ecosystem and Big Data Characteristics

The project integrates multiple urban data sources into a unified analytical pipeline.

### Data Sources

*   **Orders (Structured):** UDS delivery records (timestamps, distances, service types, durations)
*   **Weather (Semi‑structured):** Hourly precipitation and weather conditions (OpenWeather API)
*   **Flood Data (Spatial):** 159 flood‑prone locations in HCMC with severity indicators
*   **Traffic / Market Indicators:** Congestion index and delivery supply metrics

### Big Data Characteristics (5Vs)

*   **Volume:** Designed to scale to city‑wide delivery logs
*   **Velocity:** Structured for batch processing with real‑time extension
*   **Variety:** CSV, API‑derived weather data, spatial flood data
*   **Veracity:** Official meteorological and infrastructure sources
*   **Value:** Direct operational insight for urban logistics under stress

***

## 3. Technical Architecture

The system is built as a **distributed data pipeline**, optimized for reproducibility and scalability.

### Core Technologies

*   **Infrastructure:** Docker Compose (local cluster orchestration)
*   **Storage:** Hadoop Distributed File System (HDFS) – distributed data lake
*   **Processing & Analytics:** Apache Spark (PySpark, Spark SQL)
*   **Machine Learning:** Spark MLlib (Linear Regression for ETA prediction)
*   **Visualization:** Python (Matplotlib/Seaborn) and Looker Studio (presentation layer)

### Future Extensions (ACT Phase)

*   **Graph Processing:** Spark GraphX (flood‑aware routing)
*   **Streaming:** Kafka + Spark Streaming (real‑time ETA & routing updates)

***

## 4. Project Methodology

The project follows the **Google Data Analytics framework**:

    Ask → Prepare → Process → Analyze → Share → Act

### Ask

Define SMART questions focusing on:

*   **Q1 – Time:** ETA prediction and delay risk
*   **Q2 – Money:** Economic impact on driver income
*   **Q3 – Space:** Spatial inefficiency caused by flooding

### Prepare

*   Collect multi‑source urban data
*   Store raw data in HDFS
*   Validate data completeness and ethics

### Process

*   Clean and standardize data using PySpark
*   Normalize units (mm, minutes, meters)
*   Align all datasets using a **temporal joining key (`hour_timestamp`)**
*   Perform distributed joins across orders, weather, flood, and traffic data
*   Output unified dataset: `final_features.parquet`

### Analyze

*   **Q1:** Context‑aware ETA regression (Spark MLlib)
*   **Q2:** Economic analysis of income per minute (Spark SQL)
*   **Q3:** Spatial analysis of distance, time, and route efficiency (Spark SQL)

### Share

*   Export analytical results to CSV
*   Visualize insights using charts and dashboards
*   Focus on clear communication rather than raw computation

### Act

*   Propose system designs for:
    *   Context‑aware ETA dashboards
    *   Income stabilization mechanisms
    *   Flood‑aware routing (future work)

***

## 5. Key Analytical Insights

### Q1 – ETA and Delay Risk

*   Flood conditions create a **distinct operational regime** different from rain
*   Context segmentation reduces ETA error significantly
*   Final ETA model achieves **RMSE ≈ 38–40 minutes** in stable conditions

### Q2 – Economic Impact

*   Driver income efficiency drops by **\~30% in flood‑only conditions**
*   Income loss is driven by **time inefficiency**, not fee reduction
*   Flood‑only scenarios represent a critical economic blind spot

### Q3 – Spatial Inefficiency

*   Flooding **does not significantly increase nominal distance**
*   Flooding **increases delivery time and reduces efficiency**
*   Route efficiency drops from \~88 to \~62 in flood‑only contexts
*   Flood impact is **non‑linear and spatially dependent**

> **Key takeaway:**  
> *Urban delivery inefficiency during the rainy season is a spatial problem, not a distance problem.*

***

## 6. Project Limitations

*   The project addresses **multiple analytical dimensions simultaneously** (time, money, space), limiting depth in each component
*   Dataset size is moderate and lacks:
    *   GPS‑level route traces
    *   Turn‑by‑turn navigation data
*   ML models perform well in stable contexts but struggle under flood conditions
*   Proposed solutions are **not yet validated with real end‑users**

***

## 7. Future Development

Planned extensions focus on turning analysis into a deployable system:

*   **Real‑time pipeline:** Kafka + Spark Streaming for live ETA updates
*   **Flood‑aware routing:** Graph‑based pathfinding using Spark GraphX
*   **Design Thinking validation:**
    *   Low‑ and mid‑fidelity prototypes
    *   Driver and customer interviews
    *   Feedback‑driven iteration
*   **Towards MVP:**
    *   Integrate ETA, pricing, and routing into a unified system

***

## 8. Repository Structure

    data/
      ├── raw/
      ├── processed/
      └── analysis/
    src/
      ├── 01_ASK/
      ├── 02_PREPARE/
      ├── 03_PROCESS/
      ├── 04_ANALYZE/
      ├── 05_SHARE/
      └── 06_ACT/
    infrastructure/
    docs/

## 9. Setup Instructions
To maintain a consistent environment, run the provided Docker Compose configuration:

```bash
# Clone the repository
git clone https://github.com/huydoan31251024499-ueh/uds-bigdata-project

# Spin up Spark/HDFS Cluster
docker-compose up -d

# Install required python libraries
pip install -r requirements.txt
```

***

## 10. Conclusion

This project demonstrates how **distributed data processing and analytics** can uncover the true causes of inefficiency in urban last‑mile delivery. Rather than focusing solely on prediction accuracy or routing distance, the study highlights **spatial disruption as the core challenge** of rainy‑season logistics in Ho Chi Minh City.

> *This work provides a data‑driven foundation for building resilient, context‑aware urban delivery systems.*

***

**Authors:** Huy Doan et al.  
**Course:** Big Data and Applications – UEH University