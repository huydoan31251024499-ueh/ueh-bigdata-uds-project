# Project **“Xe Dù” (UDS)**

## Big Data Analytics for Urban Last‑Mile Delivery in Ho Chi Minh City

This project applies **Big Data analytics and distributed data processing** to analyze how **weather, urban flooding, and traffic conditions** affect last‑mile delivery performance for the *Xe Dù (UDS)* startup in **Ho Chi Minh City** during the **2023–2024 rainy seasons**.

The objective is not only to analyze delivery delays, but to **understand the operational, economic, and spatial mechanisms** behind delivery inefficiency and to propose **data‑driven system designs** for real‑world deployment.

***

## 1. Business Problem

Urban last‑mile delivery in Ho Chi Minh City is highly vulnerable to **localized flooding and adverse weather**, leading to multiple operational issues.

| Issue                     | Impact                   |
| ------------------------- | ------------------------ |
| Delivery time increase    | ETA uncertainty          |
| Driver income reduction   | Supply instability       |
| Higher cancellation risk  | Customer dissatisfaction |
| Infrastructure disruption | System inefficiency      |

Traditional logistics systems rely on **shortest‑distance routing** and **static pricing**, which fail to capture the **spatial and temporal complexity** of urban flooding.

> **Research question:**  
> *What causes delivery inefficiency during the rainy season, and how can a data‑driven system respond?*

***

## 2. Data Ecosystem & Big Data Characteristics

### 2.1 Data Sources

| Data Type        | Description                                                        |
| ---------------- | ------------------------------------------------------------------ |
| Orders           | Delivery records (timestamps, distances, service types, durations) |
| Weather          | Hourly precipitation and weather conditions (OpenWeather API)      |
| Flood            | 159 flood‑prone locations in HCMC with severity indicators         |
| Traffic / Market | Congestion index and delivery supply metrics                       |

***

### 2.2 Big Data Characteristics (5Vs)

| V        | Description                                       |
| -------- | ------------------------------------------------- |
| Volume   | Designed to scale to city‑wide delivery logs      |
| Velocity | Batch processing with real‑time extension         |
| Variety  | CSV, API‑derived weather data, spatial flood data |
| Veracity | Official meteorological & infrastructure sources  |
| Value    | Direct operational insights for urban logistics   |

***

## 3. Technical Architecture

The system is implemented as a **distributed data pipeline**, optimized for reproducibility and scalability.

### 3.1 Core Technology Stack

| Layer            | Technology                              |
| ---------------- | --------------------------------------- |
| Infrastructure   | Docker Compose (cluster orchestration)  |
| Storage          | Hadoop HDFS (distributed data lake)     |
| Processing       | Apache Spark (PySpark, Spark SQL)       |
| Machine Learning | Spark MLlib (Linear Regression for ETA) |
| Visualization    | Matplotlib / Seaborn, Looker Studio     |

***

### 3.2 Future Extensions (ACT Phase)

| Capability           | Technology                         |
| -------------------- | ---------------------------------- |
| Graph‑based routing  | Spark GraphX (flood‑aware routing) |
| Real‑time processing | Kafka + Spark Streaming            |

***

## 4. Methodology (Google Data Analytics Framework)

    Ask → Prepare → Process → Analyze → Share → Act

| Phase   | Key Activities                                 |
| ------- | ---------------------------------------------- |
| Ask     | Define SMART questions (Q1–Q2–Q3)              |
| Prepare | Collect multi‑source urban data, store in HDFS |
| Process | PySpark cleaning, normalization, temporal join |
| Analyze | Distributed analytics (Spark MLlib, Spark SQL) |
| Share   | Charts & dashboards for insights               |
| Act     | System design proposals (future work)          |

**SMART Questions**

| Question | Dimension                | Method                 |
| -------- | ------------------------ | ---------------------- |
| Q1       | Time (ETA & delay risk)  | Spark MLlib regression |
| Q2       | Money (driver income)    | Spark SQL              |
| Q3       | Space (route efficiency) | Spark SQL              |

***

## 5. Key Analytical Insights

### Q1 – ETA & Delay Risk

*   Flood creates a **distinct operational regime** compared to rain
*   Context‑aware segmentation significantly reduces ETA error
*   Final ETA model achieves **RMSE ≈ 38–40 minutes** in stable contexts

### Q2 – Economic Impact

*   Driver income efficiency drops **\~30% in flood‑only conditions**
*   Income loss is driven by **time inefficiency**, not fee reduction
*   Flood‑only scenarios represent a critical economic blind spot

### Q3 – Spatial Inefficiency

*   Flooding **does not significantly increase nominal distance**
*   Flooding **increases delivery time and reduces efficiency**
*   Route efficiency drops from **\~88 → \~62** in flood‑only contexts
*   Flood impact is **non‑linear and spatially dependent**

> **Key takeaway:**  
> *Urban delivery inefficiency during the rainy season is a spatial problem, not a distance problem.*

***

## 6. Project Limitations

| Limitation       | Description                                                |
| ---------------- | ---------------------------------------------------------- |
| Broad scope      | Addresses time, money, and space simultaneously            |
| Data granularity | No GPS‑level route traces or turn‑by‑turn data             |
| Model limits     | ML performs well in stable contexts, struggles under flood |
| Validation       | No real‑world end‑user testing yet                         |

***

## 7. Future Development

| Direction           | Description                                               |
| ------------------- | --------------------------------------------------------- |
| Real‑time pipeline  | Kafka + Spark Streaming                                   |
| Flood‑aware routing | Graph‑based routing with Spark GraphX                     |
| Design Thinking     | Low‑/mid‑fidelity prototypes & user interviews            |
| Towards MVP         | Integrate ETA, pricing, and routing into a unified system |

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

***

## 9. Setup Instructions

```bash
# Clone repository
git clone https://github.com/huydoan31251024499-ueh/uds-bigdata-project

# Start Spark & HDFS cluster
docker-compose up -d

# Install dependencies
pip install -r requirements.txt
```

***

## 10. Conclusion

This project demonstrates how **distributed data processing and analytics** can uncover the true causes of inefficiency in urban last‑mile delivery. Rather than focusing solely on prediction accuracy or routing distance, the analysis highlights **spatial disruption as the core challenge** of rainy‑season logistics in Ho Chi Minh City.

> *This work provides a data‑driven foundation for building resilient, context‑aware urban delivery systems.*

***

**Authors:** Huy Đoàn et al.  
**Course:** Big Data and Applications – UEH University