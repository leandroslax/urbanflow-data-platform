
# UrbanFlow Data Platform – Streaming Data Lake Architecture

## 1. Overview

The **UrbanFlow Data Platform** is a **streaming-first data architecture** designed to ingest, process and analyze **urban mobility events in near real-time**.

The platform simulates events such as:

- Trips
- GPS positions
- Traffic congestion
- Incidents
- Weather conditions

The architecture follows the **Medallion Architecture pattern (Bronze → Silver → Gold)** implemented on **Amazon S3 Data Lake**.

Data processing is performed using **Apache Spark Structured Streaming**, while analytical modeling is handled by **dbt on Snowflake**.

The final analytics layer feeds **Amazon QuickSight dashboards** for mobility insights.

---

# 2. End-to-End High-Level Architecture

```
UrbanFlow Producer (Python Simulator)
            │
            ▼
      Amazon MSK (Kafka)
            │
            ▼
   Spark Structured Streaming
            │
            ▼
      S3 Data Lake (Bronze)
            │
            ▼
      S3 Data Lake (Silver)
            │
            ▼
       S3 Data Lake (Gold)
            │
            ▼
         Snowflake DW
            │
            ▼
            dbt
            │
            ▼
      Amazon QuickSight
```

---

# 3. Storage Architecture

## 3.1 Primary Data Lake Bucket

```
s3://urbanflow-datalake-dev-us-east-1-139961319000/urbanflow
```

This bucket stores the complete **UrbanFlow Data Lake**, including all Medallion layers.

---

## 3.2 Bronze Layer (Raw Streaming Events)

Purpose:

- Store events exactly as produced
- Preserve full event history
- Allow replay or reprocessing
- Maintain raw audit trail

Structure:

```
urbanflow/bronze/

bronze/
 ├─ viagens/
 ├─ gps/
 ├─ incidentes/
 ├─ clima/
 └─ trafego/
```

Characteristics:

- Append-only ingestion
- Minimal transformations
- Flexible schema
- Direct streaming ingestion from Kafka

---

## 3.3 Silver Layer (Clean / Curated Data)

Purpose:

- Standardize schemas
- Apply data quality rules
- Normalize timestamps
- Deduplicate events
- Prepare datasets for analytics

Structure:

```
urbanflow/silver/

silver/
 ├─ viagens/
 ├─ gps/
 ├─ incidentes/
 ├─ clima/
 └─ trafego/
```

Typical transformations:

- Timestamp normalization
- Data type casting
- Deduplication by primary keys
- Region and city standardization
- Data enrichment

---

## 3.4 Gold Layer (Analytics Data)

The **Gold Layer** stores curated and aggregated datasets optimized for analytics.

Structure:

```
urbanflow/gold/

gold/
 ├─ congestionamento_por_hora/
 ├─ incidentes_por_regiao/
 └─ tempo_medio_viagem/
```

Characteristics:

- Aggregated metrics
- Business-ready datasets
- Reduced data volume
- Optimized for BI consumption

---

# 4. Processing Layer

## Apache Spark Structured Streaming

Responsibilities:

- Kafka event ingestion
- Bronze layer persistence
- Silver layer transformations
- Gold layer aggregations

Advantages:

- Native streaming support
- Scalable distributed processing
- Compatible with Parquet and S3
- Near real-time processing

---

# 5. Orchestration

## Apache Airflow

Airflow orchestrates the complete pipeline.

Pipeline flow:

1. Start Silver transformation jobs
2. Validate Silver outputs
3. Execute Gold aggregations
4. Trigger dbt transformations
5. Validate analytics models

Benefits:

- Workflow orchestration
- Retry and failure handling
- Scheduling and monitoring
- Dependency management

---

# 6. Analytics Layer

## Snowflake Data Warehouse

Snowflake provides the **analytical serving layer** of the platform.

Responsibilities:

- Load curated datasets from S3
- Serve analytics queries
- Support dbt data modeling
- Provide scalable compute for BI

Typical datasets:

```
urbanflow_analytics
 ├─ mobility_metrics
 ├─ congestion_metrics
 ├─ incident_metrics
 └─ trip_metrics
```

---

# 7. Data Transformation Layer

## dbt (Data Build Tool)

dbt is responsible for:

- Data modeling
- Data transformations
- Analytical datasets creation
- Data testing

Model layers:

```
dbt/models

staging/
intermediate/
marts/
```

Examples:

```
stg_viagens
stg_gps
mart_mobilidade_diaria
mart_congestionamento
```

---

# 8. Visualization Layer

## Amazon QuickSight

QuickSight consumes datasets from **Snowflake** to build dashboards.

Example dashboards:

- Urban Mobility Overview
- Traffic Congestion Analysis
- Incident Monitoring
- Trip Volume Trends
- Weather Impact on Traffic

---

# 9. Architectural Principles

The UrbanFlow platform follows modern data engineering principles:

- Streaming-first ingestion
- Medallion Architecture
- Decoupled storage and compute
- Cloud-native architecture
- Scalable event-driven design
- Data Lake + Data Warehouse separation

---

# 10. Future Improvements

Possible platform evolutions:

- Apache Iceberg Lakehouse tables
- Data quality validation framework
- Data observability tools
- Real-time dashboards
- CI/CD pipelines for data jobs
- Data contracts
- Metadata catalog and governance
