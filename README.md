# 🚦 Traffic Analytics Data Platform (Delta Live Tables)

## 📌 Project Overview
This project implements a **production-grade data engineering pipeline** using **Databricks Delta Live Tables (DLT)** to ingest, validate, transform, and model large-scale **road and traffic datasets** into an analytics-ready **Bronze → Silver → Gold architecture**.

The goal is to demonstrate how to design **reliable, auditable, and scalable streaming data pipelines** that are resilient to schema drift, late-arriving data, and real-world data quality issues.

---

## 🎯 Why This Project Matters
Real-world data pipelines rarely deal with “clean” data. This project intentionally addresses **enterprise-grade challenges**, including:

- Schema evolution and unexpected columns
- Duplicate and replayed data
- Inconsistent timestamp semantics
- Slowly changing dimensions (SCD Type 2)
- Data quality enforcement with observability
- Safe dimensional modeling for BI

This makes the project representative of **actual production data engineering work**, not just toy examples.

---

## 🧱 Architecture Overview

### 🥉 Bronze – Raw, Auditable Ingestion
- Incremental file ingestion using **Auto Loader**
- Strict schema enforcement with **rescue mode**
- Full operational lineage:
  - source file path
  - file modification time
  - ingest timestamp
- Dedicated **quarantine tables** for rescued (unexpected) columns
- Ingestion **metrics tables** for monitoring corrupt and drifted data

**Purpose:**  
Preserve raw data exactly as received, while remaining fully auditable.

---

### 🥈 Silver – Cleaned, Contract-Enforced Data
- Standardized column naming and data types
- Strong **data quality contracts** (fail-fast for critical fields)
- Robust **timestamp normalization**:
  - Handles multiple input formats
  - Correctly resolves conflicts between date fields and hour fields
- Deterministic **event-level deduplication** using a stable hash key
- **SCD Type 2** implementation for road attributes with churn control

**Purpose:**  
Produce trustworthy, business-consistent datasets suitable for downstream analytics.

---

### 🥇 Gold – BI-Safe Dimensional Model
- Star schema optimized for reporting
- Deterministic surrogate keys (stable across reprocessing)
- Dimensions:
  - Region
  - Road category
  - Road name (safe alternative where no true road_id exists)
- Fact table designed to **avoid many-to-many joins and metric inflation**

**Purpose:**  
Enable accurate, performant analytics without hidden data duplication risks.

---

## 🔍 Key Engineering Decisions

### ✅ Schema Drift Handling
- Unexpected columns are captured, not dropped
- Drift visibility through metrics instead of silent failures
- Quarantine used **only for rescued data**, keeping pipelines flowing

### ✅ Idempotent & Replay-Safe Design
- Stable hash-based deduplication prevents data inflation
- Streaming pipelines tolerate reprocessing and late arrivals

### ✅ Correct Time Semantics
- Explicit logic to resolve conflicting time fields
- Prevents silent shifting of events to incorrect hours

### ✅ Production-Ready SCD2
- Change detection via row hashing
- Prevents unnecessary history churn on replays

---

## 🛠️ Technology Stack
- **Databricks Delta Live Tables (DLT)**
- **Apache Spark Structured Streaming**
- **Delta Lake**
- **Auto Loader (cloudFiles)**
- **Python / PySpark**

---

## 📊 Use Cases Enabled
- Traffic volume analysis by region, road category, and time
- Trend analysis with accurate historical road attribute changes
- BI dashboards with consistent metrics and dimensions
- Data quality monitoring and pipeline observability

---

## 🚀 What This Project Demonstrates
- End-to-end ownership of a **realistic data platform**
- Ability to reason about **data correctness**, not just transformations
- Strong understanding of **streaming vs batch trade-offs**
- Senior-level design choices that prioritize **trust and maintainability**

---

## 🧠 Ideal Audience
- Hiring managers evaluating **data engineering depth**
- Teams looking for a **reference DLT architecture**
- Engineers transitioning from batch ETL to **modern streaming pipelines**

---

## 📎 Notes
This project intentionally avoids unsafe assumptions (e.g., forcing `road_id` joins where the source data does not support them).  
All modeling decisions prioritize **data correctness over convenience**.

---

## 👤 Author
**Data Engineer**  
Specializing in scalable data pipelines, streaming systems, and analytics platforms.
