# ⚾ Blue Jays Moneyball: Financial Forecasting & Integrity System

![Python](https://img.shields.io/badge/Python-3.9%2B-blue)
![Status](https://img.shields.io/badge/Phase--1-Complete-green)
![Tests](https://img.shields.io/badge/Tests-Passing-brightgreen)
![Code Quality](https://img.shields.io/badge/Code%20Quality-A%2B-success)

## 📌 Project Overview
This project applies **Data Engineering** and **Financial Analysis** to forecast the Toronto Blue Jays' financial future. Unlike simple statistics aggregators, this system focuses on **Data Integrity (SDET principles)** to ensure that data fed into Luxury Tax simulations is accurate, validated, and resilient against system failures.

**Goal:** Predict future Arbitration salaries and simulate Competitive Balance Tax (CBT) implications using player performance metrics (WAR, Age, Service Time) and historical data.

---

## 👨‍💻 About the Author
**Chris (Suk Min) Yoon** *Senior SDET / QA Automation Engineer (10+ Years Experience)* Specializing in data quality assurance, ETL validation, and backend testing across insurance and financial domains.

* **Expertise:** Python (Pandas, SQLAlchemy, PyTest), SQL, Apache Airflow, and Docker.
* **Background:** Proven track record in building automation frameworks at **theScore (ESPN Bet)** and leading QA strategies for enterprise clients like **Avesis** and **Jewelers Mutual**.

---

## 🚀 Key Features (Phase 1: MVP)

### 1. Robust Data Warehouse & ETL Pipeline
* **Star Schema Architecture:** Implements a production-grade PostgreSQL data warehouse with `dim_players` and `fact_contracts`.
* **Orchestrated Sync:** Fully automated nightly synchronization using **Apache Airflow** to keep the 40-man roster and financial facts updated.
* **Dockerized Infrastructure:** The entire stack (Postgres, Airflow Webserver, Scheduler, Workers) is containerized for seamless deployment.

### 2. Automated Data QA (SDET Layer)
* **Strict Linting & Compliance:** Adheres to PEP 8 standards with a zero-warning policy using **Flake8** and **Pylance**.
* **Data Integrity Tests:** Integrated **Pytest** suite to validate schema consistency, foreign key relationships, and data accuracy across the ETL lifecycle.

### 3. Visual Analytics
* **Efficiency Analysis:** Automatically generates "WAR vs. Salary" visualizations to identify high-value players and financial outliers.

---

## 🛠 Tech Stack
* **Language:** Python 3.12+
* **Orchestration:** Apache Airflow
* **Database:** PostgreSQL (Star Schema)
* **Infrastrucure:** Docker & Docker Compose
* **Data Processing:** Pandas, SQLAlchemy
* **Quality Assurance:** Pytest, Flake8, Pandera

---

## 📂 Project Structure
```text
bluejays-financial-mlops/
├── dags/                # Airflow Orchestration (DAGs)
├── docker-compose.yaml  # Full stack container configuration
├── plots/               # Generated visualizations (WAR vs Salary)
├── src/
│   ├── models.py        # SQLAlchemy Schema Definitions (Data Warehouse)
│   ├── etl_bluejays.py  # Roster Sync & Extraction Logic
│   ├── init_db.py       # Database Initialization & Migration
│   └── load_salaries.py # Financial Fact Data Loading
├── tests/
│   └── test_data_loader.py # Unit & Data Integrity Tests
└── requirements.txt     # Python Dependencies

```

---

## ⚙️ How to Run

### 1. Launch the Infrastructure

Ensure Docker is running and execute:

```bash
docker-compose up -d

```

### 2. Run Data Integrity Tests

Verify that the database initialization and data loaders are meeting quality standards:

```bash
pytest tests/

```

**Expected Output:** `Passed, 0 warnings` (Zero tolerance for linting/logic errors).

### 3. Trigger the ETL Pipeline

Access the Airflow UI at `http://localhost:8080` and trigger the `bluejays_data_warehouse_v1` DAG to populate the PostgreSQL warehouse.

---

## 🗺 Roadmap

| Phase | Focus | Status |
| --- | --- | --- |
| **Phase 1** | **MVP & Data Integrity Layer** (Postgres, ETL, Airflow, Docker) | ✅ Completed |
| **Phase 2** | **"What-If" Simulation Engine** (Signings, Trades, & 2026 CBT Forecasts) | 🚧 In Progress |
| **Phase 3** | **Cloud MLOps** (GCP integration & Salary Prediction Models) | 📅 Planned |

---
