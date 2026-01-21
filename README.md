# ⚾ Blue Jays Moneyball: Financial Forecasting & Integrity System

![Python](https://img.shields.io/badge/Python-3.12%2B-blue)
![Status](https://img.shields.io/badge/Phase--1-Enhanced-green)
![Tests](https://img.shields.io/badge/Tests-Passing-brightgreen)
![Code Quality](https://img.shields.io/badge/Code%20Quality-A%2B-success)

## 📌 Project Overview
This project applies **Data Engineering** and **Financial Analysis** to forecast the Toronto Blue Jays' financial future. Unlike simple statistics aggregators, this system focuses on **Data Integrity (SDET principles)** to ensure that data fed into Luxury Tax simulations is accurate, validated, and resilient against system failures.

**Goal:** Predict future Arbitration salaries and simulate Competitive Balance Tax (CBT) implications using player performance metrics (WAR, Age, Service Time) and real-time payroll data scraped from Spotrac.

---

## 👨‍💻 About the Author
**Chris (Suk Min) Yoon** *Senior SDET / QA Automation Engineer (10+ Years Experience)* Specializing in data quality assurance, ETL validation, and backend testing across insurance and financial domains.

* **Expertise:** Python (Pandas, SQLAlchemy, PyTest), SQL, Apache Airflow, Docker, and Playwright.
* **Background:** Proven track record in building automation frameworks at **theScore (ESPN Bet)** and leading QA strategies for enterprise clients like **Avesis** and **Jewelers Mutual**.

---

## 🚀 Key Features (Phase 1: MVP & Data Ingestion)

### 1. Advanced Web Scraping Pipeline (Playwright)
* **Headless Browser Automation:** Utilizes **Playwright** to handle dynamic JavaScript content on sites like Spotrac, overcoming limitations of static parsers (BeautifulSoup).
* **Dockerized Browser Environment:** Custom-built Docker image pre-configured with Chromium binaries and system dependencies, resolving common "Ghost Binary" and shared memory issues in containerized scraping.
* **Resilience:** Implements optimized page handling (e.g., `--disable-dev-shm-usage`) to ensure stability during heavy data extraction.

### 2. Robust Data Warehouse & Orchestration
* **Apache Airflow Integration:** Automated DAGs trigger the scraping scripts, process the raw HTML, and load data into the warehouse.
* **Star Schema Architecture:** Production-grade PostgreSQL design with `dim_players` and `fact_contracts`.
* **Containerized Stack:** The entire infrastructure (Postgres, Airflow Webserver/Scheduler, Playwright Workers) is defined in `docker-compose` for one-click deployment.

### 3. Automated Data QA (SDET Layer)
* **Strict Linting & Compliance:** Adheres to PEP 8 standards with a zero-warning policy.
* **Data Integrity Tests:** Integrated **Pytest** suite to validate schema consistency, foreign key relationships, and data accuracy across the ETL lifecycle.

---

## 🛠 Tech Stack
* **Language:** Python 3.12+
* **Orchestration:** Apache Airflow 2.10+
* **Scraping:** Playwright (Chromium), LXML
* **Database:** PostgreSQL (Star Schema)
* **Infrastructure:** Docker, Docker Compose (Custom Image)
* **Data Processing:** Pandas, SQLAlchemy
* **Quality Assurance:** Pytest, Flake8, Pandera

---

## 📂 Project Structure
```text
bluejays-financial-mlops/
├── dags/
│   ├── payroll_etl_dag.py       # Airflow DAG for Spotrac extraction
│   └── bluejays_pipeline.py     # Main orchestration logic
├── scripts/
│   └── scraper_playwright.py    # Headless browser scraping logic
├── docker-compose.yaml          # Full stack container configuration
├── Dockerfile.airflow           # Custom image with Playwright & Browsers
├── plots/                       # Generated visualizations
├── src/
│   ├── models.py                # SQLAlchemy Schema Definitions
│   ├── etl_bluejays.py          # Roster Sync Logic
│   └── init_db.py               # Database Initialization
├── tests/                       # Unit & Data Integrity Tests
└── requirements.txt             # Python Dependencies

```

---

## ⚙️ How to Run

### 1. Build the Infrastructure

This project requires a custom Docker build to install Chromium dependencies.

```bash
# Build the image (using --no-cache is recommended for browser updates)
docker-compose build --no-cache

```

### 2. Launch Services

Start the Airflow Scheduler, Webserver, and PostgreSQL database.

```bash
docker-compose up -d

```

### 3. Trigger the ETL Pipeline

1. Access the Airflow UI at `http://localhost:8080`.
2. Login (default: `airflow` / `airflow`).
3. Unpause and Trigger the **`bluejays_payroll_pipeline`** DAG.
4. Watch the Graph view as the container launches a headless browser, scrapes Spotrac, and generates the payroll CSV.

### 4. Run Data Integrity Tests

Verify that the data loaders and database schema meet quality standards:

```bash
# Run tests inside the container or locally if venv is active
pytest tests/

```

---

## 🗺 Roadmap

| Phase | Focus | Status |
| --- | --- | --- |
| **Phase 1** | **MVP & Data Integrity** (Postgres, Airflow, Docker) | ✅ Completed |
| **Phase 1.5** | **Advanced Scraping** (Playwright Integration, Spotrac ETL) | ✅ Completed |
| **Phase 2** | **"What-If" Simulation Engine** (Signings, Trades, & 2026 CBT Forecasts) | 🚧 In Progress |
| **Phase 3** | **Cloud MLOps** (GCP integration & Salary Prediction Models) | 📅 Planned |
