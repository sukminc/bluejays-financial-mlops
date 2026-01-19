# Blue Jays Moneyball: Financial Forecasting & Integrity System

![Python](https://img.shields.io/badge/Python-3.9%2B-blue)
![Status](https://img.shields.io/badge/Phase--1-Complete-green)
![Tests](https://img.shields.io/badge/Tests-Passing-brightgreen)
![Code Quality](https://img.shields.io/badge/Code%20Quality-A%2B-success)

## 📌 Project Overview
This project applies **Data Engineering** and **Machine Learning** to forecast the Toronto Blue Jays' financial future.
Unlike simple stats aggregators, this system focuses on **Data Integrity (SDET principles)** to ensure that data fed into Luxury Tax simulations is accurate, validated, and resilient against API failures.

**Goal:** Predict future Arbitration salaries and simulate Luxury Tax implications using player performance metrics (WAR, Age, Service Time).

---

## 🚀 Key Features (Phase 1: MVP)

### 1. Robust ETL Pipeline
* **Hybrid Data Extraction:** Fetches stats via `pybaseball` and scrapes financial data directly from Baseball-Reference using `requests` and `pandas`.
* **Resilience & Fallback:** Implements a "Cached Fallback" mechanism. If the external site returns a 404 or is down, the system seamlessly switches to an internal cached dataset (Real 2024 Roster) to prevent pipeline crashes.

### 2. Automated Data QA (SDET Layer)
* **Contract Testing:** Uses **Pandera** to enforce strict schema validation on incoming data (e.g., "Salary must be > $740k", "WAR must be between -5 and 15").
* **Strict Linting:** Adheres to PEP 8 standards using `flake8` and `pylance` (Zero warnings policy).
* **Comprehensive Testing:** 100% test coverage for data loaders, type checks, and schema validation.

### 3. Visual Analytics
* Automatically generates "WAR vs. Salary" scatter plots to identify value efficiency (e.g., underpaid high-performers).

---

## 🛠 Tech Stack
* **Language:** Python 3.9+
* **Data Processing:** Pandas, NumPy
* **Validation:** Pandera (Schema Validation), Pytest (Unit Testing)
* **Web Scraping:** Requests, Lxml
* **Visualization:** Matplotlib, Seaborn
* **Code Quality:** Flake8, Pytest-Warnings

---

## 📂 Project Structure
```text
bluejays-financial-mlops/
├── .github/workflows/   # CI/CD Pipeline configuration
├── plots/               # Generated visualizations (WAR vs Salary)
├── src/
│   ├── data_loader.py   # ETL Logic (Scraper + Fallback + Merge)
│   ├── validator.py     # Pandera Schema Definitions (Data Contracts)
│   └── visualize.py     # Plotting Logic
├── tests/
│   ├── conftest.py      # Global Test Config (Warning suppression)
│   └── test_data_loader.py # Unit & Integration Tests
├── pytest.ini           # Test configuration
└── requirements.txt     # Dependencies

```

## ⚙️ How to Run

### 1. Setup Environment

```bash
# Create virtual environment
python -m venv .venv
source .venv/bin/activate  # Windows: .venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt

```

### 2. Run Data Integrity Tests

Verify that the scraper, fallback logic, and validation schemas are working.

```bash
pytest tests/

```

*Expected Output:* `5 passed, 0 warnings`

### 3. Generate Analysis

Fetch data (or use fallback), validate it, and generate the scatter plot.

```bash
python -m src.visualize

```

*Output:* Check `plots/war_vs_salary.png`.

---

## 🗺 Roadmap

| Phase | Focus | Status |
| --- | --- | --- |
| **Phase 1** | **MVP & Data Integrity Layer** (Scraper, Validation, Tests) | ✅ **Completed** |
| **Phase 2** | **Cloud Automation** (GCP + Airflow Pipelines) | 🚧 *Next Step* |
| **Phase 3** | **Machine Learning** (Salary Prediction Model) | 📅 *Planned* |

---

## 📈 Sample Output

**2024 Blue Jays: Performance (WAR) vs Cost (Salary)**
*(See `plots/war_vs_salary.png` in the repo)*
