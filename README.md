
# Blue Jays Moneyball: Financial Forecasting & Integrity System

## 📌 Project Overview
This project applies **Data Engineering** and **Machine Learning** to forecast the Toronto Blue Jays' financial future. 
Unlike simple salary aggregators, this system focuses on **Data Integrity (SDET principles)** to ensure that the data fed into Luxury Tax simulations is accurate, validated, and reliable.

**Goal:** Predict future Arbitration salaries and simulate Luxury Tax implications using player performance metrics (WAR, Age, Service Time).

---

## 🚀 Key Features (Phase 1: MVP)
* **Robust Data Ingestion:** Fetches real-time MLB stats and salary data using `pybaseball` with failsafe mock strategies.
* **Automated Data Integrity:** Uses `pytest` to validate schema, data types, and business logic (e.g., "Salary must be positive", "Roster size limits") before analysis begins.
* **Visual Analytics:** Automatically generates correlation graphs (WAR vs. Salary) to identify value efficiency.
* **CI/CD Ready:** Configured for GitHub Actions to run integrity checks on every commit.

---

## 🛠 Tech Stack
* **Language:** Python 3.9+
* **Data Manipulation:** Pandas, NumPy
* **Visualization:** Matplotlib, Seaborn
* **Testing & Quality:** Pytest, Flake8, Pylance
* **Data Source:** Pybaseball (MLB Stats API wrapper)

---

## 📂 Project Structure
```text
bluejays-financial-mlops/
├── .github/workflows/   # CI/CD Pipeline configuration
├── plots/               # Generated visualizations (e.g., WAR vs Salary)
├── src/
│   ├── data_loader.py   # ETL Logic (Extract, Transform, Merge)
│   └── visualize.py     # Plotting Logic
├── tests/               # Data Integrity Tests
├── pytest.ini           # Test configuration
└── requirements.txt     # Python dependencies

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

Verify that the data fetching and merging logic adheres to quality standards.

```bash
pytest tests/

```

### 3. Generate Analysis

Fetch data, merge datasets, and generate the "WAR vs Salary" scatter plot.

```bash
python -m src.visualize

```

*Output:* Check the `plots/` directory for `war_vs_salary.png`.

---

## 🗺 Roadmap

| Phase | Focus | Status |
| --- | --- | --- |
| **Phase 1** | **MVP & Data Integrity Layer** (Local ETL + Tests) | ✅ **Completed** |
| **Phase 2** | **Cloud Automation** (GCP + Airflow Pipelines) | 🚧 *Up Next* |
| **Phase 3** | **Machine Learning** (Salary Prediction Model) | 📅 *Planned* |

---

## 📈 Sample Output

**2024 Blue Jays: Performance (WAR) vs Cost (Salary)**
*(See `plots/war_vs_salary.png` in the repo)*


