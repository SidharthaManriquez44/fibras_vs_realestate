# Real Estate vs FIBRAs Investment Analysis

This project analyzes the financial performance of two investment strategies:

1. Purchasing a rental apartment in **Mexico City (Narvarte Poniente)** using mortgage financing.
2. Investing the same capital in a diversified **FIBRA portfolio** (Mexican REITs).

---

The goal is to evaluate which strategy produces better financial outcomes under realistic assumptions, including:

* mortgage payments
* rental income
* operating costs
* taxes
* property appreciation
* dividend reinvestment from FIBRAs

The analysis simulates both strategies over a **3-year investment horizon**.

---

# Project Objective

The objective of this project is to create a **fair and objective comparison** between:

* **Direct real estate ownership**
* **Public real estate investment through FIBRAs**

Both strategies will use the **same initial capital** and **equivalent monthly cash contributions** to ensure comparability.

---

# Investment Scenario

The base scenario assumes the purchase of a **new apartment in Narvarte Poniente, Mexico City**.

| Variable                 | Value         |
| ------------------------ |---------------|
| Property price           | 3,750,000 MXN |
| Loan percentage          | 70%           |
| Loan amount              | 2,625,000 MXN |
| Down payment             | 1,125,000 MXN |
| Annual interest rate     | 11.20%        |
| Monthly mortgage payment | 30,168 MXN    |
| Estimated rent           | 18,000 MXN    |
| Maintenance              | 1,000 MXN     |

Additional transaction costs such as **notary fees, property registration, and taxes** are incorporated into the model.

---

# Methodology

The analysis simulates two portfolios.


## Real Estate Model

The real estate investment includes:

* mortgage amortization
* rental income
* vacancy periods
* property taxes
* maintenance and repairs
* rent increases based on inflation (CPI)
* property appreciation

Assumptions include:

* **90% occupancy rate**
* annual rent adjustments based on **inflation**
* property price appreciation based on **historical data for Narvarte Poniente**

---

## FIBRA Portfolio Model

The alternative strategy invests the same capital into a **portfolio of Mexican REITs (FIBRAs)**.

Initial capital:

**825,000 MXN** (equivalent to the down payment).

Monthly contributions replicate the **net cash flow required by the real estate investment**.

Dividends are **fully reinvested**.

---

# FIBRA Selection Criteria

The portfolio follows strict investment rules:

* Occupancy above **90%** (excluding hotel sector)
* Avoid **overvalued FIBRAs** (price > NAV / CBFI)
* Minimum **dividend yield between 6% and 7%**
* Debt ratio **below 40%**
* Dividend sustainability:

  **AFFO must exceed the payout ratio**

If a FIBRA becomes **significantly overvalued**, the position may be partially reduced.

---

## Tools and Technologies
## Data Pipeline Architecture

| Layer | Tool | Purpose |
|------|------|------|
| Ingestion | Python | Collect macroeconomic data from FRED and other sources |
| Storage | Parquet | Efficient columnar storage for time series |
| Query Engine | DuckDB | Fast analytical queries on local data lake |
| Orchestration | Apache Airflow | Pipeline scheduling and automation |
| Machine Learning | scikit-learn / XGBoost / TensorFlow | Recession prediction models |


---

# Repository Structure

```
real-estate-vs-fibras/
│ 
├── airflow/                          # Airflow orchestration
│   └── dags/
│       └── model_extraction.py
│ 
├── data/                          
│   ├── raw/                          # Original data
│   ├── processed/                    # Data ready to model
│   └── external/                     # Download data (FRED, INEGI) 
│       
│
├── src/ 
│   ├── fibras_vs_realestate           # Application layer
│       ├── config                     # Settings of the project
│       ├── data                       # Data extraction logic
│       ├── finace                     # Real estate and fibras model
│       ├── models                     # Input data simulation
│       └── visualization              # Dashboard of the models
│
├── tests/                             # Unit tests
│
├── docs/
│   ├── assumptions.md
│   ├── methodology.md
│   └── simulation_model.md
│
└── README.md
```

---
## Documentation

- [Assumptions](docs/assumptions.md)
- [Methodology](docs/methodology.md)
- [Simulation Model](docs/simulation_model.md)
- [Results](docs/results.md)



# Expected Outputs

The model will produce:

* total portfolio value
* cumulative cash flow
* return on investment (ROI)
* comparison between both strategies

The final objective is to determine whether **direct property ownership or FIBRA investment provides better risk-adjusted returns**.

---

# License

MIT License

# Author

**Sidhartha Manríquez**

Data Architecture | Data Platforms | Financial Analytics Engineering