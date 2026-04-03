# Simulation Model

## Layer 1: Recession Indicator

The model will estimate:

**P(recession | macroeconomic indicators)**

The output is a probability between **0 and 1**.

| Probability | Interpretation            |
| ----------- | ------------------------- |
| 0–20%       | Economic expansion        |
| 20–40%      | Economic slowdown         |
| 40–60%      | Elevated recession risk   |
| 60–80%      | Recession likely          |
| 80–100%     | Recession highly probable |

The model will specifically estimate:

* **P(recession_6m)** — Probability of recession within the next 6 months
* **P(recession_12m)** — Probability of recession within the next 12 months
* **P(stagflation)** — Probability of stagflation

---

# Yield Curve

The model will validate the **Treasury yield spread indicator**:

**10-year Treasury yield – 3-month Treasury yield**

If the result is **negative**, it indicates a **yield curve inversion**.

Historically, yield curve inversions have preceded nearly all U.S. recessions.

Example of U.S. recessions according to the **National Bureau of Economic Research (NBER)**:

| Year | Prior Yield Curve Inversion |
| ---- | --------------------------- |
| 1980 | Yes                         |
| 1990 | Yes                         |
| 2001 | Yes                         |
| 2008 | Yes                         |
| 2020 | Yes                         |

Spread calculation:

```
spread = treasury_10y - treasury_3m
```

---

# Layer 2: Financial Stress

Institutional investors often incorporate **financial market indicators**, since markets typically react **before the real economy**.

Typical indicators include:

* VIX (market volatility index)
* Credit spreads
* Liquidity indices
* Stock market drawdowns

Therefore, the model will consider the following variables:

* **VIX**
* **S&P 500 returns**
* **Corporate bond spread**
* **Financial Stress Index**

---

# Layer 3: Real Economic Activity

This layer incorporates **macroeconomic indicators**, primarily sourced from **Federal Reserve Economic Data (FRED)**.

### Labor Market

* unemployment_rate
* initial_jobless_claims
* payrolls

### Inflation

* CPI
* core_CPI
* PCE

### Production

* industrial_production
* PMI
* retail_sales

---

# Modern Model Architecture

Modern macroeconomic forecasting models combine multiple indicators into a **multivariate time series**.

Conceptual feature structure:

```
X = [
 yield_curve
 unemployment
 CPI
 oil_price
 VIX
 credit_spread
 industrial_production
]
```

Possible model architectures:

* **LSTM (Long Short-Term Memory neural networks)**
* **Temporal Transformers**

Model output:

**P(recession_next_12_months)**

---

# Baseline Model Example

Example using **Logistic Regression**:

```python
import pandas as pd
from sklearn.linear_model import LogisticRegression

features = [
"yield_spread",
"unemployment_rate",
"inflation",
"oil_price",
"vix",
]
df = pd.DataFrame(features)

X = df[features]
y = df["recession"]

model = LogisticRegression()
model.fit(X, y)

df["recession_probability"] = model.predict_proba(X)[:,1]
```

---

# Hedge Fund Approaches

Large hedge funds typically incorporate **many more variables**.

Some models use **100–300 macroeconomic indicators**.

Example indicator groups:

### Macroeconomic

* GDP
* CPI
* unemployment

### Financial Markets

* S&P 500
* bond spreads
* VIX

### Liquidity

* M2 money supply
* repo rates
* bank lending

### Energy

* oil prices
* gas prices
* electricity prices

---

# Stagflation

Stagflation occurs when:

* **Inflation increases**
* **Economic growth declines**
* **Unemployment rises**

Therefore, some models estimate two probabilities simultaneously:

* **P(recession)**
* **P(high_inflation)**

If both probabilities are high, the model signals **stagflation risk**.

---

# Geopolitical Risk

Wars and geopolitical conflicts primarily impact the economy through **energy markets and supply chains**.

The model will incorporate:

* oil_price_change
* energy_CPI
* shipping_costs

Additionally, the model will analyze **global geopolitical risk**.

### Geopolitical Risk Index

This index quantifies events such as:

* wars
* diplomatic tensions
* terrorism
* economic sanctions
