# 🧧 Discount Validation & Analytics Pipeline

[![Python 3.12+](https://img.shields.io/badge/python-3.12+-blue.svg?style=for-the-badge&logo=python)](https://www.python.org/downloads/)
[![Pandas](https://img.shields.io/badge/pandas-%23150458.svg?style=for-the-badge&logo=pandas&logoColor=white)](https://pandas.pydata.org/)
[![SQLite](https://img.shields.io/badge/sqlite-%2307405e.svg?style=for-the-badge&logo=sqlite&logoColor=white)](https://www.sqlite.org/)
[![Architecture: Modular ETL](https://img.shields.io/badge/Architecture-Modular%20ETL-green.svg?style=for-the-badge)](./ARCHITECTURE.md)

A production-grade, high-throughput ETL pipeline designed to ingest, validate, and analyze discount applications. Built with **Pandas** for vectorized performance and **SQLite** for ACID-compliant persistence.

---

## ✨ Core Features

*   **🛡️ Strict Integrity Gating**: Automated Dead Letter Queue (DLQ) for malformed records (negative prices, invalid timestamps).
*   **🧠 Intelligent Rules Engine**:
    *   **Expiry Validation**: Rejects coupons used after their expiration date.
    *   **Category Scope**: Ensures coupons are only applied to eligible product categories.
    *   **Deduplication**: Prevents double-dipping of the same coupon on a single order.
*   **⚔️ Conflict Resolution**: O(N log N) logic that resolves exclusivity conflicts by maximizing customer value.
*   **📊 Business Intelligence**: Real-time impact reporting covering actual revenue, burn rates, and revenue protected.
*   **🔌 Modular Architecture**: Highly decoupled layers for extraction, transformation, and loading.

---

## 📂 Project Structure

```text
pipelinevalidation/
├── src/
│   ├── extract.py      # Ingestion & DLQ Routing
│   ├── transform.py    # Rules Engine & Financial Logic
│   ├── load.py         # ACID Persistence (SQLite)
│   ├── analytics.py    # Reporting & BI
│   ├── schemas.py      # Strict Type Definitions
│   └── config.py       # Global Logging & Settings
├── main.py             # Pipeline Orchestrator
├── ARCHITECTURE.md     # Technical Deep-Dive
└── requirements.txt    # Dependencies
```

---

## 🚀 Quick Start

### 1. Environment Setup
```bash
# Clone the repository
git clone https://github.com/your-repo/pipelinevalidation.git
cd pipelinevalidation

# Install dependencies
pip install -r requirements.txt
```

### 2. Execute the Pipeline
```bash
python main.py
```

### 3. Review Results
The pipeline will output a **Business Impact Report** to the console and persist all records to `discount_analytics.db`:
*   `discount_audit`: Final accepted and rejected records.
*   `quarantine_logs`: Records failed by the DLQ.

---

## 📊 Sample Impact Report

```text
════════════════════════════════════════════════════════════
          DISCOUNT VALIDATOR: BUSINESS IMPACT REPORT
════════════════════════════════════════════════════════════
PIPELINE VOLUME:
  Processed: 9 | Accepted: 5 | Rejected: 4
  Quarantined (DLQ): 2

FINANCIAL METRICS:
  Actual Revenue:         $1,365.00
  Counterfactual Revenue: $2,250.00
  Total Discount Burn:    $885.00

LOSS PREVENTION:
  Revenue Protected:      $50.00
════════════════════════════════════════════════════════════
```

---

## 🛠️ Built With

*   [Pandas](https://pandas.pydata.org/) - High-performance data manipulation.
*   [NumPy](https://numpy.org/) - Numerical optimization.
*   [SQLite](https://www.sqlite.org/) - Transactional storage.
*   [Mermaid.js](https://mermaid.js.org/) - Architecture visualization.

---

## ⚖️ License
Distributed under the MIT License. See `LICENSE` for more information.

---
*Developed with ❤️ by the Khushboo kaushik*
