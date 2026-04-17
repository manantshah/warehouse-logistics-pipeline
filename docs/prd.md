### 🎯 Objective
Build an end-to-end DataOps pipeline to provide daily visibility into warehouse fulfillment performance, specifically identifying bottlenecks in order processing.

### 📈 Business Problem
Warehouse managers currently lack a centralized, automated view of fulfillment health. Manual reporting is slow, prone to error, and prevents real-time decision-making regarding labor allocation and shipping delays.

### 👥 Stakeholders
- **Warehouse Operations Manager:** Needs to see fulfillment volume by location.
- **Logistics Team:** Needs to identify delayed orders before they impact customers.
- **Data Engineering (Me):** Needs a scalable, observable pipeline for future scaling.

### ⏱️ Service Level Agreements (SLAs)
- **Freshness:** Data must be regularly refreshed in the dashboard.
- **Accuracy:** Order IDs must be unique; null values in timestamps are not permitted.
- **Reliability:** The pipeline must include automated data quality tests (dbt) before reaching Prod.