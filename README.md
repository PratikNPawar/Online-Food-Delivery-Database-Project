<div align="center">

# 🛵 FoodEx — Online Food Delivery Analytics

**End-to-end data warehouse for a food delivery platform**

[![MySQL](https://img.shields.io/badge/MySQL-8.0-4479A1?style=flat-square&logo=mysql&logoColor=white)](https://www.mysql.com/)
[![Azure Synapse](https://img.shields.io/badge/Azure_Synapse-Analytics-0078D4?style=flat-square&logo=microsoftazure&logoColor=white)](https://azure.microsoft.com/en-us/products/synapse-analytics)
[![Azure Data Factory](https://img.shields.io/badge/Azure_Data_Factory-ETL-0078D4?style=flat-square&logo=microsoftazure&logoColor=white)](https://azure.microsoft.com/en-us/products/data-factory)
[![Power BI](https://img.shields.io/badge/Power_BI-Dashboards-F2C811?style=flat-square&logo=powerbi&logoColor=black)](https://powerbi.microsoft.com)
[![Azure ML](https://img.shields.io/badge/Azure_ML-AI_Models-0078D4?style=flat-square&logo=microsoftazure&logoColor=white)](https://azure.microsoft.com/en-us/products/machine-learning)
[![dbt](https://img.shields.io/badge/dbt-Transformations-FF694B?style=flat-square&logo=dbt&logoColor=white)](https://www.getdbt.com)
[![License: MIT](https://img.shields.io/badge/License-MIT-22c55e?style=flat-square)](LICENSE)

**[🌐 Live Portfolio](https://pratikNPawar.github.io/Online-Food-Delivery-Database-Project)  ·  [📊 Schema](#-database-schema)  ·  [🔄 ETL](#-etl-pipeline)  ·  [🤖 AI Models](#-ai--ml-models)**

</div>

---

## What this project covers

| Layer | Tech | What's built |
|---|---|---|
| **OLTP Database** | MySQL 8.0 | 9 tables: Orders, Customers, Restaurants, Menus, Delivery, Payments, Feedback |
| **ETL Pipeline** | Azure Data Factory | MySQL → Bronze → Silver → Gold medallion architecture |
| **Data Warehouse** | Azure Synapse Analytics | Star schema: FactOrders + 6 dimensions, SCD Type 2 |
| **Transformations** | dbt Core | Staging → marts, data quality tests on every model |
| **Dashboards** | Microsoft Power BI | 5 pages: Sales, Delivery, Customers, Restaurant, AI Insights |
| **AI / ML** | Azure Machine Learning | Demand forecasting, churn prediction, delivery ETA, sentiment analysis |

---

## Architecture

```
┌────────────────────────────────────────────┐
│  MySQL OLTP (FoodEx)                       │
│  9 tables · stored procedures · triggers   │
└───────────────────┬────────────────────────┘
                    │
                    │  Azure Data Factory
                    │  CDC every 15 minutes
                    ▼
┌────────────────────────────────────────────┐
│  Azure Data Lake Storage Gen2              │
│  🥉 Bronze  →  🥈 Silver  →  🥇 Gold      │
│       dbt transformations between layers   │
└───────────────────┬────────────────────────┘
                    │
                    ▼
┌────────────────────────────────────────────┐
│  Azure Synapse Analytics (Dedicated Pool)  │
│  FactOrders · DimCustomer · DimRestaurant  │
│  DimMenuItem · DimDeliveryExec · DimDate   │
└──────────────┬─────────────────────────────┘
               │
       ┌───────┴────────┐
       ▼                ▼
   Power BI         Azure ML
   5 Dashboards     4 AI Models
```

---

## Database Schema

**9 tables** modelling every interaction in the platform:

```
Customer  ──places──────►  Order  ──contains──►  Items_ordered  ──from──►  Menu_items  ──►  Restaurant
    │                         │
    │                         ├──delivers──►  Delivery_Exec
    │                         ├──pays──────►  Payment
    │                         └──provides──►  Feedback
    │
    └──subscribes──►  Free_Delivery_Pass (FDP)
```

Key design decisions:
- `Order` stores the **full status pipeline** with 4 timestamps — `placed → confirmed → picked → delivered`
- Delivery delay = `TIMEDIFF(order_delivered_time, expected_delivery_time)` — negative means early
- `Feedback.rated_person` flag (`'D'` = delivery exec, `'R'` = restaurant) enables split performance scoring
- **SCD Type 2** on `DimCustomer` preserves history whenever loyalty tier changes

---

## SQL Analytics

| Query | Business question answered |
|---|---|
| Cuisine frequency | Which cuisines drive the most orders? |
| Top 5 locations | Where should we hire more delivery execs? |
| Orders by day of week | When do we need peak staffing? |
| COD vs online payment split | How much to invest in payment gateways? |
| Customer acquisition MoM | Individual vs business customer growth? |
| Customer bounce rate | What % registered but never ordered? |
| Inactive customer list | Who to target with re-engagement campaigns? |
| Delivery exec timeline | Restaurant prep time vs ride time vs expected ETA? |
| Exec performance scoring | 1–5 star score per exec based on delay minutes |

### Sample: Customer bounce rate
```sql
-- What % of registered users never placed a single order?
SELECT
    ROUND(
        COUNT(CASE WHEN cust_id NOT IN
            (SELECT `cust_id(FK)` FROM `Order`)
        THEN 1 END) * 100.0
        / COUNT(cust_id), 2
    ) AS bounce_rate_pct
FROM Customer
WHERE cust_user = 1;
```

### Sample: Customer RFM segmentation
```sql
WITH RFM AS (
    SELECT
        c.cust_id,
        DATEDIFF(DAY, MAX(o.order_placed_time), GETDATE()) AS recency_days,
        COUNT(DISTINCT o.order_id)                         AS frequency,
        SUM(o.total_amount)                                AS monetary
    FROM Customer c
    JOIN `Order` o ON c.cust_id = o.`cust_id(FK)`
    GROUP BY c.cust_id
),
Scored AS (
    SELECT *,
        NTILE(5) OVER (ORDER BY recency_days ASC)  AS r_score,
        NTILE(5) OVER (ORDER BY frequency DESC)    AS f_score,
        NTILE(5) OVER (ORDER BY monetary DESC)     AS m_score
    FROM RFM
)
SELECT
    CASE
        WHEN r_score >= 4 AND f_score >= 4 AND m_score >= 4 THEN 'Champions'
        WHEN r_score >= 3 AND f_score >= 3                   THEN 'Loyal Customers'
        WHEN r_score <= 2 AND f_score >= 3                   THEN 'At Risk'
        WHEN r_score <= 2 AND f_score <= 2                   THEN 'Lost Customers'
        ELSE 'Potential Loyalists'
    END AS segment,
    COUNT(*)           AS customers,
    AVG(monetary)      AS avg_spend
FROM Scored
GROUP BY segment
ORDER BY avg_spend DESC;
```

---

## Stored Procedures & Triggers

**`cust_inactive()`** — flags customers who registered 2+ years ago without ordering, or whose last order was 2+ years ago. Called nightly via ADF schedule trigger.

**`update_cust_trig`** — `BEFORE UPDATE` trigger on the `Customer` table. Automatically writes old row values to `Customer_updated` before any change. Zero-code audit trail — every customer record change is preserved forever.

---

## ETL Pipeline

Azure Data Factory loads data from MySQL every 15 minutes using **watermark-based CDC** (change data capture):

```
MySQL → ADF (CDC, 15-min incremental) → ADLS Bronze (raw Parquet)
                                              ↓  dbt staging models
                                         ADLS Silver (cleansed, typed, deduped)
                                              ↓  dbt mart models
                                         Synapse Gold (star schema)
                                              ↓
                                     Power BI / Azure ML
```

**dbt handles all transformations:**
- Decode boolean flags → readable strings (`order_type`, `cust_type`, `order_source`)
- Compute `prep_time_mins`, `delivery_ride_mins`, `delivery_delay_mins`
- SCD Type 2 upserts on `DimCustomer` (loyalty tier history preserved)
- Schema tests (`not_null`, `unique`, `accepted_values`) on every model

---

## Power BI Dashboards

| Dashboard | Key metrics |
|---|---|
| Executive summary | GMV, avg order value, orders by city map, gross margin |
| Delivery operations | On-time rate, avg delivery time, exec leaderboard |
| Customer insights | Bounce rate, inactive %, RFM scatter, churn risk heatmap |
| Restaurant & menu | Top cuisines, restaurant ratings, price range analysis |
| AI insights | 30-day demand forecast, anomaly feed, next-best-action |

**Key DAX measures:**
```dax
OnTimeDeliveryRate% =
    DIVIDE(COUNTROWS(FILTER(FactOrders, [delivery_delay_mins] <= 0)), COUNTROWS(FactOrders)) * 100

BounceRate% =
    DIVIDE(CALCULATE(COUNTROWS(DimCustomer), [total_orders] = 0), COUNTROWS(DimCustomer)) * 100

AvgDeliveryTime =
    AVERAGE(FactOrders[total_delivery_mins])
```

---

## AI / ML Models

| Model | Algorithm | What it does |
|---|---|---|
| **Demand forecasting** | Facebook Prophet | Predicts hourly order volume per city and cuisine. Used to pre-position delivery execs before peak hours. |
| **Churn prediction** | XGBoost classifier | Scores every customer's churn probability from recency, frequency, delivery ratings, FDP status. |
| **Delivery ETA** | Gradient Boosting | Predicts real delivery time from distance, time of day, restaurant type, exec workload. |
| **Review sentiment** | Azure Cognitive Services | Extracts food / service / delivery aspect sentiment from `Feedback.comments`. |

All models tracked in MLflow on Azure ML. Outputs written back to Synapse for Power BI alerts.

---

## Quick Start

```bash
# 1. Clone
git clone https://github.com/PratikNPawar/Online-Food-Delivery-Database-Project.git
cd Online-Food-Delivery-Database-Project

# 2. Create database
mysql -u root -p -e "CREATE DATABASE FOODEX;"

# 3. Load schema + stored procedures + triggers
mysql -u root -p FOODEX < FoodEX_final_03132019.sql

# 4. Load sample data (80 orders, menus, customers, feedback)
mysql -u root -p FOODEX < Foodex_final_03132019_Data_export.sql

# 5. Verify
mysql -u root -p -e "USE FOODEX; SELECT COUNT(*) AS total_orders FROM \`Order\`;"
```

---

## Project Structure

```
Online-Food-Delivery-Database-Project/
│
├── FoodEX_final_03132019.sql              # Core schema, stored procedures, triggers, analytics
├── Foodex_final_03132019_Data_export.sql  # Sample data: 80 orders across restaurants
├── DBMS Final Project.docx                # Project documentation
├── Foodex Process Diagram.png             # Business process flow
├── Foodex Swimlane.png                    # Swimlane: Customer / Restaurant / Exec
├── index.html                             # GitHub Pages portfolio site
└── README.md
```

---

## Roadmap

- [x] MySQL OLTP schema (9 tables, FK relationships, 80+ sample orders)
- [x] SQL analytics (9 business queries)
- [x] Stored procedures + audit triggers
- [ ] Azure Data Factory ETL pipelines
- [ ] Azure Synapse star schema (FactOrders + 6 dims)
- [ ] Power BI dashboards (5 pages)
- [ ] Demand forecasting model (Prophet)
- [ ] Customer churn model (XGBoost)
- [ ] GitHub Actions CI/CD (dbt test on PR)

---

## Author

**Pratik N. Pawar**
[GitHub](https://github.com/PratikNPawar) · [LinkedIn](https://linkedin.com/in/YOUR_LINKEDIN_HERE)

---

<div align="center">
⭐ Star this repo if it helped you understand food delivery data systems!
</div>
