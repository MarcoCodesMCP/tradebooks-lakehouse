# TradeBooks Lakehouse 🔧📊
**An end-to-end Databricks lakehouse that turns messy bookkeeping exports from trade contractors (plumbers, electricians, HVAC, roofers, carpenters) into profitability, cash-flow and collections insights.**

Built on Databricks (Unity Catalog, Delta Lake, Auto Loader, Lakeflow Jobs, AI/BI Dashboards) with **PySpark** for data engineering and **pandas** for analytics.

---

## The business problem
Small trade contractors run on QuickBooks exports, spreadsheets and paper timesheets. They know their revenue — but not **which jobs actually make money, who owes them, or what next month looks like**. TradeBooks is the data platform behind a bookkeeping + analytics service for these contractors.

## Architecture (Medallion)

```mermaid
flowchart LR
    A[Client exports<br/>CSV + JSON] -->|Auto Loader| B[(Bronze<br/>raw, as-is)]
    B -->|PySpark clean,<br/>dedupe, DQ rules, MERGE| C[(Silver<br/>trusted)]
    C -->|aggregations,<br/>window functions| D[(Gold<br/>business marts)]
    D -->|pandas| E[Insights<br/>forecast · RFM · pricing · risk]
    D --> F[AI/BI Dashboard]
    E --> F
    C -.bad rows.-> Q[(Quarantine)]
```

| Layer | Tables | What happens |
|---|---|---|
| Landing | Volume `landing/` | 7 entities as monthly CSV/JSON drops (~45K rows, 21 months) |
| Bronze | `bronze_*` | Incremental ingest with Auto Loader, schema rescue, lineage columns |
| Silver | `silver_*`, `silver_quarantine`, `dq_run_log` | Type casting, mixed-format dates, `$1,234` strings, sign errors, dedupe, DQ rules, Delta `MERGE` |
| Gold | `gold_job_profitability`, `gold_monthly_pnl`, `gold_ar_aging`, `gold_cash_flow`, `gold_tech_utilization`, `gold_lead_source`, `gold_client_kpis` | Business-ready marts |
| Insights | `insight_*` | Forecasting, segmentation, pricing, collections risk |

## Results (synthetic data, as of 2026-09-30)
| Metric | Value |
|---|---|
| Combined TTM revenue (5 contractors) | **$17.3M** at **22.0%** net margin |
| Job types below 40% margin target | **12 of 15** |
| Biggest pricing opportunity | Roofing full replacements — **~29.8%** price increase → **~$1.78M/yr** uplift |
| Open A/R | **$1.85M**, of which **$0.96M** is 60+ days late |
| Customers flagged for priority collections | **109** |
| "Champions" segment | **28%** of customers → **50%** of revenue |
| 3-month revenue forecast backtest error (MAPE) | **13.3%** |

## Skills demonstrated
**Data engineering (PySpark):** Auto Loader incremental ingestion · Unity Catalog + Volumes · Delta Lake `MERGE` upserts · idempotent re-runs · window-function dedupe · data-quality rules + quarantine · DQ run log · partitioning · multi-task Lakeflow Job orchestration · Databricks Asset Bundle (IaC)

**Data analytics (pandas):** P&L and unit economics · A/R aging + DSO · seasonality indexing · trend × seasonal forecasting with backtesting · RFM segmentation · pricing elasticity math · risk scoring · matplotlib visualization · SQL dashboards

## Repo layout
```
notebooks/
  00_setup.py               schema + volumes
  01_generate_data.py       synthetic messy source data (pandas)
  02_bronze_ingest.py       Auto Loader → Bronze (PySpark)
  03_silver_transform.py    cleaning, DQ, MERGE → Silver (PySpark)
  04_gold_marts.py          business marts → Gold (PySpark)
  05_analytics_insights.py  forecasting, RFM, pricing, risk (pandas)
sql/dashboard_queries.sql   10 datasets for the AI/BI dashboard
databricks.yml              Lakeflow Job as code (Asset Bundle)
WALKTHROUGH.md              step-by-step build guide
```

## Run it
Runs on **Databricks Free Edition** (serverless). See [`WALKTHROUGH.md`](WALKTHROUGH.md), or quick version:
1. Workspace → **Create → Git folder** → paste this repo URL
2. Run notebooks `00` → `05` in order (defaults: catalog `workspace`, schema `tradebooks`)
3. Build the dashboard from `sql/dashboard_queries.sql`

*All data is synthetic. No real client or employer data is used.*

UPDATE: 
README
TradeBooks Lakehouse 🔧📊
An end-to-end Databricks lakehouse that turns messy bookkeeping exports from trade contractors (plumbers, electricians, HVAC, roofers, carpenters) into profitability, cash-flow and collections insights — with automated recommendations and an AI analyst you can ask questions in plain English.
Built on Databricks (Unity Catalog, Delta Lake, Auto Loader, Lakeflow Jobs, AI/BI Dashboards, Genie) with PySpark for data engineering and pandas for analytics.
The business problem
Small trade contractors run on QuickBooks exports, spreadsheets and paper timesheets. They know their revenue — but not which jobs actually make money, who owes them, or what next month looks like. TradeBooks is the data platform behind a bookkeeping + analytics service for these contractors: drop in a client's files, and the pipeline produces their monthly numbers, charts and 3–5 recommended actions automatically.
Architecture (Medallion)
flowchart LR
    A[Client exports<br/>CSV + JSON] -->|Auto Loader| B[(Bronze<br/>raw, as-is)]
    B -->|PySpark clean,<br/>dedupe, DQ rules, MERGE| C[(Silver<br/>trusted)]
    C -->|aggregations,<br/>window functions| D[(Gold<br/>business marts)]
    D -->|pandas| E[Insights<br/>forecast · RFM · pricing · risk]
    E --> R[Recommendations<br/>insights + actions]
    D --> F[AI/BI Dashboard]
    E --> F
    R --> F
    D --> G[Genie AI analyst]
    R --> G
    C -.bad rows.-> Q[(Quarantine)]
Client exports
CSV + JSON

Bronze
raw, as-is

Silver
trusted

Gold
business marts

Insights
forecast · RFM · pricing · risk

Recommendations
insights + actions

AI/BI Dashboard

Genie AI analyst

Quarantine

Auto Loader

PySpark clean,
dedupe, DQ rules, MERGE

aggregations,
window functions

pandas

bad rows

​
Layer
Tables
What happens
Landing
Volume landing/
7 entities as monthly CSV/JSON drops (~45K rows, 21 months)
Bronze
bronze_*
Incremental ingest with Auto Loader, schema rescue, lineage columns
Silver
silver_*, silver_quarantine, dq_run_log
Type casting, mixed-format dates, $1,234 strings, sign errors, dedupe, DQ rules, Delta MERGE
Gold
gold_job_profitability, gold_monthly_pnl, gold_ar_aging, gold_cash_flow, gold_tech_utilization, gold_lead_source, gold_client_kpis
Business-ready marts
Insights
insight_*
Forecasting, segmentation, pricing, collections risk
Recommendations
insight_recommendations
3–5 plain-English insights + actions per client, with confidence level, data basis and disclaimer
Orchestration
A 7-task Lakeflow Job runs the whole pipeline end to end on serverless compute (~5 min), defined as code in databricks.yml.
Unity Catalog tracks lineage automatically across all tables:
Automated recommendations
Notebook 06 writes each contractor's monthly "what to do next" list. Example output:
Pricing · High confidence — Repipe jobs run at a 29.8% gross margin, below the 40% target.
→ Raise Repipe prices about 17.1% or cut its costs; that could add about $271.0K in profit per year.
Based on 285 completed Repipe jobs over 21 months.
Design rule: every number is calculated by code from the data, never by AI. AI wording is optional, and any rewrite that changes a number is rejected. Every recommendation carries a confidence level, what data it is based on, and a disclaimer that it is an estimate, not a guarantee.
AI analyst (Genie)
A Genie space connected to the Gold, Insight and Recommendation tables answers plain-English questions inside the dashboard ("Who should we call first for overdue payments?"). It is guided by business instructions: definitions (net margin = total profit ÷ total revenue), the as-of date, formatting, and a required disclaimer on recommendations. Answers were validated against known table values, and instructions were refined when an answer was wrong.
Results (synthetic data, as of 2026-09-30)
Metric
Value
Combined TTM revenue (5 contractors)
$17.3M at 22.0% net margin
Job types below 40% margin target
12 of 15
Biggest pricing opportunity
Roofing full replacements — ~29.8% price increase → ~$1.78M/yr uplift
Open A/R
$1.85M, of which $0.96M is 60+ days late
Customers flagged for priority collections
109
"Champions" segment
28% of customers → 50% of revenue
3-month revenue forecast backtest error (MAPE)
13.3%
Data quality issues found and fixed
Issue
Impact
Fix
Timesheet rule capped hours at 16/day, but each row is a whole job's hours
3,091 valid timesheets (44%) quarantined → labor cost understated, profit overstated
Limit raised to 400 hrs; quarantine back to 0%
"Cash collected" compared payments (incl. 6.35% CT sales tax) to revenue (excl. tax)
KPI showed an impossible 101%
Revenue grossed up for tax in the KPI
AI analyst used today's date for "last 12 months"
TTM revenue off by ~8%
Instruction added: use the data's as-of date
Skills demonstrated
Data engineering (PySpark): Auto Loader incremental ingestion · Unity Catalog + Volumes · Delta Lake MERGE upserts · idempotent re-runs · window-function dedupe · data-quality rules + quarantine · DQ run log · partitioning · multi-task Lakeflow Job orchestration · Databricks Asset Bundle (IaC) · Git-based workflow
Data analytics (pandas): P&L and unit economics · A/R aging + DSO · seasonality indexing · trend × seasonal forecasting with backtesting · RFM segmentation · pricing math · risk scoring · rules-based recommendation engine · SQL dashboards · AI analyst configuration and validation
Repo layout
notebooks/
  00_setup.py               schema + volumes
  01_generate_data.py       synthetic messy source data (pandas)
  02_bronze_ingest.py       Auto Loader → Bronze (PySpark)
  03_silver_transform.py    cleaning, DQ, MERGE → Silver (PySpark)
  04_gold_marts.py          business marts → Gold (PySpark)
  05_analytics_insights.py  forecasting, RFM, pricing, risk (pandas)
  06_recommendations.py     per-client insights + actions for reports, dashboard and Genie
sql/dashboard_queries.sql   datasets for the AI/BI dashboard
databricks.yml              Lakeflow Job as code (Asset Bundle)
WALKTHROUGH.md              step-by-step build guide
images/                     screenshots
​
Run it
Runs on Databricks Free Edition (serverless). See WALKTHROUGH.md, or quick version:
Workspace → Create → Git folder → paste this repo URL
Run notebooks 00 → 06 in order (defaults: catalog workspace, schema tradebooks)
Build the dashboard from sql/dashboard_queries.sql
All data is synthetic. No real client or employer data is used.
