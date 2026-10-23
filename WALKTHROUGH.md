# TradeBooks Lakehouse — Step-by-Step Build Walkthrough
Do everything by hand once so you learn **where things live in Databricks and why each step exists**. Total time: ~3–4 hours.
Each phase has: **Do** (clicks) → **Check** (what you should see) → **Learn** (the concept) → **Try** (a small exercise).

---

## Phase 0 · Accounts (15 min)
**Do**
1. Create a free account at **databricks.com → Try Databricks → Free Edition** (sign up with your personal Gmail, not a work account).
2. Create a **GitHub** account if you don't have one.

**Learn** — Free Edition is serverless: no clusters to configure, you just attach notebooks to *Serverless* compute. It includes Unity Catalog with a default catalog called `workspace`.

---

## Phase 1 · Put the project on GitHub (15 min)
**Do**
1. Unzip `tradebooks-lakehouse.zip` on your computer.
2. GitHub → **New repository** → name `tradebooks-lakehouse` → Public → Create.
3. On the empty repo page click **uploading an existing file** → drag *all* the unzipped files/folders in → **Commit changes**.

**Check** — README renders on the repo homepage with the architecture diagram.

---

## Phase 2 · Tour the workspace (15 min)
Open Databricks and click through the left sidebar. Know what each does:

| Sidebar item | What it is |
|---|---|
| **Workspace** | Your files and notebooks (like a file explorer) |
| **Catalog** | Unity Catalog — every table, volume, and who can access them. Hierarchy: `catalog.schema.table` |
| **Jobs & Pipelines** | Scheduled/orchestrated workflows (Lakeflow Jobs) |
| **SQL Editor** | Run SQL against your tables |
| **Dashboards** | AI/BI dashboards (like a lightweight Qlik/Power BI) |
| **Compute** | Serverless is used automatically in Free Edition |

---

## Phase 3 · Import the repo as a Git folder (10 min)
**Do**
1. **Workspace → Home → Create → Git folder**.
2. Paste your GitHub repo URL → **Create Git folder**.

**Check** — you see `notebooks/`, `sql/`, `README.md` inside Databricks.

**Learn** — Git folders keep notebooks version-controlled. Edits can be committed back to GitHub with the **branch name button** at the top of a notebook — this is how real teams work.

---

## Phase 4 · `00_setup` — Unity Catalog objects (10 min)
**Do**
1. Open `notebooks/00_setup`. Top right → **Connect → Serverless**.
2. Notice the **widgets** bar at the top (catalog, schema, reset). Leave defaults.
3. **Run all**.

**Check** — Output lists two volumes: `landing` and `checkpoints`. Go to **Catalog → workspace → tradebooks** and find them.

**Learn**
- A **schema** groups tables. A **Volume** is governed storage for *files* (CSVs, JSON, images) — tables live in schemas, raw files live in volumes.
- Widgets = notebook parameters. Jobs pass values into them later.

---

## Phase 5 · `01_generate_data` — Source data (10 min)
**Do** — Open, connect to Serverless, **Run all**.

**Check**
- Output prints row counts (~5 clients, ~800 customers, ~7K jobs, etc.).
- **Catalog → tradebooks → Volumes → landing** → browse folders like `jobs/` and `payments/`. Click a file to preview.

**Learn**
- This simulates contractors' bookkeeping exports landing each month. Data is **messy on purpose**: duplicates, `$1,234.00` text amounts, `MM/DD/YYYY` dates, negative amounts, missing customer IDs.
- Read the `inject_mess()` function — knowing what's broken is how you design the cleaning.

**Try** — Open one `invoices_*.csv` in the volume and find a row with a `$` amount.

---

## Phase 6 · `02_bronze_ingest` — Auto Loader (20 min)
**Do** — Run all.

**Check**
- Table of 7 bronze tables with row counts.
- Last cell shows invoices with `$` amounts and `/` dates — still dirty. That's correct.
- **Catalog → tradebooks** → open `bronze_invoices` → **Sample data** tab, then **Lineage** tab.

**Learn**
- **Bronze = raw copy + metadata** (`_source_file`, `_ingested_at`). Never clean here, so you can always rebuild downstream.
- **Auto Loader** (`cloudFiles`) remembers processed files in the checkpoint. `availableNow=True` = process everything new, then stop (cheap batch).
- `inferColumnTypes=false` keeps everything as strings so bad values can't break ingestion.

**Try — prove incremental loading works**
1. Re-run the ingest cell. Row counts **do not double**. Auto Loader skipped files it already saw.
2. In a new cell run:
   ```sql
   %sql
   DESCRIBE HISTORY workspace.tradebooks.bronze_jobs
   ```
   Every write is a version — that's Delta Lake's transaction log.

---

## Phase 7 · `03_silver_transform` — Cleaning + Data Quality (30 min)
**Do** — Run cell by cell (not Run all) and read each one.

**Check**
- DQ table: per entity, bronze rows → duplicates removed → quarantined → silver rows → `pct_clean`.
- Final cell: quarantine reasons (e.g., `missing_customer_id`, `bad_invoice_date`).

**Learn** — the key techniques:

| Problem | Fix in code |
|---|---|
| `$1,234.50` text | `regexp_replace` + `try_cast` to DECIMAL |
| Two date formats | `coalesce(try_to_timestamp(ISO), try_to_timestamp(US))` |
| Negative amounts | `abs()` + `_amount_corrected` flag for audit |
| `MATERIALS ` / `materials` | `initcap(trim())` |
| Duplicate rows | `row_number()` over the ID, keep latest |
| Bad rows | Sent to `silver_quarantine` with a reason — never silently dropped |
| Re-runs | Delta `MERGE` = upsert, so running twice doesn't duplicate |

**Try**
1. Run the notebook again → silver row counts stay the same (idempotent).
2. Time travel:
   ```sql
   %sql
   SELECT COUNT(*) FROM workspace.tradebooks.silver_invoices VERSION AS OF 0
   ```
3. Look at a quarantined record:
   ```sql
   %sql
   SELECT * FROM workspace.tradebooks.silver_quarantine LIMIT 5
   ```

---

## Phase 8 · `04_gold_marts` — Business tables (30 min)
**Do** — Run cell by cell. Read the markdown table at the top first.

**Check** — 7 `gold_*` tables with row counts; last cell shows the KPI scorecard.

**Learn**
- **Job profitability** = invoice revenue − materials − subs/permits − (hours × hourly cost). This is the core product of the business.
- **P&L**: revenue → COGS (job costs + labor) → gross profit → overhead → net profit.
- **A/R aging**: balance = invoice + tax − payments; bucketed by days past due.
- **DSO** (Days Sales Outstanding) = A/R ÷ (annual revenue ÷ 365) — how long it takes to get paid.
- Window functions: `lag()` for month-over-month %, running `sum()` for YTD and cumulative cash.

**Try** — Open **SQL Editor**, run:
```sql
SELECT job_type, ROUND(AVG(gross_margin_pct),1) AS margin
FROM workspace.tradebooks.gold_job_profitability
GROUP BY job_type ORDER BY margin;
```
Which job type is least profitable?

---

## Phase 9 · `05_analytics_insights` — pandas analysis (30 min)
**Do** — Run cell by cell.

**Check** — Charts for margin by job type, seasonality, revenue forecast; tables for RFM segments and collections; a final list of bullet findings.

**Learn**
- `.toPandas()` pulls aggregated Gold data into memory — fine because Gold is small. **Use Spark for big data, pandas for small analysis.**
- **Seasonality index**: month avg ÷ overall avg (HVAC spikes summer/winter, roofing dies in winter).
- **Forecast**: remove seasonality → fit a trend line → add seasonality back. **MAPE** on the last 3 months tells you how wrong it is (~13%).
- **RFM**: score customers on Recency, Frequency, Monetary → segments like *Champions* and *At Risk*.
- **Pricing**: `required price = cost ÷ (1 − target margin)`.

**Try** — Change `TARGET_MARGIN = 40.0` to `35.0`, re-run, and see how many job types still need a price increase.

---

## Phase 10 · Orchestrate as a Job (20 min)
**Do**
1. **Jobs & Pipelines → Create → Job**. Name it `TradeBooks · Medallion Pipeline`.
2. Task 1: name `setup`, type **Notebook**, source **Workspace**, path → `notebooks/00_setup`, compute **Serverless**.
3. **+ Add task** → `generate_data` → `01_generate_data` → *Depends on* `setup`.
4. Repeat for `bronze` (02), `silver` (03), `gold` (04), `analytics` (05), each depending on the previous.
5. **Job parameters**: add `catalog = workspace`, `schema = tradebooks`.
6. **Run now**.

**Check** — The graph view shows 6 boxes in a chain turning green. Click any task to see its notebook output.

**Learn**
- This is what "production pipeline" means: scheduled, dependency-ordered, with run history and failure alerts.
- **Schedules & Triggers → Add trigger → Scheduled** (e.g., daily 6 AM). Leave it paused so it doesn't use quota.
- `databricks.yml` in the repo defines this same job **as code** (Asset Bundles) — mention that on your resume.

**Screenshot this graph for LinkedIn.**

---

## Phase 11 · Build the dashboard (40 min)
**Do**
1. **Dashboards → Create dashboard**.
2. **Data** tab → **Create from SQL** → paste query #1 from `sql/dashboard_queries.sql` → run. Repeat for each query (10 datasets).
3. **Canvas** tab → add widgets:
   - 4 **Counter** tiles from dataset 1 (Revenue, Net margin %, Open A/R, DSO)
   - **Line** chart: dataset 3 (x = month, y = amount, color = series, filter by client)
   - **Bar (stacked)**: dataset 4 (y = client, x = balance, color = aging_bucket)
   - **Table**: dataset 5 (collections call list)
   - **Bar**: dataset 6 (pricing gaps), dataset 7 (lead sources)
   - **Table**: dataset 10 (data quality monitor)
4. Add a **Filter** widget on `client_name`.
5. **Publish**.

**Learn** — Same skills as Qlik: datasets → visuals → filters. Dashboards query Gold tables live, so a job rerun refreshes them.

**Screenshot the dashboard for LinkedIn.**

---

## Phase 12 · Publish your work (20 min)
1. Add screenshots to GitHub: create an `images/` folder in the repo, upload `job_graph.png` and `dashboard.png`, and add to README:
   `![Dashboard](images/dashboard.png)`
2. **LinkedIn → Profile → Add section → Projects**: name *TradeBooks Lakehouse*, link the GitHub repo, paste the summary from `LINKEDIN.md`.
3. Post the LinkedIn post from `LINKEDIN.md` with the dashboard screenshot.

---

## Troubleshooting
| Error | Fix |
|---|---|
| `SCHEMA_NOT_FOUND` / `Volume does not exist` | Run `00_setup` first |
| Bronze row counts doubled | You deleted checkpoints but not tables. Re-run `00_setup` with `reset = true`, then 01 → 05 |
| `Table or view not found: silver_...` | Run notebooks in order |
| `DIVIDE_BY_ZERO` / cast errors | Serverless uses strict (ANSI) SQL; the code uses `try_cast`/`nullif` for this — check you didn't edit those |
| Free Edition quota message | Wait for reset; the job is small (a few minutes total) |

## Interview talking points
- "Why Bronze/Silver/Gold?" — replayability, separation of raw vs trusted vs business-ready, and different consumers per layer.
- "How do you handle bad data?" — DQ rules, quarantine table with reasons, DQ run log with % clean per run.
- "How do you avoid duplicates on re-runs?" — Auto Loader checkpoints for ingestion + Delta MERGE for upserts.
- "Spark vs pandas?" — Spark for distributed transforms on growing raw data; pandas for small aggregated analysis and modeling.
