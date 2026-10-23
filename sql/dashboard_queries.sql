-- ============================================================
-- TradeBooks · AI/BI Dashboard datasets
-- Paste each query as a separate "Dataset" in a Databricks dashboard.
-- Replace workspace.tradebooks if you used a different catalog/schema.
-- ============================================================

-- 1. KPI tiles (counter widgets)
SELECT
  SUM(revenue_ttm)                                   AS revenue_ttm,
  ROUND(SUM(net_profit_ttm) / SUM(revenue_ttm) * 100, 1) AS net_margin_pct,
  SUM(ar_balance)                                    AS open_ar,
  ROUND(AVG(dso_days), 1)                            AS avg_dso_days
FROM workspace.tradebooks.gold_client_kpis;

-- 2. Monthly revenue vs net profit (line / combo chart)
SELECT month, client_name, revenue, net_profit, net_margin_pct
FROM workspace.tradebooks.gold_monthly_pnl
ORDER BY month;

-- 3. Revenue forecast (actual + forecast on one line chart)
SELECT month, client_name, revenue AS amount, 'Actual' AS series
FROM workspace.tradebooks.gold_monthly_pnl
UNION ALL
SELECT CAST(month AS DATE), client_name, forecast_revenue, 'Forecast'
FROM workspace.tradebooks.insight_revenue_forecast
ORDER BY month;

-- 4. A/R aging (stacked bar)
SELECT client_name, aging_bucket, SUM(balance_due) AS balance_due
FROM workspace.tradebooks.gold_ar_aging
GROUP BY ALL;

-- 5. Collections call list (table)
SELECT client_name, customer_name, open_invoices, ROUND(balance_due, 2) AS balance_due,
       max_days_past_due, risk_score, priority
FROM workspace.tradebooks.insight_collections_risk
WHERE priority = 'High'
ORDER BY risk_score DESC
LIMIT 25;

-- 6. Job-type margin vs 40% target (bar)
SELECT client_name, job_type, gross_margin_pct, price_increase_pct, annualized_profit_uplift, action
FROM workspace.tradebooks.insight_pricing
ORDER BY annualized_profit_uplift DESC;

-- 7. Lead source ROI (bar)
SELECT lead_source, SUM(jobs) AS jobs, SUM(revenue) AS revenue,
       ROUND(SUM(gross_profit) / SUM(revenue) * 100, 1) AS margin_pct
FROM workspace.tradebooks.gold_lead_source
GROUP BY lead_source
ORDER BY revenue DESC;

-- 8. Technician utilization heatmap
SELECT client_id, tech_id, month, utilization_pct, utilization_band
FROM workspace.tradebooks.gold_tech_utilization;

-- 9. Customer segments (pie / bar)
SELECT segment, COUNT(*) AS customers, ROUND(SUM(monetary), 0) AS revenue
FROM workspace.tradebooks.insight_customer_rfm
GROUP BY segment
ORDER BY revenue DESC;

-- 10. Data quality monitor (table) — shows the pipeline is trustworthy
SELECT entity, bronze_rows, duplicates_removed, quarantined, silver_rows, pct_clean, run_at
FROM workspace.tradebooks.dq_run_log
QUALIFY ROW_NUMBER() OVER (PARTITION BY entity ORDER BY run_at DESC) = 1;
