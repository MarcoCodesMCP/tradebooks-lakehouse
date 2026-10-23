# LinkedIn copy

## Projects section — description (≈ 2,000 char limit)
Built an end-to-end Databricks lakehouse for a bookkeeping + analytics service aimed at trade contractors (plumbing, electrical, HVAC, roofing, carpentry).

• Data engineering (PySpark): incremental ingestion with Auto Loader into Unity Catalog; Bronze → Silver → Gold medallion architecture on Delta Lake; data-quality rules with a quarantine table and run log; idempotent Delta MERGE upserts; 6-task Lakeflow Job defined as code with Databricks Asset Bundles.
• Data analytics (pandas): job-level profitability, monthly P&L, A/R aging and DSO, seasonality indexing, a trend × seasonal revenue forecast (13.3% backtest MAPE), RFM customer segmentation, pricing-gap analysis and collections risk scoring.
• Delivered in an AI/BI dashboard with KPI tiles, forecast, aging, and a prioritized collections call list.

Findings on the synthetic dataset: 12 of 15 job types under a 40% margin target, a ~$1.78M/yr pricing opportunity, and $0.96M of A/R over 60 days late.

Skills: Databricks · PySpark · Delta Lake · Unity Catalog · SQL · pandas · Data Modeling · Forecasting · Data Quality

## Post
Most small contractors know their revenue. Few know which jobs actually make money.

I built TradeBooks Lakehouse on Databricks to answer that — an end-to-end pipeline that takes messy bookkeeping exports and turns them into profitability, cash-flow, and collections insights.

🔧 Engineering (PySpark)
→ Auto Loader for incremental ingestion
→ Bronze / Silver / Gold medallion architecture on Delta Lake
→ Data-quality rules + quarantine (bad rows never silently disappear)
→ Delta MERGE so re-runs never duplicate data
→ Orchestrated as a 6-task Lakeflow Job

📊 Analytics (pandas)
→ Job-level profitability and monthly P&L
→ A/R aging, DSO, and a collections risk score
→ Revenue forecast with 13.3% backtest error
→ RFM customer segmentation

What the data showed (synthetic, 5 contractors, 21 months):
• 12 of 15 job types ran below a 40% margin target
• One pricing gap alone was worth ~$1.78M/yr
• 28% of customers drove 50% of revenue

Code + full walkthrough on GitHub: [link]

#Databricks #DataEngineering #DataAnalytics #PySpark #DeltaLake #BusinessIntelligence
