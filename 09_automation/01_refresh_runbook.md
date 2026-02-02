# Refresh Runbook

## Daily
1. Generate or ingest raw CSVs
2. Load to Bronze (PySpark)
3. Run Silver SQL
4. Run Gold SQL
5. Export Gold CSVs

## Weekly
- Refresh Tableau extracts
- Rebuild Streamlit cache

## Monthly
- Archive Gold exports
- Snapshot KPIs for MoM and YoY reference
