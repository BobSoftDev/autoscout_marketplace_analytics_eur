# KPI Snapshot Process

## Objective
Guarantee that MoM and YoY never change after they are published.

## Steps
1. At month-end, freeze Gold tables
2. Copy KPI rows into kpi_growth_drivers_month_snapshot
3. Lock Tableau to snapshot for prior months

This prevents historical KPIs from being restated.
