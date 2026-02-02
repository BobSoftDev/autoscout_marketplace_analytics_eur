# Data Flow Diagram

Python (synthetic generation)
→ Inbound CSVs
→ PySpark Ingestion
→ Bronze Delta Tables (raw, dates as STRING)
→ SQL Transformations
→ Silver Tables (typed, relational)
→ SQL Aggregations
→ Gold Tables (analytics ready)
→ Gold CSV Exports (single file per table)
→ Tableau & Streamlit
→ Executive Decisions
