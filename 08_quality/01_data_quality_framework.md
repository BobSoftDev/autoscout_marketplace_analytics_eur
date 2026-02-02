# Data Quality Framework — AutoScout Marketplace Analytics

## Purpose
Ensure that executive KPIs are:
- Accurate
- Stable over time
- Comparable across segments

## Quality dimensions
| Dimension | Description |
|--------|-------------|
| Completeness | Required fields exist and are populated |
| Validity | Values fall in allowed ranges |
| Consistency | Relationships between tables match business logic |
| Timeliness | Data is refreshed on schedule |
| Accuracy | KPIs behave logically over time |

## Quality ownership
- Data Engineering: Bronze & Silver correctness
- Analytics Engineering: Gold logic
- BI Owners: Tableau & Streamlit integrity
- Business: KPI interpretation
