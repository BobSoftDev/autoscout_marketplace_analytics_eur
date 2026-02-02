# Analytics Release Management

## Environments
- DEV — analysts experiment
- QA — KPI validation
- PROD — executive reporting

## Promotion rules
- Gold SQL must be identical across environments
- Only validated Gold tables can be released to PROD
- Tableau & Streamlit always point to PROD Gold exports
