# Failure Handling

## If Gold export fails
- Use last successful export
- Mark dashboards as "Data delayed"

## If Bronze load fails
- Quarantine broken CSV
- Continue loading other tables

## If KPIs look illogical
- Trigger Gold validation
- Block Tableau refresh until resolved
