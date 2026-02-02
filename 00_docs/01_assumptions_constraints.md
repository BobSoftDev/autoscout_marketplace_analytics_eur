# Assumptions & Constraints — AutoScout Marketplace Analytics (EUR)

This document defines the non-negotiable analytical rules for this system.
All BI tools, models, and decisions must conform to these constraints.

---

## 1. Financial & Time Rules
| Rule | Description |
|------|-------------|
| Currency | All financial values are in EUR |
| Time grain | Monthly is the executive decision grain |
| Comparisons | All KPIs must support MoM and YoY |
| Month key | Stored as YYYY-MM |

---

## 2. Data Architecture Rules
| Rule | Description |
|------|-------------|
| Bronze | Raw data, dates stored as STRING |
| Silver | Cleaned, typed, relationalized |
| Gold | Only layer allowed for BI and executive reporting |
| Gold exports | Exactly one CSV per Gold table |
| SQL | Used only for Silver & Gold transformations |
| PySpark | Used only for ingestion and file handling |

---

## 3. BI & KPI Rules
| Rule | Description |
|------|-------------|
| Single source of truth | Gold tables only |
| KPI calculation | Never recalculated in Tableau or Streamlit |
| Aggregation logic | SUM / SUM (never AVG of ratios) |
| Relationships | Tableau must use Relationships, not physical joins |

---

## 4. Marketplace Modeling Assumptions
| Assumption | Meaning |
|-----------|---------|
| Searches | Proxy for buyer demand |
| Contacts | Proxy for lead generation |
| Sales | Proxy for completed transactions |
| Price position | Proxy for pricing competitiveness |
| Photo bucket | Proxy for listing quality |
| Dealer tier | Proxy for professionalism and service level |

---

## 5. What This System Does NOT Assume
- That correlation implies causation  
- That all dealers behave the same  
- That price is the only conversion driver  
- That traffic growth equals revenue growth  

---

## 6. Change Control
Any change to:
- KPI formulas  
- Funnel definitions  
- Revenue attribution  

must be approved by:
- Analytics Engineering
- Finance
- Executive sponsor

and documented in this file.

---
