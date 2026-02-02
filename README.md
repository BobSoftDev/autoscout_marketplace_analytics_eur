<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8" />
  <meta name="viewport" content="width=device-width,initial-scale=1" />
  <title>AutoScout Marketplace Conversion & Monetization Analytics (EUR)</title>
</head>
<body>
  <h1>AutoScout Marketplace Conversion &amp; Monetization Analytics (EUR)</h1>

  <p>
    This repository contains a production-style analytics platform for an AutoScout-type automotive digital marketplace.
    It provides an executive-grade, auditable answer to:
    <strong>which user, listing, and dealer behaviors drive conversion and revenue</strong>,
    and <strong>how to optimize the marketplace</strong> to increase successful vehicle transactions.
  </p>

  <h2>Business questions</h2>
  <ul>
    <li>How does each step of the Search &rarr; Contact &rarr; Sale funnel perform by segment and over time?</li>
    <li>Which listing attributes (price, photos, age, seller type, featured status) influence lead and sale probability?</li>
    <li>How do dealers differ in conversion, monetization, and sensitivity to pricing position?</li>
    <li>What is revenue per listing and revenue per lead?</li>
    <li>Where are the biggest MoM and YoY growth or leakage drivers?</li>
  </ul>

  <h2>Core KPIs (EUR)</h2>
  <ul>
    <li>Searches, Contacts (Leads), Sales</li>
    <li>Search&rarr;Contact rate, Contact&rarr;Sale rate, Search&rarr;Sale rate</li>
    <li>Total revenue (EUR)</li>
    <li>Revenue per lead (EUR)</li>
    <li>Revenue per listing (EUR)</li>
    <li>MoM and YoY (absolute and %)</li>
  </ul>

  <h2>Architecture overview</h2>
  <ol>
    <li><strong>Python</strong> generates realistic, correlated time-series marketplace datasets (CSV).</li>
    <li><strong>PySpark</strong> ingests CSVs from an inbound volume into <strong>Bronze Delta</strong> (dates stored as STRING).</li>
    <li><strong>SQL</strong> transforms Bronze &rarr; Silver (clean, typed, relational) &rarr; Gold (analytics-ready facts/dimensions/metrics).</li>
    <li><strong>Gold</strong> is exported as <strong>single CSV files</strong> (one file per Gold table).</li>
    <li><strong>Tableau</strong> and <strong>Streamlit</strong> both answer the same executive questions using the same Gold tables.</li>
    <li><strong>Executive PowerPoint</strong> summarizes drivers, leakage, and prioritized actions (McKinsey-style).</li>
  </ol>

  <h2>Repository structure (high level)</h2>
  <ul>
    <li><code>00_docs/</code> &mdash; Charter, metric dictionary, data contracts</li>
    <li><code>01_architecture/</code> &mdash; Data flow, ownership, model overview</li>
    <li><code>05_tableau/</code> &mdash; Tableau data sources, relationships, calculated fields, wireframes</li>
    <li><code>06_streamlit_app/</code> &mdash; Executive analytics app (local mode reads Gold CSV exports)</li>
    <li><code>07_ppt_executive/</code> &mdash; Board-ready storyline, methodology, recommendations</li>
  </ul>

  <h2>Gold tables (source of truth)</h2>
  <ul>
    <li><code>agg_funnel_month</code> &mdash; funnel volume + conversion by month and segment</li>
    <li><code>agg_listing_month</code> &mdash; listing performance drivers (photos, featured, price position)</li>
    <li><code>agg_dealer_month</code> &mdash; dealer conversion + monetization by month</li>
    <li><code>agg_revenue_stream_month</code> &mdash; revenue mix by stream over time</li>
    <li><code>kpi_growth_drivers_month</code> &mdash; executive KPIs with MoM/YoY deltas</li>
    <li><code>dim_date_month</code>, <code>dim_dealer</code>, <code>dim_listing</code> &mdash; conformed dimensions</li>
  </ul>

  <h2>How to run Streamlit (local Gold exports)</h2>
  <p>
    Place Gold export CSVs in a local folder and point the app to it using
    <code>GOLD_EXPORTS_DIR</code>.
  </p>

  <pre><code>cd 06_streamlit_app
# PowerShell example:
$env:GOLD_EXPORTS_DIR="C:\path\to\gold_exports_csv"
streamlit run app\app.py</code></pre>

  <h2>Tableau modeling notes</h2>
  <ul>
    <li>Use <strong>Relationships</strong> (logical layer), not physical joins, to avoid fan-out and inflated totals.</li>
    <li>Compute economics as <code>SUM(revenue) / SUM(denominator)</code>, not <code>AVG(revenue/denominator)</code>.</li>
  </ul>

  <h2>License</h2>
  <p>
    Internal / portfolio use. Replace synthetic data with production data and keep transformation logic unchanged.
  </p>
</body>
</html>
