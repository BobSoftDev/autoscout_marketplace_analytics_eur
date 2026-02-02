<!-- README.md (HTML version) -->

<h1>AutoScout Marketplace Conversion &amp; Monetization Analytics (EUR)</h1>

<p>
  This repository contains a production-style analytics platform for an AutoScout-type automotive marketplace.
  The goal is to explain <strong>where conversion and revenue come from</strong>, <strong>where the funnel leaks</strong>,
  and <strong>what to optimize</strong> to increase successful vehicle transactions and monetization.
</p>

<hr/>

<h2>Business Questions</h2>
<ul>
  <li><strong>Funnel performance:</strong> How does each step of the Search → Contact → Sale funnel perform by segment and over time?</li>
  <li><strong>Listing drivers:</strong> Which listing attributes (price, photos, vehicle age, seller type, featured status) most influence leads and sales?</li>
  <li><strong>Dealer drivers:</strong> How do dealers differ in conversion, monetization, and elasticity?</li>
  <li><strong>Unit economics:</strong> What is revenue per listing and revenue per lead?</li>
  <li><strong>Growth drivers:</strong> Where are the biggest MoM and YoY growth or leakage drivers?</li>
</ul>

<hr/>

<h2>Core KPIs (EUR)</h2>
<ul>
  <li>Searches, Contacts (Leads), Sales</li>
  <li>Search→Contact Rate = Contacts / Searches</li>
  <li>Contact→Sale Rate = Sales / Contacts</li>
  <li>Search→Sale Rate = Sales / Searches</li>
  <li>Total Revenue (EUR)</li>
  <li>Revenue per Listing (EUR) = Revenue / Active Listings</li>
  <li>Revenue per Lead (EUR) = Revenue / Contacts</li>
  <li>MoM and YoY changes for executive comparability</li>
</ul>

<hr/>

<h2>Architecture</h2>
<p><strong>Enterprise-style Lakehouse pipeline</strong> with strict separation of responsibilities:</p>
<ul>
  <li><strong>Python</strong> — Synthetic data generation (correlated, time-series consistent)</li>
  <li><strong>PySpark</strong> — Ingestion from inbound CSV to Bronze Delta (dates stored as STRING)</li>
  <li><strong>SQL</strong> — Silver &amp; Gold transformations (no COPY INTO path literals; all identifiers backticked)</li>
  <li><strong>Gold exports</strong> — Single CSV file per Gold table</li>
  <li><strong>Tableau</strong> — Primary BI layer (one business question per page)</li>
  <li><strong>Streamlit</strong> — Executive analytics app aligned to Tableau questions</li>
  <li><strong>PowerPoint</strong> — McKinsey-style executive deck with recommendations and speaker notes</li>
</ul>

<hr/>

<h2>Gold Layer Tables (Source of Truth)</h2>
<ul>
  <li><code>agg_funnel_month</code> — Funnel volume + conversion rates by month and segment</li>
  <li><code>agg_listing_month</code> — Listing performance (featured, photos, price position) by month</li>
  <li><code>agg_dealer_month</code> — Dealer performance + unit economics by month</li>
  <li><code>agg_revenue_stream_month</code> — Revenue mix by stream and month</li>
  <li><code>kpi_growth_drivers_month</code> — KPI driver table with MoM and YoY changes</li>
  <li><code>dim_date_month</code>, <code>dim_listing</code>, <code>dim_dealer</code> — Conformed dimensions</li>
</ul>

<hr/>

<h2>Repository Structure (Key Folders)</h2>
<ul>
  <li><code>00_docs/</code> — Charter, metric definitions, operational notes</li>
  <li><code>01_architecture/</code> — Architecture narrative, data model, governance</li>
  <li><code>05_tableau/</code> — Data contracts, relationships, calculated fields, wireframes</li>
  <li><code>06_streamlit_app/</code> — Executive app reading Gold CSV exports from local disk</li>
  <li><code>07_ppt_executive/</code> — McKinsey-style deck storyline + recommendations</li>
</ul>

<hr/>

<h2>How to Run (Streamlit, Local Gold Exports)</h2>
<p>
  Place your Gold CSV exports in a local folder and set <code>GOLD_EXPORTS_DIR</code> to that folder.
  Example (Windows PowerShell):
</p>

<pre><code>$env:GOLD_EXPORTS_DIR="C:\path\to\gold_exports_csv"
cd 06_streamlit_app
streamlit run app\app.py</code></pre>

<p>
  Required files:
</p>
<ul>
  <li><code>agg_funnel_month.csv</code></li>
  <li><code>agg_listing_month.csv</code></li>
  <li><code>agg_dealer_month.csv</code></li>
  <li><code>agg_revenue_stream_month.csv</code></li>
  <li><code>kpi_growth_drivers_month.csv</code></li>
</ul>

<hr/>

<h2>Tableau Modeling Guidance</h2>
<ul>
  <li>Use <strong>Relationships</strong> (logical layer), avoid physical joins for fact tables</li>
  <li>Prefer separate data sources per theme: Funnel, Listing, Dealer, Revenue, KPI Drivers</li>
  <li>Core calculated fields should be <strong>SUM/SUM</strong> (avoid AVG of ratios)</li>
</ul>

<hr/>

<h2>Notes</h2>
<ul>
  <li>All values are in <strong>EUR</strong>.</li>
  <li>The initial dataset is synthetic; production onboarding replaces sources without changing logic.</li>
  <li>Causal claims require experimentation; this system provides directionally reliable diagnostics.</li>
</ul>
