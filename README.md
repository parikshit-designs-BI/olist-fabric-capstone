# Olist E-commerce Analytics — End-to-End Lakehouse on Microsoft Fabric

**Capstone project log & documentation**
Author: Parikshit Sharma · Workspace: `capstone-dev` (Fabric Trial, Central India) · Period: 28 Sep 2026 – 6 Oct 2026

---

## 1. Project goal

Upskill from Power BI developer (5 yrs) to end-to-end data platform ownership by building one complete project covering:

- Data lake / warehouse design (medallion architecture, star schema, SCD Type 2)
- Data pipeline & orchestration (ingestion, transformation, data quality, scheduling)
- Semantic model & Power BI report (Direct Lake, time intelligence, RLS)
- DevOps / portfolio (Git integration, README, demo)

**Business theme:** E-commerce sales & delivery analytics on the Brazilian marketplace **Olist** — revenue trend and YoY growth, category/state performance, delivery SLA (late deliveries, delivery days), customer experience (review scores), payment behaviour.

---

## 2. Architecture

```
Kaggle CSVs (9 files)
        │  manual upload
        ▼
Lakehouse  lh_capstone
 ├── Files/bronze/olist/*.csv          raw landing zone
 ├── Tables/bronze.*   (9 tables)      raw copy as string + audit cols      ← 01_bronze_ingest
 ├── Tables/silver.*   (9 + dq_log)    typed, deduped, standardised         ← 02_silver_clean
 └── Tables/gold.*     (7 tables)      star schema, SCD2 customer           ← 03_gold_build
        │
Data pipeline  pl_olist_daily   Bronze ──▶ Silver ──▶ Gold  (daily 06:00 IST)
        │
Semantic model  sm_olist  (Direct Lake)  relationships · measures · date table
        │
Power BI report  olist_capstone.pbix  (live connection)
```

**Stack:** Microsoft Fabric (Lakehouse, Notebooks/PySpark, Data Factory pipelines, Direct Lake), Delta Lake, Power BI Desktop, Power BI Modeling MCP (for model automation).

---

## 3. Dataset

Kaggle — *Brazilian E-Commerce Public Dataset by Olist* (2016–2018, ~99k orders).

| File | Grain | Original rows |
|---|---|---|
| olist_orders_dataset.csv | 1 row per order | 99,441 |
| olist_order_items_dataset.csv | 1 row per order line | 112,650 |
| olist_order_payments_dataset.csv | 1 row per payment | 103,886 |
| olist_order_reviews_dataset.csv | 1 row per review | 99,224 |
| olist_customers_dataset.csv | 1 row per order-customer | 99,441 |
| olist_sellers_dataset.csv | 1 row per seller | 3,095 |
| olist_products_dataset.csv | 1 row per product | 32,951 |
| olist_geolocation_dataset.csv | zip prefix → lat/lng | 1,000,163 |
| product_category_name_translation.csv | PT → EN category | 71 |

Known source quirks (kept intentionally, handled in silver): duplicate `review_id` across orders; 1M geolocation rows for ~19k zip prefixes; typo column names `product_name_lenght`; Portuguese category names; null delivery dates for undelivered orders; 2016 has only ~3 months of data.

**Replacement data (6 Oct 2026):** a synthetic 2021-01-01 → 2026-10-06 version of all 9 files was generated with identical schema (same headers, timestamp format, 5-digit zip prefixes, nulls, quirks) and realistic behaviour (YoY growth, November peak, 97% delivered, late deliveries lower review scores). See §9.

---

## 4. Layer design

### Bronze — raw copy
- Reads each CSV with `header=true`, `inferSchema=false` (everything stays **string**), `multiLine=true` (review text contains line breaks).
- Adds only two audit columns: `_source_file`, `_load_ts`.
- Writes `bronze.<table>` as Delta, mode overwrite.
- Rationale: nothing is changed, so any downstream bug can be fixed by re-running from bronze without touching the source.

### Silver — clean & standardised
Per table: proper types, trim, case standardisation, dedup on business key, translation join.

| Table | Business key | Notable transforms |
|---|---|---|
| customers | customer_id | zip → int, city initcap, state upper |
| sellers | seller_id | same as customers |
| products | product_id | PT→EN category via translation, `coalesce(..., 'unknown')`, dimensions → int |
| orders | order_id | 5 timestamp columns parsed, status lower |
| order_items | (order_id, order_item_id) | price/freight → decimal(12,2) |
| order_payments | (order_id, payment_sequential) | installments int, value decimal |
| order_reviews | **(review_id, order_id)** | composite key because review_id repeats |
| geolocation | zip_code_prefix | 1M rows → 19,015 (avg lat/lng per prefix) |

**Data quality log** — 8 checks run on every load and appended to `silver.dq_log` with `run_ts`:
`orders.order_id_null`, `orders.purchase_ts_null`, `orders.bad_status`, `order_items.orphan_order`, `order_items.orphan_product`, `order_items.negative_price`, `reviews.score_out_of_range`, `customers.zip_null`. All passed on both the original and synthetic data.

### Gold — star schema

| Table | Type | Key |
|---|---|---|
| dim_date | generated calendar | `date_key` (yyyymmdd int) |
| dim_product | SCD Type 1 | `product_key` = xxhash64(product_id) |
| dim_seller | SCD Type 1 (+ lat/lng from geolocation) | `seller_key` = xxhash64(seller_id) |
| dim_customer | **SCD Type 2** | `customer_key` sequential surrogate |
| fact_order_items | grain: order line | FKs + 3 date keys, price, freight, total, delivery_days, days_vs_estimate, is_late |
| fact_payments | grain: payment | customer_key, purchase_date_key, type, installments, value |
| fact_reviews | grain: review | customer_key, 2 date keys, score, has_comment, answer_days |

**SCD Type 2 logic (dim_customer)**
Tracked columns: `zip_code_prefix`, `city`, `state` → `row_hash` = SHA-256 of concatenated tracked values.
1. First run (table does not exist): full load, `valid_from = 1900-01-01`, `valid_to = 9999-12-31`, `is_current = true`.
2. Subsequent runs: compare source hash vs current-row hash → `changed`; source rows with no current row → `new`.
3. Delta `MERGE` closes changed rows: `valid_to = RUN_TS`, `is_current = false`.
4. Append new versions for `changed ∪ new` with `customer_key = max_key + row_number`, `valid_from = RUN_TS`.
5. Facts use a **point-in-time join**: `purchase_ts >= valid_from AND purchase_ts < valid_to`, so historical orders keep the customer version that was valid at purchase time.

**SCD2 test (5 Oct):** changed `customer_city` of customer `06b8999e2fba1a1fbc88172c00ba8bc7` from `franca` → `campinas` in the source CSV, re-ran 01 → 02 → 03. Output: `dim_customer: SCD2 run -> 1 changed, 0 new, 99442 total rows`; two rows for that customer (old `is_current=false`, new `true`).

---

## 5. Orchestration

**Data pipeline `pl_olist_daily`** — three Notebook activities chained on *On success*:
`Bronze (01_bronze_ingest)` → `Silver (02_silver_clean)` → `Gold (03_gold_build)`.
First run 5 Oct 2026: all three Succeeded, total ≈ 6 min (Bronze 1m45s, Silver 2m22s, Gold 2m6s). Scheduled daily 06:00 IST.

Lesson learned: running the three notebooks in parallel by hand caused silver/gold to read stale data (SCD2 showed `0 changed`). Sequential dependencies in the pipeline are what prevent this.

---

## 6. Semantic model `sm_olist` (Direct Lake)

Created from the Lakehouse with the 7 gold tables. Configured partly in the web model editor and partly through the Power BI Modeling MCP connector (XMLA endpoint `powerbi://api.powerbi.com/v1.0/myorg/capstone-dev`).

**Relationships (9, all many-to-one, single direction)**

| From | To | Note |
|---|---|---|
| fact_order_items[customer_key] | dim_customer | |
| fact_order_items[product_key] | dim_product | |
| fact_order_items[seller_key] | dim_seller | |
| fact_order_items[purchase_date_key] | dim_date | active |
| fact_order_items[delivered_date_key] | dim_date | **inactive** (for USERELATIONSHIP) |
| fact_payments[customer_key] | dim_customer | |
| fact_payments[purchase_date_key] | dim_date | |
| fact_reviews[customer_key] | dim_customer | |
| fact_reviews[purchase_date_key] | dim_date | |

Issue hit: a relationship accidentally created as `dim_customer → fact_reviews` (reversed) produced an *ambiguous paths* error when adding `fact_reviews → dim_date`. Fixed by deleting and recreating it in the correct direction.

**Model hygiene (via MCP)**
- `dim_date` marked as date table on `date`; `month_name` sorted by `month`, `day_name` by `day_of_week`
- All `*_key` columns, `row_hash`, `valid_from`, `valid_to` hidden; `is_current` visible
- Default summarization removed from numeric dimension/attribute columns (year, zip, review_score, installments, etc.)
- `dim_seller[lat]/[lng]` categorised Latitude/Longitude
- `_Measures` container table (calculated `{BLANK()}`, `Value` hidden); all measures moved there

**Measures**

| Folder | Measures |
|---|---|
| Sales | Revenue, Freight, Total Value, Orders, Items Sold, Avg Order Value |
| Delivery | Late Deliveries %, Avg Delivery Days |
| Time Intelligence | Revenue PY, Revenue YoY %, Orders PY, Orders YoY %, Avg Order Value PY, Avg Order Value YoY % |
| Reviews | Avg Review Score, Reviews |
| Payments | Payments, Avg Installments |
| Customers | Customers (`is_current = TRUE`) |
| Reference Labels | Revenue Ref Label, Orders Ref Label, Avg Order Value Ref Label, Revenue YoY Ref Label, Period Label, YoY Color |

Key DAX decisions:
- `*_PY = IF(ISFILTERED(dim_date), CALCULATE([X], SAMEPERIODLASTYEAR(dim_date[date])))` — without the guard, an unfiltered 2016–2019 calendar shifted one year still covered all data, giving YoY = 0.0%.
- Reference labels use `SWITCH(TRUE(), ISBLANK([X]) → "No data in selected period", ISBLANK([X PY]) → "Select a year to compare", ...)` with ▲/▼ arrows; `YoY Color` returns hex for conditional formatting (green / red / grey).
- `Period Label` shows "Year 2018", "2018-03", "06 Oct 2018 – 31 Dec 2019" or "All periods (…)" depending on the filter context.

Validated with DAX queries: Revenue 13.59M, Orders 98,666, Customers 99,441, Avg Review 4.09; 2018 vs 2017: ▲ 20.0% revenue, ▲ 20.6% orders, ▼ 0.5% AOV.

---

## 7. Report `olist_capstone.pbix`

Live connection to `sm_olist` (not Import — keeps Direct Lake). Planned pages:

1. **Executive** — 4 new-format cards (Revenue, Orders, Avg Order Value, Revenue YoY %) with reference labels; revenue by month line; top-10 categories bar; revenue by state map; year slicer (default 2018 / latest year).
2. **Delivery** — Late %, Avg Delivery Days, late % by state, delivery-days trend, late vs review score.
3. **Customers** — customers by state, new vs repeat (`customer_unique_id`), SCD2 demo table with `is_current` filter.

Status at time of writing: Executive card row built; remaining visuals pending.

Formatting notes: Indian digit grouping (`1,35,91,644`) came from Desktop regional settings → File → Options → Regional Settings → English (United States). Date slicer extended into 2019 because `dim_date` was generated to 2019-12-31 while data ends Sep 2018 → use a year slicer or trim the calendar.

---

## 8. Issues encountered & fixes (chronological)

| # | Issue | Cause | Fix |
|---|---|---|---|
| 1 | Notebook: `No default context found, please attach a lakehouse` | No lakehouse attached | Add data items → Existing → `lh_capstone` (Lakehouse, not SQL endpoint) |
| 2 | Two `lh_capstone` items in picker | Lakehouse + its SQL analytics endpoint share a name | Pick the Lakehouse item |
| 3 | Tried to create notebook as a Files subfolder | Files vs workspace items confusion | Notebooks are workspace items (New item → Notebook) |
| 4 | SQL analytics endpoint "not visible" | It is nested under the lakehouse in the workspace list / dropdown inside lakehouse | Open from list or lakehouse top-right switcher; or `%%sql` cells in notebook |
| 5 | `03_gold_build` cell was empty on reopen | Code not saved / duplicate notebook | Re-pasted code |
| 6 | SCD2 run showed `0 changed` | (a) notebooks run in parallel; (b) edited CSV never uploaded | Run sequentially; edit CSV in-place with pandas (`/lakehouse/default/Files/...`) |
| 7 | Ambiguous relationship path | Reversed relationship dim → fact | Delete & recreate fact → dim |
| 8 | YoY always 0.0% with no filter | SAMEPERIODLASTYEAR on full calendar | `ISFILTERED(dim_date)` guard |
| 9 | ▼100% label on empty period | Current blank, PY not | `ISBLANK` guard in labels and YoY % |
| 10 | MCP connected to wrong model (local work PBIX) | Server reconnected to last Desktop session | Always pass `connectionName = Fabric-capstone-dev-sm_olist` |
| 11 | MCP timeouts | Local MCP server unresponsive | Restart Claude Desktop / MCP server; updates are idempotent |

---

## 9. Synthetic 2021–2026 data (replacement set)

Generated 6 Oct 2026 to replace the 2016–2018 source files; same schema, same file names.

| File | Rows |
|---|---|
| orders / customers | 107,025 |
| order_items | 126,407 |
| order_payments | 110,763 |
| order_reviews | 105,472 |
| products | 32,000 |
| sellers | 3,100 |
| geolocation | 202,352 (14,983 unique prefixes) |
| category_translation | 70 |

Properties: purchases 2021-01-01 01:41 → 2026-10-06 22:30; monthly volume grows ~800 → ~2,200 with November ×1.8; ~4%/yr price drift; status mix 96.4% delivered / 1.6% shipped / rest canceled, unavailable, processing, invoiced, approved, created; recent orders skew to in-transit statuses; N/NE states deliver slower; ~14.5% late; late/undelivered orders get low review scores; 3.5% split payments (voucher + card); 0.4% shared `review_id`; 1.8% null categories; all referential checks = 0 orphans; all customer/seller zips exist in geolocation.

**Steps to switch Fabric to this data**
1. Delete the 9 files in `Files/bronze/olist/`, upload the new 9.
2. In `03_gold_build` change the calendar range to `to_date('2021-01-01'), to_date('2026-12-31')`.
3. Run once: `spark.sql("DROP TABLE IF EXISTS gold.dim_customer")` (fresh SCD2 initial load — all customers are new).
4. Run `pl_olist_daily`; refresh the PBIX.

---

## 10. Remaining work

- [ ] Finish Executive page visuals; build Delivery and Customers pages
- [ ] Publish report to `capstone-dev`
- [ ] RLS role (e.g. `State Manager`: `dim_customer[state] = "SP"`), test with *View as*
- [ ] Incremental load pattern for bronze/silver (watermark on `order_purchase_timestamp`) instead of full overwrite
- [ ] Pipeline failure alerting (Teams/email on activity failure)
- [ ] Git integration: Workspace settings → Git → GitHub repo; commit notebooks, pipeline, model (TMDL), report (PBIP)
- [ ] README with architecture diagram, SCD2 and pipeline screenshots; 5-minute walkthrough video
- [ ] Optional phase 2: swap transformation layer to dbt, or add Real-Time Intelligence stream

---

## 11. Artefacts

| Item | Location |
|---|---|
| `01_bronze_ingest.py` | notebook code — bronze load |
| `02_silver_clean.py` | notebook code — silver + DQ log |
| `03_gold_build.py` | notebook code — star schema + SCD2 |
| `gen_olist.py` | synthetic data generator (2021–2026) |
| `olist_dummy_2021_2026.zip` | 9 replacement CSVs |
| `sm_olist` | Fabric semantic model (Direct Lake) |
| `olist_capstone.pbix` | Power BI report (live) |

## 12. Interview talking points

- Why three layers, and what "trust" each layer carries
- Why bronze keeps everything as string
- Composite key on reviews; why geolocation is aggregated, not deduped
- Type 1 vs Type 2 dimensions; hash-based change detection; point-in-time fact join
- Why a pipeline (dependencies) instead of manual runs — with the real failure story
- Direct Lake vs Import vs DirectQuery; why live connection from Desktop
- `ISFILTERED` guard on time intelligence; reference-label pattern on cards
- Data quality log as a table, not just a print
