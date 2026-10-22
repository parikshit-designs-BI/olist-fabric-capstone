# Notebooks

Run order (enforced by `pipeline/pl_olist_daily`):

| # | Notebook | Reads | Writes |
|---|---|---|---|
| 01 | `01_bronze_ingest` | `Files/bronze/olist/*.csv` | `bronze.*` (9 tables, all string + audit cols) |
| 02 | `02_silver_clean` | `bronze.*` | `silver.*` (typed, deduped) + `silver.dq_log` |
| 03 | `03_gold_build` | `silver.*` | `gold.dim_*`, `gold.fact_*` (star schema, SCD2 on dim_customer) |

All notebooks expect `lh_capstone` attached as the default lakehouse.
`.py` files are plain-text copies of the same code for easy diffing.
Note: the Fabric copy of `03_gold_build` has the calendar range set to 2021-01-01 → 2026-12-31.
