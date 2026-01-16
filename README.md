# tpc-ds-export-to-bq

Generate **TPC-DS** data into a **DuckDB** file, export **one Parquet per table**, then create a **BigQuery** dataset and load each table from Parquet.

## Quick start

### 1. Install deps (Poetry)

```bash
poetry install
```

### 2. Configure env (recommended)

Copy `.env.example` to `.env` and fill in values:

```bash
cp .env.example .env
```

### 3. Run

This will:

- Create/update DuckDB at `--duckdb-path` (default: `/tmp/tpcds_sf10.duckdb`)
- Export Parquet files to `--parquet-dir` (default: `/tmp/tpcds_sf10_parquet`)
- Create BigQuery dataset **`tpsds_sf10`** (configurable via `--dataset-id`)
- Load each table into BigQuery as `YOUR_GCP_PROJECT.tpsds_sf10.<table>`

## Notes

- DuckDB generates TPC-DS via `INSTALL tpcds; LOAD tpcds; CALL dsdgen(sf = 10);`
