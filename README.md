# tpc-ds-export-to-bq

Generate **TPC-DS** data into a **DuckDB** file, export **one Parquet per table**, then create a **BigQuery** dataset and load each table from Parquet.

## Quick start

### 1) Install deps (Poetry)

```bash
poetry install
```

### 2) Configure env (recommended)

Copy `env.example` to `.env` and fill in values:

```bash
cp env.example .env
```

### 3) Run

For large SF10 data, **use GCS staging** (recommended):

```bash
poetry run python scripts/load_tpcds_sf10.py \
  --project-id YOUR_GCP_PROJECT \
  --gcs-bucket YOUR_GCS_BUCKET \
  --gcs-prefix tpsds_sf10
```

This will:

- Create/update DuckDB at `--duckdb-path` (default: `/tmp/tpcds_sf10.duckdb`)
- Export Parquet files to `--parquet-dir` (default: `/tmp/tpcds_sf10_parquet`)
- Create BigQuery dataset **`tpsds_sf10`** (configurable via `--dataset-id`)
- Load each table into BigQuery as `YOUR_GCP_PROJECT.tpsds_sf10.<table>`

## Notes

- DuckDB generates TPC-DS via `INSTALL tpcds; LOAD tpcds; CALL dsdgen(sf = 10);`
- If you omit `--gcs-bucket`, the script will try to stream each local Parquet to BigQuery, which is often impractical for SF10.

