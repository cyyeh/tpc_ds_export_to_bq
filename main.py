#!/usr/bin/env python3
from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

import duckdb
from dotenv import load_dotenv


def _env_default(name: str, fallback: str | None = None) -> str | None:
    v = os.getenv(name)
    return v if v not in (None, "") else fallback


def _quote_path_for_duckdb(p: Path) -> str:
    # DuckDB SQL string literal escaping: single quote doubled.
    return str(p).replace("'", "''")

def generate_tpcds_into_duckdb(db_path: Path, sf: int, overwrite: bool) -> None:
    print(f"Generating TPC-DS(SF={sf}) into DuckDB at {db_path}")
    db_path.parent.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(str(db_path))
    try:
        con.execute("INSTALL tpcds;")
        con.execute("LOAD tpcds;")

        if overwrite:
            print("Overwriting existing tables")
            tables = [r[0] for r in con.execute("SHOW TABLES;").fetchall()]
            for t in tables:
                con.execute(f'DROP TABLE IF EXISTS "{t}" CASCADE;')
            print("All tables dropped")

        con.execute(f"CALL dsdgen(sf = {sf});")
    finally:
        con.close()


def export_tables_to_parquet(db_path: Path, parquet_dir: Path, compression: str, overwrite: bool) -> list[str]:
    print(f"Exporting tables to Parquet at {parquet_dir}")
    parquet_dir.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(str(db_path), read_only=True)
    try:
        table_names = [r[0] for r in con.execute("SHOW TABLES;").fetchall()]
        if not table_names:
            raise RuntimeError("No tables found in DuckDB database. Did generation succeed?")

        for t in table_names:
            out_path = parquet_dir / f"{t}.parquet"
            if out_path.exists() and not overwrite:
                continue
            out_path_sql = _quote_path_for_duckdb(out_path)
            con.execute(
                f"""
                COPY (SELECT * FROM "{t}")
                TO '{out_path_sql}'
                (FORMAT PARQUET, COMPRESSION {compression});
                """
            )
        return table_names
    finally:
        con.close()


def ensure_bq_dataset(project_id: str, dataset_id: str, location: str) -> None:
    print(f"Ensuring BigQuery dataset {dataset_id} in project {project_id} at location {location}")
    from google.cloud import bigquery

    client = bigquery.Client(project=project_id)
    dataset_ref = bigquery.Dataset(f"{project_id}.{dataset_id}")
    dataset_ref.location = location
    try:
        client.create_dataset(dataset_ref)
    except Exception:
        return


def upload_and_load_parquet_to_bq(
    *,
    project_id: str,
    dataset_id: str,
    location: str,
    parquet_dir: Path,
    table_names: list[str],
    write_disposition: str,
) -> None:
    print(f"Uploading and loading Parquet to BigQuery at {project_id}.{dataset_id}")
    from google.cloud import bigquery

    client = bigquery.Client(project=project_id, location=location)
    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.PARQUET,
        write_disposition=write_disposition,
        autodetect=True,
    )

    for t in table_names:
        print(f"Uploading and loading table {t} to BigQuery at {project_id}.{dataset_id}")
        local_path = parquet_dir / f"{t}.parquet"
        if not local_path.exists():
            continue

        table_id = f"{project_id}.{dataset_id}.{t}"
        with local_path.open("rb") as f:
            job = client.load_table_from_file(f, table_id, job_config=job_config)
        job.result()


def parse_args(argv: list[str]) -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Generate TPC-DS SF10 into DuckDB, export per-table Parquet, and load into BigQuery dataset tpsds_sf10."
    )

    p.add_argument("--sf", type=int, default=int(_env_default("TPCDS_SF", "10") or "10"))
    p.add_argument(
        "--duckdb-path",
        type=Path,
        default=Path(_env_default("DUCKDB_PATH", "/tmp/tpcds_sf10.duckdb") or "/tmp/tpcds_sf10.duckdb"),
    )
    p.add_argument(
        "--parquet-dir",
        type=Path,
        default=Path(_env_default("PARQUET_DIR", "/tmp/tpcds_sf10_parquet") or "/tmp/tpcds_sf10_parquet"),
    )
    p.add_argument("--compression", default=_env_default("PARQUET_COMPRESSION", "ZSTD"))
    p.add_argument("--overwrite", action="store_true", default=True, help="Overwrite existing DuckDB tables and parquet outputs.")

    # BigQuery
    p.add_argument("--project-id", default=_env_default("GCP_PROJECT_ID"))
    p.add_argument("--dataset-id", default=_env_default("BQ_DATASET_ID", "tpsds_sf10"))
    p.add_argument("--location", default=_env_default("BQ_LOCATION", "US"))
    p.add_argument("--write-disposition", default=_env_default("BQ_WRITE_DISPOSITION", "WRITE_TRUNCATE"))
    p.add_argument(
        "--gcp-credentials",
        type=Path,
        default=_env_default("GOOGLE_APPLICATION_CREDENTIALS"),
        help="Path to a service account JSON key. If omitted, uses ADC.",
    )

    return p.parse_args(argv)


def main(argv: list[str]) -> int:
    load_dotenv()
    args = parse_args(argv)

    if not args.project_id:
        print("Missing --project-id (or GCP_PROJECT_ID in env).", file=sys.stderr)
        return 2

    args.compression = str(args.compression).upper()

    generate_tpcds_into_duckdb(args.duckdb_path, sf=args.sf, overwrite=args.overwrite)
    table_names = export_tables_to_parquet(
        args.duckdb_path, args.parquet_dir, compression=args.compression, overwrite=args.overwrite
    )

    ensure_bq_dataset(args.project_id, args.dataset_id, args.location)
    upload_and_load_parquet_to_bq(
        project_id=args.project_id,
        dataset_id=args.dataset_id,
        location=args.location,
        parquet_dir=args.parquet_dir,
        table_names=table_names,
        write_disposition=args.write_disposition,
    )

    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))

