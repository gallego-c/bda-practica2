from pathlib import Path

import duckdb
import pandas as pd
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import types as T

from trusted_config import (
    FORMATTED_DUCKDB_PATH,
    STAGING_ROOT,
    TRUSTED_DUCKDB_PATH,
    TRUSTED_PARQUET_ROOT,
)


TRUSTED_TABLES = [
    "cardiovascular_disease",
    "heart_disease_health_indicators",
    "heart_disease_cleveland",
]


def ensure_dirs() -> None:
    TRUSTED_PARQUET_ROOT.mkdir(parents=True, exist_ok=True)
    STAGING_ROOT.mkdir(parents=True, exist_ok=True)


def export_formatted_tables_to_parquet(run_id: str, log) -> dict[str, Path]:
    if not FORMATTED_DUCKDB_PATH.exists():
        raise FileNotFoundError(f"Formatted DB not found at {FORMATTED_DUCKDB_PATH}")

    export_dir = STAGING_ROOT / run_id
    export_dir.mkdir(parents=True, exist_ok=True)

    con = duckdb.connect(str(FORMATTED_DUCKDB_PATH))
    try:
        catalog_name = FORMATTED_DUCKDB_PATH.stem
        exports: dict[str, Path] = {}
        for table_name in TRUSTED_TABLES:
            out_path = export_dir / f"{table_name}.parquet"
            fq_table = f"{catalog_name}.formatted.{table_name}"
            log.info("Exporting %s -> %s", fq_table, out_path)
            con.execute(
                f"COPY (SELECT * FROM {fq_table}) TO '{out_path.as_posix()}' (FORMAT PARQUET)"
            )
            exports[table_name] = out_path
        return exports
    finally:
        con.close()


def write_parquet_snapshot(df: DataFrame, table_name: str, run_id: str, log) -> Path:
    out_dir = TRUSTED_PARQUET_ROOT / table_name / run_id
    out_dir.mkdir(parents=True, exist_ok=True)
    log.info("Writing trusted snapshot %s -> %s", table_name, out_dir)
    df.write.mode("overwrite").parquet(str(out_dir))
    return out_dir


def write_quality_and_quarantine(
    spark: SparkSession,
    quality_metrics: list[dict],
    quarantine_dfs: list[DataFrame],
    run_id: str,
    log,
) -> tuple[Path, Path]:
    quarantine_schema = T.StructType([
        T.StructField("run_id", T.StringType(), False),
        T.StructField("dataset", T.StringType(), False),
        T.StructField("record_key", T.StringType(), False),
        T.StructField("quarantine_reason", T.StringType(), False),
        T.StructField("quarantined_at_utc", T.StringType(), False),
    ])

    quality_path = TRUSTED_PARQUET_ROOT / "quality_report" / run_id
    quality_path.mkdir(parents=True, exist_ok=True)
    quality_file = quality_path / "quality_report.parquet"
    log.info("Writing trusted snapshot quality_report -> %s", quality_file)
    pd.DataFrame(
        quality_metrics,
        columns=[
            "run_id",
            "dataset",
            "rule_name",
            "total_rows",
            "violations",
            "violation_pct",
            "action_applied",
            "measured_at_utc",
        ],
    ).to_parquet(quality_file, index=False)

    if quarantine_dfs:
        quarantine_df = quarantine_dfs[0]
        for qdf in quarantine_dfs[1:]:
            quarantine_df = quarantine_df.unionByName(qdf)
    else:
        quarantine_df = spark.createDataFrame([], schema=quarantine_schema)

    quarantine_path = write_parquet_snapshot(quarantine_df, "quarantine", run_id, log)
    return quality_path, quarantine_path


def drop_obsolete_tables(
    con: duckdb.DuckDBPyConnection,
    catalog_name: str,
    schema_name: str,
    allowed_tables: list[str],
) -> None:
    existing_tables = con.execute(
        """
        SELECT table_name
        FROM duckdb_tables()
        WHERE schema_name = ?
        """,
        [schema_name],
    ).fetchall()

    for (table_name,) in existing_tables:
        if table_name not in allowed_tables:
            con.execute(
                f"DROP TABLE IF EXISTS {catalog_name}.{schema_name}.{table_name}"
            )


def materialize_trusted_duckdb(run_id: str, log) -> None:
    TRUSTED_DUCKDB_PATH.parent.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(str(TRUSTED_DUCKDB_PATH))
    try:
        catalog_name = TRUSTED_DUCKDB_PATH.stem
        con.execute("CREATE SCHEMA IF NOT EXISTS trusted")
        con.execute("CREATE SCHEMA IF NOT EXISTS metadata")
        drop_obsolete_tables(con, catalog_name, "trusted", TRUSTED_TABLES)
        drop_obsolete_tables(con, catalog_name, "metadata", ["quality_report", "quarantine"])

        for table_name in TRUSTED_TABLES:
            src = (TRUSTED_PARQUET_ROOT / table_name / run_id / "*.parquet").as_posix()
            con.execute(
                f"CREATE OR REPLACE TABLE {catalog_name}.trusted.{table_name} AS SELECT * FROM read_parquet('{src}')"
            )

        q_src = (TRUSTED_PARQUET_ROOT / "quality_report" / run_id / "*.parquet").as_posix()
        quarantine_src = (TRUSTED_PARQUET_ROOT / "quarantine" / run_id / "*.parquet").as_posix()
        con.execute(
            f"CREATE OR REPLACE TABLE {catalog_name}.metadata.quality_report AS SELECT * FROM read_parquet('{q_src}')"
        )
        con.execute(
            f"CREATE OR REPLACE TABLE {catalog_name}.metadata.quarantine AS SELECT * FROM read_parquet('{quarantine_src}')"
        )

        counts = con.execute(
            """
            SELECT 'trusted.cardiovascular_disease' AS table_name, COUNT(*) AS n FROM trusted.trusted.cardiovascular_disease
            UNION ALL
            SELECT 'trusted.heart_disease_health_indicators', COUNT(*) FROM trusted.trusted.heart_disease_health_indicators
            UNION ALL
            SELECT 'trusted.heart_disease_cleveland', COUNT(*) FROM trusted.trusted.heart_disease_cleveland
            UNION ALL
            SELECT 'metadata.quality_report', COUNT(*) FROM trusted.metadata.quality_report
            UNION ALL
            SELECT 'metadata.quarantine', COUNT(*) FROM trusted.metadata.quarantine
            """
        ).fetchall()

        for table_name, n_rows in counts:
            log.info("Trusted materialized %s: %s rows", table_name, f"{n_rows:,}")
    finally:
        con.close()
