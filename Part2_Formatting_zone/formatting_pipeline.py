import logging
import os
import re
import sys
from datetime import datetime, timezone
from pathlib import Path

import duckdb
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
log = logging.getLogger(__name__)


SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = SCRIPT_DIR.parent
LANDING_ZONE_ROOT = PROJECT_ROOT / "Part1_Landing_zone" / "landing_zone"
DUCKDB_PATH = SCRIPT_DIR / "formatted_zone" / "formatted.duckdb"
DUCKDB_PATH.parent.mkdir(parents=True, exist_ok=True)


def get_spark() -> SparkSession:
    configure_java_home()
    java_options = configure_hadoop_home()
    python_exec = sys.executable
    os.environ["PYSPARK_PYTHON"] = python_exec
    os.environ["PYSPARK_DRIVER_PYTHON"] = python_exec

    builder = (
        SparkSession.builder
        .appName("FormattedZone_StructuralFormattingPipeline")
        .master(os.getenv("SPARK_MASTER", "local[*]"))
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.sql.legacy.timeParserPolicy", "LEGACY")
        .config("spark.pyspark.python", python_exec)
        .config("spark.pyspark.driver.python", python_exec)
    )
    if java_options:
        builder = (
            builder
            .config("spark.driver.extraJavaOptions", java_options)
            .config("spark.executor.extraJavaOptions", java_options)
        )
    return builder.getOrCreate()


def configure_java_home() -> None:
    if os.environ.get("JAVA_HOME"):
        return
    try:
        import jdk4py
    except ImportError:
        return

    java_home = Path(jdk4py.JAVA_HOME)
    os.environ["JAVA_HOME"] = str(java_home)
    os.environ["PATH"] = str(java_home / "bin") + os.pathsep + os.environ.get("PATH", "")


def configure_hadoop_home() -> str:
    hadoop_home = PROJECT_ROOT / ".hadoop"
    hadoop_bin = hadoop_home / "bin"
    if not (hadoop_bin / "winutils.exe").exists():
        return ""

    hadoop_home_java = hadoop_home.as_posix()
    hadoop_bin_java = hadoop_bin.as_posix()
    os.environ["HADOOP_HOME"] = hadoop_home_java
    os.environ["PATH"] = str(hadoop_bin) + os.pathsep + os.environ.get("PATH", "")
    return f"-Djava.library.path={hadoop_bin_java} -Dhadoop.home.dir={hadoop_home_java}"


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def quote_identifier(identifier: str) -> str:
    return '"' + identifier.replace('"', '""') + '"'


def normalize_column_name(name: str) -> str:
    normalized = re.sub(r"[^0-9A-Za-z_]+", "_", name.strip())
    normalized = re.sub(r"_+", "_", normalized).strip("_")
    return normalized.lower()


def normalize_column_names(names: list[str]) -> list[str]:
    seen: dict[str, int] = {}
    normalized_names = []
    for raw_name in names:
        base_name = normalize_column_name(raw_name) or "column"
        count = seen.get(base_name, 0)
        seen[base_name] = count + 1
        normalized_names.append(base_name if count == 0 else f"{base_name}_{count + 1}")
    return normalized_names


def normalize_columns(df: DataFrame) -> DataFrame:
    return df.toDF(*normalize_column_names(df.columns))


def log_schema(df: DataFrame, label: str) -> None:
    log.info("\n%s\n%s schema\n%s", "=" * 60, label, "=" * 60)
    df.printSchema()


def find_latest_parquet(dataset_name: str) -> Path:
    base = LANDING_ZONE_ROOT / dataset_name
    ingest_dirs = sorted(base.glob("ingested_*"), reverse=True)
    if not ingest_dirs:
        raise FileNotFoundError(f"No landing ingestion found for '{dataset_name}' in {base}")

    latest = ingest_dirs[0]
    parquet_path = latest / f"{dataset_name}.parquet"
    if not parquet_path.exists():
        raise FileNotFoundError(f"Expected landing Parquet not found at {parquet_path}")

    log.info("[%s] Using landing snapshot: %s", dataset_name, latest.name)
    return parquet_path


def normalize_single_delimited_column(
    df: DataFrame,
    expected_columns: list[str],
    dataset_name: str,
) -> DataFrame:
    if len(df.columns) != 1:
        return df

    only_col = df.columns[0]
    for delimiter in (";", ","):
        header = [normalize_column_name(part) for part in only_col.split(delimiter)]
        if header == expected_columns:
            log.warning(
                "[%s] Detected single-column raw ingestion with delimiter '%s'; expanding it structurally.",
                dataset_name,
                delimiter,
            )
            split_col = F.split(F.col(only_col), delimiter)
            return df.select(
                *[
                    F.trim(split_col.getItem(index)).alias(column_name)
                    for index, column_name in enumerate(expected_columns)
                ]
            )

    return df


def stable_formatted_record_id(df: DataFrame, dataset_name: str) -> F.Column:
    raw_columns = [F.coalesce(F.col(col_name).cast(T.StringType()), F.lit("")) for col_name in df.columns]
    return F.sha2(F.concat_ws("|", F.lit(dataset_name), *raw_columns), 256)


def structurally_format_dataset(
    spark: SparkSession,
    dataset_name: str,
    expected_columns: list[str] | None = None,
) -> DataFrame:
    parquet_path = find_latest_parquet(dataset_name)
    df_raw = spark.read.parquet(str(parquet_path))
    if expected_columns:
        df_raw = normalize_single_delimited_column(df_raw, expected_columns, dataset_name)

    df_fmt = normalize_columns(df_raw)
    log_schema(df_fmt, f"{dataset_name} [FORMATTED STRUCTURE]")

    return (
        df_fmt
        .withColumn("_source_dataset", F.lit(dataset_name))
        .withColumn("_formatted_record_id", stable_formatted_record_id(df_fmt, dataset_name))
        .withColumn("_formatted_at_utc", F.lit(utc_now_iso()))
    )


def format_cardiovascular_disease(spark: SparkSession) -> DataFrame:
    return structurally_format_dataset(
        spark,
        "cardiovascular_disease",
        [
            "id",
            "age",
            "gender",
            "height",
            "weight",
            "ap_hi",
            "ap_lo",
            "cholesterol",
            "gluc",
            "smoke",
            "alco",
            "active",
            "cardio",
        ],
    )


def format_heart_disease_health_indicators(spark: SparkSession) -> DataFrame:
    return structurally_format_dataset(spark, "heart_disease_health_indicators")


def format_cleveland(spark: SparkSession) -> DataFrame:
    return structurally_format_dataset(spark, "heart_disease_cleveland")


def write_to_duckdb(df_pandas, table_name: str, con: duckdb.DuckDBPyConnection) -> None:
    temp_view = "_tmp_" + re.sub(r"[^0-9A-Za-z_]", "_", table_name)
    catalog_name = DUCKDB_PATH.stem
    con.register(temp_view, df_pandas)
    con.execute("CREATE SCHEMA IF NOT EXISTS formatted")
    con.execute(
        f"CREATE OR REPLACE TABLE {quote_identifier(catalog_name)}.formatted.{quote_identifier(table_name)} "
        f"AS SELECT * FROM {quote_identifier(temp_view)}"
    )
    con.unregister(temp_view)


def drop_obsolete_tables(
    con: duckdb.DuckDBPyConnection,
    schema_name: str,
    allowed_tables: list[str],
) -> None:
    catalog_name = DUCKDB_PATH.stem
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
                f"DROP TABLE IF EXISTS {quote_identifier(catalog_name)}.{quote_identifier(schema_name)}.{quote_identifier(table_name)}"
            )


def write_all_to_duckdb(
    df_cardio: DataFrame,
    df_cdc: DataFrame,
    df_cleveland: DataFrame,
) -> None:
    con = duckdb.connect(str(DUCKDB_PATH))
    try:
        tables = [
            ("cardiovascular_disease", df_cardio),
            ("heart_disease_health_indicators", df_cdc),
            ("heart_disease_cleveland", df_cleveland),
        ]
        drop_obsolete_tables(con, "formatted", [table_name for table_name, _ in tables])
        for table_name, df_spark in tables:
            write_to_duckdb(df_spark.toPandas(), table_name, con)

        summary = con.execute(
            """
            SELECT schema_name, table_name
            FROM duckdb_tables()
            WHERE schema_name = 'formatted'
            ORDER BY table_name
            """
        ).fetchdf()
        log.info("\nFormatted tables registered in DuckDB:\n%s", summary.to_string(index=False))
    finally:
        con.close()


def main() -> None:
    spark = get_spark()
    log.info("Spark version: %s", spark.version)
    try:
        df_cardio = format_cardiovascular_disease(spark)
        df_cdc = format_heart_disease_health_indicators(spark)
        df_cleveland = format_cleveland(spark)
        write_all_to_duckdb(df_cardio, df_cdc, df_cleveland)
    finally:
        spark.stop()

    log.info("Formatted Zone completed successfully.")
    log.info("DuckDB file: %s", DUCKDB_PATH.resolve())


if __name__ == "__main__":
    main()
