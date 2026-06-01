import logging
import os
import sys
from pathlib import Path

from pyspark.sql import SparkSession

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from spark_runtime import configure_spark_runtime

from trusted_config import TRUSTED_DUCKDB_PATH, build_run_id
from trusted_rules import clean_cardiovascular, clean_cdc_indicators, clean_cleveland
from trusted_storage import (
    ensure_dirs,
    materialize_trusted_duckdb,
    read_formatted_tables,
    write_parquet_snapshot,
    write_quality_and_quarantine,
)


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
log = logging.getLogger(__name__)


def get_spark() -> SparkSession:
    java_options = configure_spark_runtime(PROJECT_ROOT)
    python_exec = sys.executable
    os.environ["PYSPARK_PYTHON"] = python_exec
    os.environ["PYSPARK_DRIVER_PYTHON"] = python_exec

    builder = (
        SparkSession.builder
        .appName("TrustedZone_DataQualityPipeline")
        .master(os.getenv("SPARK_MASTER", "local[*]"))
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.driver.host", "127.0.0.1")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .config("spark.sql.execution.arrow.pyspark.enabled", "true")
        .config("spark.sql.execution.arrow.pyspark.fallback.enabled", "true")
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


def main() -> None:
    ensure_dirs()
    run_id = build_run_id()
    log.info("Trusted Zone run_id=%s", run_id)

    spark = get_spark()
    try:
        formatted = read_formatted_tables(spark, log)
        cardio_raw = formatted["cardiovascular_disease"]
        cdc_raw = formatted["heart_disease_health_indicators"]
        cleveland_raw = formatted["heart_disease_cleveland"]

        quality_metrics: list[dict] = []
        quarantine_dfs = []

        cardio_clean, cardio_quarantine = clean_cardiovascular(cardio_raw, run_id, quality_metrics)
        cdc_clean, cdc_quarantine = clean_cdc_indicators(cdc_raw, run_id, quality_metrics)
        cleveland_clean, cleveland_quarantine = clean_cleveland(cleveland_raw, run_id, quality_metrics)

        quarantine_dfs.extend([cardio_quarantine, cdc_quarantine, cleveland_quarantine])

        write_parquet_snapshot(cardio_clean, "cardiovascular_disease", run_id, log)
        write_parquet_snapshot(cdc_clean, "heart_disease_health_indicators", run_id, log)
        write_parquet_snapshot(cleveland_clean, "heart_disease_cleveland", run_id, log)

        write_quality_and_quarantine(spark, quality_metrics, quarantine_dfs, run_id, log)
    finally:
        spark.stop()

    materialize_trusted_duckdb(run_id, log)
    log.info("Trusted Zone completed successfully.")
    log.info("Trusted DB: %s", TRUSTED_DUCKDB_PATH.resolve())


if __name__ == "__main__":
    main()
