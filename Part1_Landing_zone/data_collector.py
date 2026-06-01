"""
Landing Zone data collector for the BDA project.

This stage downloads the three agreed Kaggle datasets and stores raw Parquet
snapshots without business transformations:

- Sulianova cardiovascular disease dataset
- CDC heart disease health indicators dataset
- Cleveland heart disease dataset
"""

import logging
import os
import shutil
import sys
from datetime import datetime
from pathlib import Path

import kaggle
from pyspark.sql import SparkSession


SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = SCRIPT_DIR.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from spark_runtime import configure_spark_runtime


LANDING_ZONE_ROOT = SCRIPT_DIR / "landing_zone"

DATASETS = [
    ("sulianova/cardiovascular-disease-dataset", "cardiovascular_disease"),
    ("alexteboul/heart-disease-health-indicators-dataset", "heart_disease_health_indicators"),
    ("cherngs/heart-disease-cleveland-uci", "heart_disease_cleveland"),
]


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
        .appName("LandingZone_DataCollector")
        .master(os.getenv("SPARK_MASTER", "local[*]"))
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.driver.host", "127.0.0.1")
        .config("spark.driver.bindAddress", "127.0.0.1")
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


def download_kaggle_dataset(kaggle_id: str, dest_dir: Path) -> Path:
    dest_dir.mkdir(parents=True, exist_ok=True)
    log.info("Downloading dataset '%s' -> %s", kaggle_id, dest_dir)
    kaggle.api.dataset_download_files(
        dataset=kaggle_id,
        path=str(dest_dir),
        unzip=True,
        quiet=False,
    )
    return dest_dir


def find_csv_files(directory: Path) -> list[Path]:
    csvs = sorted(directory.rglob("*.csv"))
    if not csvs:
        raise FileNotFoundError(f"No CSV files found under {directory}")
    log.info("CSV files found: %s", [csv.name for csv in csvs])
    return csvs


def convert_csv_to_parquet_raw(
    spark: SparkSession,
    csv_paths: list[Path],
    output_parquet_path: Path,
) -> None:
    output_parquet_path.parent.mkdir(parents=True, exist_ok=True)
    df = (
        spark.read
        .option("header", "true")
        .option("inferSchema", "true")
        .option("nullValue", "")
        .option("mode", "PERMISSIVE")
        .csv([str(path) for path in csv_paths])
    )

    log.info("Loaded raw dataset: %s rows x %s columns", df.count(), len(df.columns))
    log.info("Original columns: %s", df.columns)

    (
        df.coalesce(1)
        .write
        .mode("overwrite")
        .parquet(str(output_parquet_path))
    )
    log.info("Raw Parquet snapshot written to %s", output_parquet_path)


def collect(dataset_name: str, kaggle_id: str, spark: SparkSession) -> None:
    run_ts = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    ingest_dir = LANDING_ZONE_ROOT / dataset_name / f"ingested_{run_ts}"
    tmp_download_dir = ingest_dir / "_tmp_download"

    log.info("=" * 72)
    log.info("STARTING LANDING INGESTION: %s [%s]", dataset_name, run_ts)
    log.info("=" * 72)

    try:
        download_kaggle_dataset(kaggle_id, tmp_download_dir)
        csv_files = find_csv_files(tmp_download_dir)
        convert_csv_to_parquet_raw(spark, csv_files, ingest_dir / f"{dataset_name}.parquet")
    except Exception:
        log.exception("Landing ingestion failed for '%s'", dataset_name)
        raise
    finally:
        if tmp_download_dir.exists():
            shutil.rmtree(tmp_download_dir)
            log.info("Removed temporary download directory: %s", tmp_download_dir)


def main() -> None:
    log.info("### LANDING ZONE DATA COLLECTOR ###")
    log.info("Datasets configured: %s", [dataset_id for dataset_id, _ in DATASETS])

    spark = get_spark()
    failed: list[str] = []
    try:
        for kaggle_id, dataset_name in DATASETS:
            try:
                collect(dataset_name, kaggle_id, spark)
            except Exception:
                failed.append(dataset_name)
    finally:
        spark.stop()

    if failed:
        log.error("Datasets with ingestion errors: %s", failed)
        sys.exit(1)

    log.info("### Landing ingestion finished successfully ###")


if __name__ == "__main__":
    main()
