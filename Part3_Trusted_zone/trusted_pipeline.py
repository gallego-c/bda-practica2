import logging
import os
import shutil
import sys

from pyspark.sql import SparkSession

from trusted_config import TRUSTED_DUCKDB_PATH, build_run_id
from trusted_rules import clean_cardiovascular, clean_cdc_indicators, clean_cleveland
from trusted_storage import (
    ensure_dirs,
    export_formatted_tables_to_parquet,
    materialize_trusted_duckdb,
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
    require_linux_java_runtime()
    python_exec = sys.executable
    os.environ["PYSPARK_PYTHON"] = python_exec
    os.environ["PYSPARK_DRIVER_PYTHON"] = python_exec
    
    # Set default username for Hadoop to work in WSL2
    os.environ.setdefault("USER", "claudia")
    os.environ.setdefault("HADOOP_USER_NAME", "claudia")

    builder = (
        SparkSession.builder
        .appName("TrustedZone_DataQualityPipeline")
        .master(os.getenv("SPARK_MASTER", "local[*]"))
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.pyspark.python", python_exec)
        .config("spark.pyspark.driver.python", python_exec)
        .config("spark.driver.extraJavaOptions", "-Dcom.sun.jndi.ldap.connect.pool=false -Djavax.security.auth.useSubjectCredsOnly=false -Duser.name=claudia")
        .config("spark.executor.extraJavaOptions", "-Dcom.sun.jndi.ldap.connect.pool=false -Djavax.security.auth.useSubjectCredsOnly=false -Duser.name=claudia")
    )
    return builder.getOrCreate()


def require_linux_java_runtime() -> None:
    if sys.platform != "linux":
        raise RuntimeError("This project is supported only inside Linux/WSL.")

    java_home = os.environ.get("JAVA_HOME")
    if java_home and (os.path.exists(os.path.join(java_home, "bin", "java"))):
        return
    if shutil.which("java"):
        return

    raise RuntimeError(
        "Java was not found. Install a JDK in WSL with: "
        "sudo apt update && sudo apt install -y default-jdk"
    )


def main() -> None:
    ensure_dirs()
    run_id = build_run_id()
    log.info("Trusted Zone run_id=%s", run_id)

    spark = get_spark()
    try:
        exported = export_formatted_tables_to_parquet(run_id, log)

        cardio_raw = spark.read.parquet(str(exported["cardiovascular_disease"]))
        cdc_raw = spark.read.parquet(str(exported["heart_disease_health_indicators"]))
        cleveland_raw = spark.read.parquet(str(exported["heart_disease_cleveland"]))

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
