import logging
import os
import re
import sys
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
    return (
        SparkSession.builder
        .appName("FormattedZone_DataFormattingPipeline")
        .master(os.getenv("SPARK_MASTER", "local[*]"))
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.sql.legacy.timeParserPolicy", "LEGACY")
        .getOrCreate()
    )


def quote_identifier(identifier: str) -> str:
    return '"' + identifier.replace('"', '""') + '"'


def normalize_column_name(name: str) -> str:
    normalized = re.sub(r"[^0-9A-Za-z_]+", "_", name.strip())
    normalized = re.sub(r"_+", "_", normalized).strip("_")
    return normalized.lower()


def normalize_columns(df: DataFrame) -> DataFrame:
    return df.toDF(*[normalize_column_name(col_name) for col_name in df.columns])


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


def parse_binary(column: F.Column) -> F.Column:
    text = F.lower(F.trim(column.cast(T.StringType())))
    return (
        F.when(text.isin("1", "1.0", "true", "yes", "y"), F.lit(True))
        .when(text.isin("0", "0.0", "false", "no", "n"), F.lit(False))
        .otherwise(F.lit(None).cast(T.BooleanType()))
    )


def parse_binary_or_positive(column: F.Column) -> F.Column:
    text = F.lower(F.trim(column.cast(T.StringType())))
    return (
        F.when(text.rlike(r"^-?\d+(\.\d+)?$") & (column.cast(T.DoubleType()) > 0), F.lit(True))
        .when(text.rlike(r"^-?\d+(\.\d+)?$") & (column.cast(T.DoubleType()) == 0), F.lit(False))
        .when(text.isin("true", "yes", "y"), F.lit(True))
        .when(text.isin("false", "no", "n"), F.lit(False))
        .otherwise(F.lit(None).cast(T.BooleanType()))
    )


def parse_gender(column: F.Column, female_tokens: list[str], male_tokens: list[str]) -> F.Column:
    text = F.lower(F.trim(column.cast(T.StringType())))
    return (
        F.when(text.isin(*female_tokens), F.lit("female"))
        .when(text.isin(*male_tokens), F.lit("male"))
        .otherwise(F.lit(None).cast(T.StringType()))
    )


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
                "[%s] Detected single-column raw ingestion with delimiter '%s'; expanding it.",
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


def age_group_code_from_years(column: F.Column) -> F.Column:
    return (
        F.when(column.isNull(), F.lit(None).cast(T.IntegerType()))
        .when(column < 25, F.lit(1))
        .when(column < 30, F.lit(2))
        .when(column < 35, F.lit(3))
        .when(column < 40, F.lit(4))
        .when(column < 45, F.lit(5))
        .when(column < 50, F.lit(6))
        .when(column < 55, F.lit(7))
        .when(column < 60, F.lit(8))
        .when(column < 65, F.lit(9))
        .when(column < 70, F.lit(10))
        .when(column < 75, F.lit(11))
        .when(column < 80, F.lit(12))
        .otherwise(F.lit(13))
    )


def cdc_age_code_from_column(column: F.Column) -> F.Column:
    text = F.lower(F.trim(column.cast(T.StringType())))
    return (
        F.when(text.rlike(r"^\d+(\.0+)?$"), column.cast(T.IntegerType()))
        .when(text == "18 to 24", F.lit(1))
        .when(text == "25 to 29", F.lit(2))
        .when(text == "30 to 34", F.lit(3))
        .when(text == "35 to 39", F.lit(4))
        .when(text == "40 to 44", F.lit(5))
        .when(text == "45 to 49", F.lit(6))
        .when(text == "50 to 54", F.lit(7))
        .when(text == "55 to 59", F.lit(8))
        .when(text == "60 to 64", F.lit(9))
        .when(text == "65 to 69", F.lit(10))
        .when(text == "70 to 74", F.lit(11))
        .when(text == "75 to 79", F.lit(12))
        .when(text == "80 or older", F.lit(13))
        .otherwise(F.lit(None).cast(T.IntegerType()))
    )


def cdc_age_midpoint_from_code(column: F.Column) -> F.Column:
    return (
        F.when(column == 1, F.lit(21.0))
        .when(column == 2, F.lit(27.0))
        .when(column == 3, F.lit(32.0))
        .when(column == 4, F.lit(37.0))
        .when(column == 5, F.lit(42.0))
        .when(column == 6, F.lit(47.0))
        .when(column == 7, F.lit(52.0))
        .when(column == 8, F.lit(57.0))
        .when(column == 9, F.lit(62.0))
        .when(column == 10, F.lit(67.0))
        .when(column == 11, F.lit(72.0))
        .when(column == 12, F.lit(77.0))
        .when(column == 13, F.lit(82.0))
        .otherwise(F.lit(None).cast(T.DoubleType()))
    )


def stable_record_key(df: DataFrame, prefix: str) -> F.Column:
    raw_columns = [F.coalesce(F.col(col_name).cast(T.StringType()), F.lit("")) for col_name in df.columns]
    return F.sha2(F.concat_ws("|", F.lit(prefix), *raw_columns), 256)


def format_cardiovascular_disease(spark: SparkSession) -> DataFrame:
    dataset_name = "cardiovascular_disease"
    parquet_path = find_latest_parquet(dataset_name)
    df_raw = spark.read.parquet(str(parquet_path))
    expected_columns = [
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
    ]
    df_raw = normalize_single_delimited_column(df_raw, expected_columns, dataset_name)
    df_raw = normalize_columns(df_raw)
    log_schema(df_raw, f"{dataset_name} [RAW]")

    df_fmt = (
        df_raw
        .withColumn("patient_id", F.col("id").cast(T.StringType()))
        .withColumn("age_years", F.floor(F.col("age").cast(T.DoubleType()) / F.lit(365.25)).cast(T.IntegerType()))
        .withColumn("age_group_code", age_group_code_from_years(F.col("age_years")))
        .withColumn(
            "gender",
            parse_gender(F.col("gender"), ["1", "1.0", "female", "f"], ["2", "2.0", "male", "m"]),
        )
        .withColumn("height_cm", F.col("height").cast(T.DoubleType()))
        .withColumn("weight_kg", F.col("weight").cast(T.DoubleType()))
        .withColumn(
            "bmi",
            F.when(
                F.col("height").cast(T.DoubleType()) > 0,
                F.col("weight").cast(T.DoubleType()) / F.pow(F.col("height").cast(T.DoubleType()) / 100.0, 2),
            ).otherwise(F.lit(None).cast(T.DoubleType())),
        )
        .withColumn("systolic_bp", F.col("ap_hi").cast(T.DoubleType()))
        .withColumn("diastolic_bp", F.col("ap_lo").cast(T.DoubleType()))
        .withColumn("cholesterol_level", F.col("cholesterol").cast(T.IntegerType()))
        .withColumn("glucose_level", F.col("gluc").cast(T.IntegerType()))
        .withColumn("is_smoker", parse_binary(F.col("smoke")))
        .withColumn("drinks_alcohol", parse_binary(F.col("alco")))
        .withColumn("is_active", parse_binary(F.col("active")))
        .withColumn("has_cardiovascular_disease", parse_binary(F.col("cardio")))
        .select(
            "patient_id",
            "age_years",
            "age_group_code",
            "gender",
            "height_cm",
            "weight_kg",
            "bmi",
            "systolic_bp",
            "diastolic_bp",
            "cholesterol_level",
            "glucose_level",
            "is_smoker",
            "drinks_alcohol",
            "is_active",
            "has_cardiovascular_disease",
        )
    )
    log_schema(df_fmt, f"{dataset_name} [FORMATTED]")
    return df_fmt


def format_heart_disease_health_indicators(spark: SparkSession) -> DataFrame:
    dataset_name = "heart_disease_health_indicators"
    parquet_path = find_latest_parquet(dataset_name)
    df_raw = normalize_columns(spark.read.parquet(str(parquet_path)))
    log_schema(df_raw, f"{dataset_name} [RAW]")

    df_fmt = (
        df_raw
        .withColumn("respondent_id", stable_record_key(df_raw, "cdc"))
        .withColumn("age_group_code", cdc_age_code_from_column(F.col("age")))
        .withColumn("age_years_proxy", cdc_age_midpoint_from_code(F.col("age_group_code")))
        .withColumn(
            "gender",
            parse_gender(F.col("sex"), ["0", "0.0", "female", "f"], ["1", "1.0", "male", "m"]),
        )
        .withColumn("bmi", F.col("bmi").cast(T.DoubleType()))
        .withColumn("high_blood_pressure_flag", parse_binary(F.col("highbp")))
        .withColumn("high_cholesterol_flag", parse_binary(F.col("highchol")))
        .withColumn("chol_check_recent_flag", parse_binary(F.col("cholcheck")))
        .withColumn("smoking_flag", parse_binary(F.col("smoker")))
        .withColumn("stroke_history_flag", parse_binary(F.col("stroke")))
        .withColumn("physical_activity_flag", parse_binary(F.col("physactivity")))
        .withColumn("fruits_daily_flag", parse_binary(F.col("fruits")))
        .withColumn("veggies_daily_flag", parse_binary(F.col("veggies")))
        .withColumn("heavy_alcohol_flag", parse_binary(F.col("hvyalcoholconsump")))
        .withColumn("any_healthcare_flag", parse_binary(F.col("anyhealthcare")))
        .withColumn("no_doctor_cost_flag", parse_binary(F.col("nodocbccost")))
        .withColumn("general_health_score", F.col("genhlth").cast(T.IntegerType()))
        .withColumn("mental_unhealthy_days", F.col("menthlth").cast(T.DoubleType()))
        .withColumn("physical_unhealthy_days", F.col("physhlth").cast(T.DoubleType()))
        .withColumn("difficulty_walking_flag", parse_binary(F.col("diffwalk")))
        .withColumn("education_level", F.col("education").cast(T.IntegerType()))
        .withColumn("income_level", F.col("income").cast(T.IntegerType()))
        .withColumn("has_heart_disease", parse_binary(F.col("heartdiseaseorattack")))
        .select(
            "respondent_id",
            "age_group_code",
            "age_years_proxy",
            "gender",
            "bmi",
            "high_blood_pressure_flag",
            "high_cholesterol_flag",
            "chol_check_recent_flag",
            "smoking_flag",
            "stroke_history_flag",
            "physical_activity_flag",
            "fruits_daily_flag",
            "veggies_daily_flag",
            "heavy_alcohol_flag",
            "any_healthcare_flag",
            "no_doctor_cost_flag",
            "general_health_score",
            "mental_unhealthy_days",
            "physical_unhealthy_days",
            "difficulty_walking_flag",
            "education_level",
            "income_level",
            "has_heart_disease",
        )
    )
    log_schema(df_fmt, f"{dataset_name} [FORMATTED]")
    return df_fmt


def format_cleveland(spark: SparkSession) -> DataFrame:
    dataset_name = "heart_disease_cleveland"
    parquet_path = find_latest_parquet(dataset_name)
    df_raw = normalize_columns(spark.read.parquet(str(parquet_path)))
    log_schema(df_raw, f"{dataset_name} [RAW]")

    df_fmt = (
        df_raw
        .withColumn("record_key", stable_record_key(df_raw, "cleveland"))
        .withColumn("age_years", F.col("age").cast(T.IntegerType()))
        .withColumn("age_group_code", age_group_code_from_years(F.col("age_years")))
        .withColumn(
            "gender",
            parse_gender(F.col("sex"), ["0", "0.0", "female", "f"], ["1", "1.0", "male", "m"]),
        )
        .withColumn("chest_pain_type", F.col("cp").cast(T.IntegerType()))
        .withColumn("resting_bp", F.col("trestbps").cast(T.DoubleType()))
        .withColumn("serum_cholesterol", F.col("chol").cast(T.DoubleType()))
        .withColumn("fasting_blood_sugar_high", parse_binary(F.col("fbs")))
        .withColumn("resting_ecg", F.col("restecg").cast(T.IntegerType()))
        .withColumn("max_heart_rate", F.col("thalach").cast(T.DoubleType()))
        .withColumn("exercise_induced_angina", parse_binary(F.col("exang")))
        .withColumn("st_depression", F.col("oldpeak").cast(T.DoubleType()))
        .withColumn("st_slope", F.col("slope").cast(T.IntegerType()))
        .withColumn("num_major_vessels", F.col("ca").cast(T.IntegerType()))
        .withColumn("thalassemia_type", F.col("thal").cast(T.IntegerType()))
        .withColumn("has_heart_disease", parse_binary_or_positive(F.col("condition")))
        .select(
            "record_key",
            "age_years",
            "age_group_code",
            "gender",
            "chest_pain_type",
            "resting_bp",
            "serum_cholesterol",
            "fasting_blood_sugar_high",
            "resting_ecg",
            "max_heart_rate",
            "exercise_induced_angina",
            "st_depression",
            "st_slope",
            "num_major_vessels",
            "thalassemia_type",
            "has_heart_disease",
        )
    )
    log_schema(df_fmt, f"{dataset_name} [FORMATTED]")
    return df_fmt


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
