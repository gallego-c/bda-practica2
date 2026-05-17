from typing import Iterable

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F
from pyspark.sql import types as T

from trusted_config import utc_now_iso


def add_quality_metric(
    metrics: list[dict],
    run_id: str,
    dataset: str,
    rule_name: str,
    total_rows: int,
    violations: int,
    action_applied: str,
) -> None:
    violation_pct = (float(violations) / float(total_rows) * 100.0) if total_rows > 0 else 0.0
    metrics.append(
        {
            "run_id": run_id,
            "dataset": dataset,
            "rule_name": rule_name,
            "total_rows": int(total_rows),
            "violations": int(violations),
            "violation_pct": float(round(violation_pct, 4)),
            "action_applied": action_applied,
            "measured_at_utc": utc_now_iso(),
        }
    )


def required_range(column_name: str, lower: float, upper: float) -> F.Column:
    return F.coalesce((F.col(column_name) >= lower) & (F.col(column_name) <= upper), F.lit(False))


def optional_range(column_name: str, lower: float, upper: float) -> F.Column:
    return F.coalesce(F.col(column_name).isNull() | ((F.col(column_name) >= lower) & (F.col(column_name) <= upper)), F.lit(False))


def required_domain(column_name: str, valid_values: Iterable) -> F.Column:
    return F.coalesce(F.col(column_name).isin(*list(valid_values)), F.lit(False))


def required_not_null(column_name: str) -> F.Column:
    return F.col(column_name).isNotNull()


def build_quarantine(
    df: DataFrame,
    run_id: str,
    dataset: str,
    record_key_col: str,
    all_ok: F.Column,
    reason_expr: F.Column,
) -> DataFrame:
    return (
        df.filter(~all_ok)
        .withColumn("run_id", F.lit(run_id))
        .withColumn("dataset", F.lit(dataset))
        .withColumn("record_key", F.col(record_key_col).cast(T.StringType()))
        .withColumn("quarantine_reason", reason_expr)
        .withColumn("quarantined_at_utc", F.lit(utc_now_iso()))
        .select("run_id", "dataset", "record_key", "quarantine_reason", "quarantined_at_utc")
    )


def finalize_cleaned(df: DataFrame, all_ok: F.Column, run_id: str) -> DataFrame:
    return (
        df.filter(all_ok)
        .drop(*[column_name for column_name in df.columns if column_name.startswith("_rule_")])
        .drop("_dup_rn")
        .withColumn("_run_id", F.lit(run_id))
        .withColumn("_processed_at_utc", F.lit(utc_now_iso()))
    )


def clean_cardiovascular(df_raw: DataFrame, run_id: str, metrics: list[dict]) -> tuple[DataFrame, DataFrame]:
    dataset = "cardiovascular_disease"
    total_rows = df_raw.count()

    dup_window = Window.partitionBy("patient_id").orderBy(F.col("patient_id"))
    df = (
        df_raw
        .withColumn("_rule_patient_id_present", required_not_null("patient_id"))
        .withColumn("_rule_age_range", required_range("age_years", 18, 100))
        .withColumn("_rule_age_group_code", required_range("age_group_code", 1, 13))
        .withColumn("_rule_gender_domain", required_domain("gender", ["female", "male"]))
        .withColumn("_rule_height_range", required_range("height_cm", 120, 250))
        .withColumn("_rule_weight_range", required_range("weight_kg", 30, 300))
        .withColumn("_rule_bmi_range", required_range("bmi", 10, 80))
        .withColumn("_rule_systolic_range", required_range("systolic_bp", 70, 250))
        .withColumn("_rule_diastolic_range", required_range("diastolic_bp", 40, 150))
        .withColumn("_rule_bp_order", F.coalesce(F.col("systolic_bp") >= F.col("diastolic_bp"), F.lit(False)))
        .withColumn("_rule_chol_domain", required_domain("cholesterol_level", [1, 2, 3]))
        .withColumn("_rule_glucose_domain", required_domain("glucose_level", [1, 2, 3]))
        .withColumn("_rule_smoker_present", required_not_null("is_smoker"))
        .withColumn("_rule_alcohol_present", required_not_null("drinks_alcohol"))
        .withColumn("_rule_activity_present", required_not_null("is_active"))
        .withColumn("_rule_target_present", required_not_null("has_cardiovascular_disease"))
        .withColumn("_dup_rn", F.row_number().over(dup_window))
        .withColumn("_rule_unique_patient_id", F.col("_dup_rn") == 1)
    )

    rules = [
        ("patient_id_present", "_rule_patient_id_present"),
        ("age_years_between_18_100", "_rule_age_range"),
        ("age_group_code_between_1_13", "_rule_age_group_code"),
        ("gender_domain_male_female", "_rule_gender_domain"),
        ("height_cm_between_120_250", "_rule_height_range"),
        ("weight_kg_between_30_300", "_rule_weight_range"),
        ("bmi_between_10_80", "_rule_bmi_range"),
        ("systolic_bp_between_70_250", "_rule_systolic_range"),
        ("diastolic_bp_between_40_150", "_rule_diastolic_range"),
        ("systolic_bp_gte_diastolic_bp", "_rule_bp_order"),
        ("cholesterol_level_domain_1_2_3", "_rule_chol_domain"),
        ("glucose_level_domain_1_2_3", "_rule_glucose_domain"),
        ("is_smoker_present", "_rule_smoker_present"),
        ("drinks_alcohol_present", "_rule_alcohol_present"),
        ("is_active_present", "_rule_activity_present"),
        ("target_present", "_rule_target_present"),
        ("patient_id_unique", "_rule_unique_patient_id"),
    ]
    for rule_name, rule_column in rules:
        add_quality_metric(metrics, run_id, dataset, rule_name, total_rows, df.filter(~F.col(rule_column)).count(), "quarantine_row")

    all_ok = (
        F.col("_rule_patient_id_present")
        & F.col("_rule_age_range")
        & F.col("_rule_age_group_code")
        & F.col("_rule_gender_domain")
        & F.col("_rule_height_range")
        & F.col("_rule_weight_range")
        & F.col("_rule_bmi_range")
        & F.col("_rule_systolic_range")
        & F.col("_rule_diastolic_range")
        & F.col("_rule_bp_order")
        & F.col("_rule_chol_domain")
        & F.col("_rule_glucose_domain")
        & F.col("_rule_smoker_present")
        & F.col("_rule_alcohol_present")
        & F.col("_rule_activity_present")
        & F.col("_rule_target_present")
        & F.col("_rule_unique_patient_id")
    )

    reason_expr = F.concat_ws(
        ";",
        F.when(~F.col("_rule_patient_id_present"), F.lit("missing_patient_id")),
        F.when(~F.col("_rule_age_range"), F.lit("age_out_of_range")),
        F.when(~F.col("_rule_age_group_code"), F.lit("age_group_invalid")),
        F.when(~F.col("_rule_gender_domain"), F.lit("gender_invalid")),
        F.when(~F.col("_rule_height_range"), F.lit("height_out_of_range")),
        F.when(~F.col("_rule_weight_range"), F.lit("weight_out_of_range")),
        F.when(~F.col("_rule_bmi_range"), F.lit("bmi_out_of_range")),
        F.when(~F.col("_rule_systolic_range"), F.lit("systolic_bp_out_of_range")),
        F.when(~F.col("_rule_diastolic_range"), F.lit("diastolic_bp_out_of_range")),
        F.when(~F.col("_rule_bp_order"), F.lit("bp_order_invalid")),
        F.when(~F.col("_rule_chol_domain"), F.lit("cholesterol_domain_invalid")),
        F.when(~F.col("_rule_glucose_domain"), F.lit("glucose_domain_invalid")),
        F.when(~F.col("_rule_smoker_present"), F.lit("missing_smoker_flag")),
        F.when(~F.col("_rule_alcohol_present"), F.lit("missing_alcohol_flag")),
        F.when(~F.col("_rule_activity_present"), F.lit("missing_activity_flag")),
        F.when(~F.col("_rule_target_present"), F.lit("missing_target")),
        F.when(~F.col("_rule_unique_patient_id"), F.lit("duplicate_patient_id")),
    )

    quarantine = build_quarantine(df, run_id, dataset, "patient_id", all_ok, reason_expr)
    cleaned = finalize_cleaned(df, all_ok, run_id)
    add_quality_metric(metrics, run_id, dataset, "rows_quarantined_total", total_rows, quarantine.count(), "excluded_from_trusted")
    return cleaned, quarantine


def clean_cdc_indicators(df_raw: DataFrame, run_id: str, metrics: list[dict]) -> tuple[DataFrame, DataFrame]:
    dataset = "heart_disease_health_indicators"
    total_rows = df_raw.count()

    df = (
        df_raw
        .withColumn("_rule_respondent_id_present", required_not_null("respondent_id"))
        .withColumn("_rule_age_group_code", required_range("age_group_code", 1, 13))
        .withColumn("_rule_age_years_proxy", required_range("age_years_proxy", 18, 90))
        .withColumn("_rule_gender_domain", required_domain("gender", ["female", "male"]))
        .withColumn("_rule_bmi_range", required_range("bmi", 10, 100))
        .withColumn("_rule_high_bp_present", required_not_null("high_blood_pressure_flag"))
        .withColumn("_rule_high_chol_present", required_not_null("high_cholesterol_flag"))
        .withColumn("_rule_smoking_present", required_not_null("smoking_flag"))
        .withColumn("_rule_activity_present", required_not_null("physical_activity_flag"))
        .withColumn("_rule_alcohol_present", required_not_null("heavy_alcohol_flag"))
        .withColumn("_rule_general_health_range", optional_range("general_health_score", 1, 5))
        .withColumn("_rule_mental_days_range", optional_range("mental_unhealthy_days", 0, 30))
        .withColumn("_rule_physical_days_range", optional_range("physical_unhealthy_days", 0, 30))
        .withColumn("_rule_difficulty_walking_present", required_not_null("difficulty_walking_flag"))
        .withColumn("_rule_education_range", optional_range("education_level", 1, 6))
        .withColumn("_rule_income_range", optional_range("income_level", 1, 8))
        .withColumn("_rule_target_present", required_not_null("has_heart_disease"))
        .withColumn("_dup_rn", F.lit(1))
    )

    rules = [
        ("respondent_id_present", "_rule_respondent_id_present"),
        ("age_group_code_between_1_13", "_rule_age_group_code"),
        ("age_years_proxy_between_18_90", "_rule_age_years_proxy"),
        ("gender_domain_male_female", "_rule_gender_domain"),
        ("bmi_between_10_100", "_rule_bmi_range"),
        ("high_blood_pressure_present", "_rule_high_bp_present"),
        ("high_cholesterol_present", "_rule_high_chol_present"),
        ("smoking_flag_present", "_rule_smoking_present"),
        ("physical_activity_present", "_rule_activity_present"),
        ("heavy_alcohol_present", "_rule_alcohol_present"),
        ("general_health_between_1_5_or_null", "_rule_general_health_range"),
        ("mental_unhealthy_days_between_0_30_or_null", "_rule_mental_days_range"),
        ("physical_unhealthy_days_between_0_30_or_null", "_rule_physical_days_range"),
        ("difficulty_walking_present", "_rule_difficulty_walking_present"),
        ("education_between_1_6_or_null", "_rule_education_range"),
        ("income_between_1_8_or_null", "_rule_income_range"),
        ("target_present", "_rule_target_present"),
    ]
    for rule_name, rule_column in rules:
        add_quality_metric(metrics, run_id, dataset, rule_name, total_rows, df.filter(~F.col(rule_column)).count(), "quarantine_row")

    all_ok = (
        F.col("_rule_respondent_id_present")
        & F.col("_rule_age_group_code")
        & F.col("_rule_age_years_proxy")
        & F.col("_rule_gender_domain")
        & F.col("_rule_bmi_range")
        & F.col("_rule_high_bp_present")
        & F.col("_rule_high_chol_present")
        & F.col("_rule_smoking_present")
        & F.col("_rule_activity_present")
        & F.col("_rule_alcohol_present")
        & F.col("_rule_general_health_range")
        & F.col("_rule_mental_days_range")
        & F.col("_rule_physical_days_range")
        & F.col("_rule_difficulty_walking_present")
        & F.col("_rule_education_range")
        & F.col("_rule_income_range")
        & F.col("_rule_target_present")
    )

    reason_expr = F.concat_ws(
        ";",
        F.when(~F.col("_rule_respondent_id_present"), F.lit("missing_respondent_id")),
        F.when(~F.col("_rule_age_group_code"), F.lit("age_group_invalid")),
        F.when(~F.col("_rule_age_years_proxy"), F.lit("age_proxy_invalid")),
        F.when(~F.col("_rule_gender_domain"), F.lit("gender_invalid")),
        F.when(~F.col("_rule_bmi_range"), F.lit("bmi_out_of_range")),
        F.when(~F.col("_rule_high_bp_present"), F.lit("missing_high_bp_flag")),
        F.when(~F.col("_rule_high_chol_present"), F.lit("missing_high_chol_flag")),
        F.when(~F.col("_rule_smoking_present"), F.lit("missing_smoking_flag")),
        F.when(~F.col("_rule_activity_present"), F.lit("missing_activity_flag")),
        F.when(~F.col("_rule_alcohol_present"), F.lit("missing_alcohol_flag")),
        F.when(~F.col("_rule_general_health_range"), F.lit("general_health_invalid")),
        F.when(~F.col("_rule_mental_days_range"), F.lit("mental_health_days_invalid")),
        F.when(~F.col("_rule_physical_days_range"), F.lit("physical_health_days_invalid")),
        F.when(~F.col("_rule_difficulty_walking_present"), F.lit("missing_diff_walk_flag")),
        F.when(~F.col("_rule_education_range"), F.lit("education_invalid")),
        F.when(~F.col("_rule_income_range"), F.lit("income_invalid")),
        F.when(~F.col("_rule_target_present"), F.lit("missing_target")),
    )

    quarantine = build_quarantine(df, run_id, dataset, "respondent_id", all_ok, reason_expr)
    cleaned = finalize_cleaned(df, all_ok, run_id)
    add_quality_metric(metrics, run_id, dataset, "rows_quarantined_total", total_rows, quarantine.count(), "excluded_from_trusted")
    return cleaned, quarantine


def clean_cleveland(df_raw: DataFrame, run_id: str, metrics: list[dict]) -> tuple[DataFrame, DataFrame]:
    dataset = "heart_disease_cleveland"
    total_rows = df_raw.count()

    dup_window = Window.partitionBy("record_key").orderBy(F.col("record_key"))
    df = (
        df_raw
        .withColumn("_rule_record_key_present", required_not_null("record_key"))
        .withColumn("_rule_age_range", required_range("age_years", 18, 100))
        .withColumn("_rule_age_group_code", required_range("age_group_code", 1, 13))
        .withColumn("_rule_gender_domain", required_domain("gender", ["female", "male"]))
        .withColumn("_rule_cp_domain", required_domain("chest_pain_type", [0, 1, 2, 3]))
        .withColumn("_rule_resting_bp_range", required_range("resting_bp", 70, 250))
        .withColumn("_rule_chol_range", required_range("serum_cholesterol", 50, 700))
        .withColumn("_rule_fbs_present", required_not_null("fasting_blood_sugar_high"))
        .withColumn("_rule_resting_ecg_domain", required_domain("resting_ecg", [0, 1, 2]))
        .withColumn("_rule_max_hr_range", required_range("max_heart_rate", 40, 250))
        .withColumn("_rule_exang_present", required_not_null("exercise_induced_angina"))
        .withColumn("_rule_st_depression_range", required_range("st_depression", 0, 10))
        .withColumn("_rule_st_slope_domain", required_domain("st_slope", [0, 1, 2]))
        .withColumn("_rule_num_vessels_range", optional_range("num_major_vessels", 0, 3))
        .withColumn("_rule_thal_domain", required_domain("thalassemia_type", [0, 1, 2]))
        .withColumn("_rule_target_present", required_not_null("has_heart_disease"))
        .withColumn("_dup_rn", F.row_number().over(dup_window))
        .withColumn("_rule_unique_record_key", F.col("_dup_rn") == 1)
    )

    rules = [
        ("record_key_present", "_rule_record_key_present"),
        ("age_years_between_18_100", "_rule_age_range"),
        ("age_group_code_between_1_13", "_rule_age_group_code"),
        ("gender_domain_male_female", "_rule_gender_domain"),
        ("chest_pain_type_domain_0_3", "_rule_cp_domain"),
        ("resting_bp_between_70_250", "_rule_resting_bp_range"),
        ("serum_cholesterol_between_50_700", "_rule_chol_range"),
        ("fasting_blood_sugar_present", "_rule_fbs_present"),
        ("resting_ecg_domain_0_2", "_rule_resting_ecg_domain"),
        ("max_heart_rate_between_40_250", "_rule_max_hr_range"),
        ("exercise_induced_angina_present", "_rule_exang_present"),
        ("st_depression_between_0_10", "_rule_st_depression_range"),
        ("st_slope_domain_0_2", "_rule_st_slope_domain"),
        ("num_major_vessels_between_0_3_or_null", "_rule_num_vessels_range"),
        ("thalassemia_type_domain_0_2", "_rule_thal_domain"),
        ("target_present", "_rule_target_present"),
        ("record_key_unique", "_rule_unique_record_key"),
    ]
    for rule_name, rule_column in rules:
        add_quality_metric(metrics, run_id, dataset, rule_name, total_rows, df.filter(~F.col(rule_column)).count(), "quarantine_row")

    all_ok = (
        F.col("_rule_record_key_present")
        & F.col("_rule_age_range")
        & F.col("_rule_age_group_code")
        & F.col("_rule_gender_domain")
        & F.col("_rule_cp_domain")
        & F.col("_rule_resting_bp_range")
        & F.col("_rule_chol_range")
        & F.col("_rule_fbs_present")
        & F.col("_rule_resting_ecg_domain")
        & F.col("_rule_max_hr_range")
        & F.col("_rule_exang_present")
        & F.col("_rule_st_depression_range")
        & F.col("_rule_st_slope_domain")
        & F.col("_rule_num_vessels_range")
        & F.col("_rule_thal_domain")
        & F.col("_rule_target_present")
        & F.col("_rule_unique_record_key")
    )

    reason_expr = F.concat_ws(
        ";",
        F.when(~F.col("_rule_record_key_present"), F.lit("missing_record_key")),
        F.when(~F.col("_rule_age_range"), F.lit("age_out_of_range")),
        F.when(~F.col("_rule_age_group_code"), F.lit("age_group_invalid")),
        F.when(~F.col("_rule_gender_domain"), F.lit("gender_invalid")),
        F.when(~F.col("_rule_cp_domain"), F.lit("chest_pain_invalid")),
        F.when(~F.col("_rule_resting_bp_range"), F.lit("resting_bp_out_of_range")),
        F.when(~F.col("_rule_chol_range"), F.lit("cholesterol_out_of_range")),
        F.when(~F.col("_rule_fbs_present"), F.lit("missing_fbs_flag")),
        F.when(~F.col("_rule_resting_ecg_domain"), F.lit("resting_ecg_invalid")),
        F.when(~F.col("_rule_max_hr_range"), F.lit("max_heart_rate_out_of_range")),
        F.when(~F.col("_rule_exang_present"), F.lit("missing_exang_flag")),
        F.when(~F.col("_rule_st_depression_range"), F.lit("st_depression_out_of_range")),
        F.when(~F.col("_rule_st_slope_domain"), F.lit("st_slope_invalid")),
        F.when(~F.col("_rule_num_vessels_range"), F.lit("num_major_vessels_invalid")),
        F.when(~F.col("_rule_thal_domain"), F.lit("thalassemia_invalid")),
        F.when(~F.col("_rule_target_present"), F.lit("missing_target")),
        F.when(~F.col("_rule_unique_record_key"), F.lit("duplicate_record_key")),
    )

    quarantine = build_quarantine(df, run_id, dataset, "record_key", all_ok, reason_expr)
    cleaned = finalize_cleaned(df, all_ok, run_id)
    add_quality_metric(metrics, run_id, dataset, "rows_quarantined_total", total_rows, quarantine.count(), "excluded_from_trusted")
    return cleaned, quarantine
