from pathlib import Path

from sklearn.model_selection import train_test_split

from analysis_config import CV_FOLDS, RANDOM_SEED, TEST_SIZE
from analysis_utils import (
    best_f1_threshold,
    classification_metrics,
    fit_candidate_models,
    prepare_model_frame,
    utc_now_iso,
)


def run_integrated_core_pipeline(df, reports_dir: Path, models_dir: Path, save_json, save_model):
    numeric_features = [
        "age_years_proxy",
        "bmi",
    ]
    categorical_features = [
        "gender",
        "high_blood_pressure_flag",
        "high_cholesterol_flag",
        "glucose_risk_flag",
        "smoking_flag",
        "physical_activity_flag",
        "heavy_alcohol_flag",
    ]

    X, y = prepare_model_frame(df, numeric_features, categorical_features)
    X_train, X_test, y_train, y_test = train_test_split(
        X,
        y,
        test_size=TEST_SIZE,
        random_state=RANDOM_SEED,
        stratify=y,
    )

    best_name, best_model, model_scores = fit_candidate_models(
        X_train,
        y_train,
        numeric_features,
        categorical_features,
        scoring="roc_auc",
        score_label="roc_auc",
        cv_folds=CV_FOLDS,
        random_seed=RANDOM_SEED,
    )

    train_prob = best_model.predict_proba(X_train)[:, 1]
    threshold = best_f1_threshold(y_train, train_prob)
    test_prob = best_model.predict_proba(X_test)[:, 1]
    test_pred = (test_prob >= threshold).astype(int)

    report = {
        "pipeline": "model_pipeline_integrated_core",
        "dataset": "integrated_risk_model_input",
        "run_at_utc": utc_now_iso(),
        "rows_total": int(len(df)),
        "rows_train": int(len(X_train)),
        "rows_test": int(len(X_test)),
        "target_positive_rate": float(y.mean()),
        "source_distribution": df["source_dataset"].value_counts().to_dict(),
        "integration_mode": "all_sources_together_without_source_feature",
        "split_strategy": "stratified_by_target",
        "candidate_models": model_scores,
        "selected_model": best_name,
        "decision_threshold": float(threshold),
        "features": numeric_features + categorical_features,
        "test_metrics": classification_metrics(y_test, test_pred, test_prob),
    }

    save_json(reports_dir / "integrated_core_report.json", report)
    save_model(models_dir / "integrated_core_model.pkl", best_model)
    return report
