from pathlib import Path

import numpy as np
from sklearn.ensemble import RandomForestClassifier
from sklearn.linear_model import LogisticRegression
from sklearn.model_selection import StratifiedKFold, cross_val_score, train_test_split
from sklearn.pipeline import Pipeline

from analysis_config import CV_FOLDS, RANDOM_SEED, TEST_SIZE
from analysis_utils import (
    best_f1_threshold,
    build_preprocessor,
    classification_metrics,
    utc_now_iso,
)


def run_cardio_pipeline(df, cleveland_df, reports_dir: Path, models_dir: Path, save_json, save_model):
    dataset_name = "cardiovascular_disease"

    feature_cols = [
        "age_years",
        "gender",
        "systolic_bp",
        "diastolic_bp",
        "bmi",
        "cholesterol_proxy",
        "glucose_proxy",
        "smoking_proxy",
        "pulse_pressure",
        "hypertension_stage1_flag",
    ]
    target_col = "target"

    df = df[feature_cols + [target_col]].copy()
    df = df.dropna(subset=[target_col])

    X = df[feature_cols].copy()
    y = df[target_col].astype(int).to_numpy()

    numeric_features = [
        "age_years",
        "systolic_bp",
        "diastolic_bp",
        "bmi",
        "cholesterol_proxy",
        "glucose_proxy",
        "smoking_proxy",
        "pulse_pressure",
    ]
    categorical_features = ["gender", "hypertension_stage1_flag"]

    for c in categorical_features:
        X[c] = X[c].astype(str)

    X_train, X_test, y_train, y_test = train_test_split(
        X,
        y,
        test_size=TEST_SIZE,
        random_state=RANDOM_SEED,
        stratify=y,
    )

    preprocessor = build_preprocessor(numeric_features, categorical_features)

    candidates = {
        "logreg": LogisticRegression(max_iter=2000, class_weight="balanced", random_state=RANDOM_SEED),
        "rf": RandomForestClassifier(
            n_estimators=350,
            max_depth=12,
            min_samples_leaf=8,
            class_weight="balanced_subsample",
            random_state=RANDOM_SEED,
            n_jobs=-1,
        ),
    }

    cv = StratifiedKFold(n_splits=CV_FOLDS, shuffle=True, random_state=RANDOM_SEED)

    model_scores = {}
    fitted = {}
    for name, estimator in candidates.items():
        pipe = Pipeline([
            ("prep", preprocessor),
            ("clf", estimator),
        ])
        cv_auc = cross_val_score(pipe, X_train, y_train, cv=cv, scoring="roc_auc", n_jobs=-1)
        model_scores[name] = {
            "cv_roc_auc_mean": float(np.mean(cv_auc)),
            "cv_roc_auc_std": float(np.std(cv_auc)),
        }
        pipe.fit(X_train, y_train)
        fitted[name] = pipe

    best_name = max(model_scores, key=lambda k: model_scores[k]["cv_roc_auc_mean"])
    best_model = fitted[best_name]

    train_prob = best_model.predict_proba(X_train)[:, 1]
    best_threshold = best_f1_threshold(y_train, train_prob)

    y_prob = best_model.predict_proba(X_test)[:, 1]
    y_pred = (y_prob >= best_threshold).astype(int)

    test_metrics = classification_metrics(y_test, y_pred, y_prob)

    report = {
        "pipeline": "model_pipeline_cardio",
        "dataset": dataset_name,
        "run_at_utc": utc_now_iso(),
        "rows_total": int(len(df)),
        "rows_train": int(len(X_train)),
        "rows_test": int(len(X_test)),
        "target_positive_rate": float(np.mean(y)),
        "candidate_models": model_scores,
        "selected_model": best_name,
        "decision_threshold": float(best_threshold),
        "test_metrics": test_metrics,
        "features": feature_cols,
    }

    if cleveland_df is not None and not cleveland_df.empty:
        ext_df = cleveland_df[feature_cols + [target_col]].copy().dropna(subset=[target_col])
        X_ext = ext_df[feature_cols].copy()
        for c in categorical_features:
            X_ext[c] = X_ext[c].astype(str)

        y_ext = ext_df[target_col].astype(int).to_numpy()
        y_ext_prob = best_model.predict_proba(X_ext)[:, 1]
        y_ext_pred = (y_ext_prob >= best_threshold).astype(int)

        report["external_validation_cleveland"] = {
            "rows": int(len(ext_df)),
            "target_positive_rate": float(np.mean(y_ext)),
            "metrics": classification_metrics(y_ext, y_ext_pred, y_ext_prob),
        }

    save_json(reports_dir / "cardio_report.json", report)
    save_model(models_dir / "cardio_model.pkl", best_model)

    return report
