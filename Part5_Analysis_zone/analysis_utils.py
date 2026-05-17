import json
import pickle
from datetime import datetime, timezone
from pathlib import Path

import duckdb
import numpy as np
import pandas as pd
from sklearn.compose import ColumnTransformer
from sklearn.ensemble import RandomForestClassifier
from sklearn.impute import SimpleImputer
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import (
    accuracy_score,
    average_precision_score,
    confusion_matrix,
    f1_score,
    precision_recall_curve,
    precision_score,
    recall_score,
    roc_auc_score,
)
from sklearn.model_selection import StratifiedKFold, cross_val_score
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import OneHotEncoder, StandardScaler


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def ensure_output_dirs(*paths: Path) -> None:
    for path in paths:
        path.mkdir(parents=True, exist_ok=True)


def load_risk_model_input(db_path: Path) -> pd.DataFrame:
    con = duckdb.connect(str(db_path), read_only=True)
    try:
        return con.execute("SELECT * FROM exploitation.exploitation.risk_model_input").df()
    finally:
        con.close()


def load_exploitation_source(db_path: Path, source_dataset: str) -> pd.DataFrame:
    con = duckdb.connect(str(db_path), read_only=True)
    try:
        return con.execute(
            """
            SELECT *
            FROM exploitation.exploitation.risk_model_input
            WHERE source_dataset = ?
            """,
            [source_dataset],
        ).df()
    finally:
        con.close()


def build_preprocessor(numeric_features: list[str], categorical_features: list[str]) -> ColumnTransformer:
    numeric_pipe = Pipeline(
        steps=[
            ("imputer", SimpleImputer(strategy="median")),
            ("scaler", StandardScaler()),
        ]
    )
    categorical_pipe = Pipeline(
        steps=[
            ("imputer", SimpleImputer(strategy="most_frequent")),
            ("onehot", OneHotEncoder(handle_unknown="ignore")),
        ]
    )
    return ColumnTransformer(
        transformers=[
            ("num", numeric_pipe, numeric_features),
            ("cat", categorical_pipe, categorical_features),
        ],
        remainder="drop",
    )


def safe_auc(metric_fn, y_true: np.ndarray, y_score: np.ndarray):
    return None if len(np.unique(y_true)) < 2 else float(metric_fn(y_true, y_score))


def classification_metrics(y_true: np.ndarray, y_pred: np.ndarray, y_prob: np.ndarray) -> dict:
    return {
        "accuracy": float(accuracy_score(y_true, y_pred)),
        "precision": float(precision_score(y_true, y_pred, zero_division=0)),
        "recall": float(recall_score(y_true, y_pred, zero_division=0)),
        "f1": float(f1_score(y_true, y_pred, zero_division=0)),
        "roc_auc": safe_auc(roc_auc_score, y_true, y_prob),
        "pr_auc": safe_auc(average_precision_score, y_true, y_prob),
        "confusion_matrix": confusion_matrix(y_true, y_pred, labels=[0, 1]).tolist(),
    }


def save_json(path: Path, payload: dict) -> None:
    with open(path, "w", encoding="utf-8") as handle:
        json.dump(payload, handle, indent=2, ensure_ascii=False)


def save_model(path: Path, model) -> None:
    with open(path, "wb") as handle:
        pickle.dump(model, handle)


def best_f1_threshold(y_true: np.ndarray, y_prob: np.ndarray) -> float:
    precision, recall, thresholds = precision_recall_curve(y_true, y_prob)
    if len(thresholds) == 0:
        return 0.5

    precision = precision[:-1]
    recall = recall[:-1]
    f1 = (2 * precision * recall) / (precision + recall + 1e-12)
    return float(thresholds[int(np.nanargmax(f1))])


def build_default_candidates(random_seed: int) -> dict:
    return {
        "logreg_balanced": LogisticRegression(
            max_iter=2000,
            class_weight="balanced",
            random_state=random_seed,
        ),
        "rf_balanced": RandomForestClassifier(
            n_estimators=120,
            max_depth=12,
            min_samples_leaf=12,
            class_weight="balanced_subsample",
            random_state=random_seed,
            n_jobs=-1,
        ),
    }


def fit_candidate_models(
    X_train: pd.DataFrame,
    y_train: np.ndarray,
    numeric_features: list[str],
    categorical_features: list[str],
    scoring: str,
    score_label: str,
    cv_folds: int,
    random_seed: int,
    candidates: dict | None = None,
) -> tuple[str, Pipeline, dict]:
    preprocessor = build_preprocessor(numeric_features, categorical_features)
    candidates = candidates or build_default_candidates(random_seed)
    cv = StratifiedKFold(n_splits=cv_folds, shuffle=True, random_state=random_seed)

    fitted_models: dict[str, Pipeline] = {}
    model_scores: dict[str, dict] = {}

    for name, estimator in candidates.items():
        pipe = Pipeline(
            steps=[
                ("prep", preprocessor),
                ("clf", estimator),
            ]
        )
        cv_scores = cross_val_score(pipe, X_train, y_train, cv=cv, scoring=scoring, n_jobs=1)
        model_scores[name] = {
            f"cv_{score_label}_mean": float(np.mean(cv_scores)),
            f"cv_{score_label}_std": float(np.std(cv_scores)),
        }
        pipe.fit(X_train, y_train)
        fitted_models[name] = pipe

    best_name = max(model_scores, key=lambda model_name: model_scores[model_name][f"cv_{score_label}_mean"])
    return best_name, fitted_models[best_name], model_scores


def prepare_model_frame(
    df: pd.DataFrame,
    numeric_features: list[str],
    categorical_features: list[str],
    target_col: str = "target",
) -> tuple[pd.DataFrame, np.ndarray]:
    feature_cols = numeric_features + categorical_features
    frame = df[feature_cols + [target_col]].copy()
    frame = frame.dropna(subset=[target_col])

    X = frame[feature_cols].copy()
    for column_name in numeric_features:
        X[column_name] = pd.to_numeric(X[column_name], errors="coerce").astype(float)
    for column_name in categorical_features:
        series = X[column_name]
        series = series.astype("object")
        series = series.where(pd.notna(series), np.nan)
        X[column_name] = series

    y = frame[target_col].astype(int).to_numpy()
    return X, y


def sample_balanced_sources(
    df: pd.DataFrame,
    source_col: str,
    random_seed: int,
) -> pd.DataFrame:
    counts = df[source_col].value_counts()
    if counts.empty:
        return df.copy()

    min_count = int(counts.min())
    parts = []
    for source_name in sorted(counts.index):
        source_df = df[df[source_col] == source_name]
        if len(source_df) > min_count:
            source_df = source_df.sample(n=min_count, random_state=random_seed)
        parts.append(source_df)

    return pd.concat(parts, axis=0).sample(frac=1.0, random_state=random_seed).reset_index(drop=True)
