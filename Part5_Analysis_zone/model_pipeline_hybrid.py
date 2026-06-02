"""Hybrid pipeline: tabular features + KG embedding features combined.

Demonstrates the core value proposition of the KG: enriching traditional
tabular ML with graph-derived semantic context to boost predictive performance.
This pipeline concatenates the best tabular features with KG node embeddings
(population group, dataset, indicator context) and trains a classifier on
the combined feature space.
"""

import logging
import sys

import numpy as np
import pandas as pd
from sklearn.ensemble import GradientBoostingClassifier, RandomForestClassifier
from sklearn.linear_model import LogisticRegression
from sklearn.model_selection import StratifiedKFold, cross_val_score, train_test_split
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler

from analysis_config import (
    EXPLOITATION_DUCKDB_PATH,
    MODELS_DIR,
    RANDOM_SEED,
    REPORTS_DIR,
    TEST_SIZE,
)
from analysis_utils import (
    best_f1_threshold,
    classification_metrics,
    ensure_output_dirs,
    load_risk_model_input,
    save_json,
    save_model,
    utc_now_iso,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
log = logging.getLogger(__name__)

KG_EMBEDDINGS_PATH = REPORTS_DIR / "kg_node_embeddings.csv"

BASE_URI = "https://example.org/bda/health-risk/"

TABULAR_NUMERIC = [
    "age_years_proxy",
    "bmi",
    "systolic_bp",
    "diastolic_bp",
    "general_health_score",
    "mental_unhealthy_days",
    "physical_unhealthy_days",
    "max_heart_rate",
]

TABULAR_BINARY = [
    "high_blood_pressure_flag",
    "high_cholesterol_flag",
    "glucose_risk_flag",
    "smoking_flag",
    "physical_activity_flag",
    "heavy_alcohol_flag",
    "difficulty_walking_flag",
    "exercise_induced_angina",
]

MAX_SAMPLES = 60000


def slug(value, fallback="unknown") -> str:
    if pd.isna(value):
        return fallback
    import re
    text = str(value).strip().lower()
    text = re.sub(r"[^a-z0-9]+", "-", text).strip("-")
    return text or fallback


def population_group_uri(age_group_code, gender) -> str:
    age_val = "unknown" if pd.isna(age_group_code) else str(int(age_group_code))
    return BASE_URI + f"population-group/age-{age_val}/gender-{slug(gender)}"


def dataset_uri(dataset_name: str) -> str:
    return BASE_URI + f"dataset/{slug(dataset_name)}"


def load_embeddings() -> dict[str, np.ndarray]:
    """Load precomputed KG node embeddings from the embedding pipeline."""
    if not KG_EMBEDDINGS_PATH.exists():
        raise FileNotFoundError(
            f"KG node embeddings not found at {KG_EMBEDDINGS_PATH}. "
            "Run kg_embedding_pipeline.py first."
        )
    df = pd.read_csv(KG_EMBEDDINGS_PATH)
    emb_cols = [c for c in df.columns if c.startswith("kg_emb_")]
    embeddings = {}
    for _, row in df.iterrows():
        embeddings[row["node"]] = row[emb_cols].to_numpy(dtype=float)
    log.info("Loaded %d node embeddings (dim=%d)", len(embeddings), len(emb_cols))
    return embeddings


def build_hybrid_features(
    df: pd.DataFrame, embeddings: dict[str, np.ndarray]
) -> tuple[pd.DataFrame, np.ndarray]:
    """Build feature matrix combining tabular + KG embedding features."""
    emb_dim = len(next(iter(embeddings.values())))
    zero = np.zeros(emb_dim, dtype=float)

    rows = []
    targets = []

    for _, record in df.iterrows():
        target = record.get("target")
        if pd.isna(target):
            continue

        # Tabular features
        tab_features = {}
        for col in TABULAR_NUMERIC:
            val = record.get(col)
            tab_features[f"tab_{col}"] = float(val) if not pd.isna(val) else np.nan
        for col in TABULAR_BINARY:
            val = record.get(col)
            tab_features[f"tab_{col}"] = float(val) if not pd.isna(val) else np.nan

        # KG embedding features
        group_uri = population_group_uri(record.get("age_group_code"), record.get("gender"))
        ds_uri = dataset_uri(record.get("source_dataset", ""))

        group_emb = embeddings.get(group_uri, zero)
        dataset_emb = embeddings.get(ds_uri, zero)

        # Interaction: element-wise product captures joint semantics
        interaction_emb = group_emb * dataset_emb

        emb_features = {}
        for i, v in enumerate(group_emb):
            emb_features[f"kg_group_{i:02d}"] = float(v)
        for i, v in enumerate(dataset_emb):
            emb_features[f"kg_dataset_{i:02d}"] = float(v)
        for i, v in enumerate(interaction_emb):
            emb_features[f"kg_interact_{i:02d}"] = float(v)

        rows.append({**tab_features, **emb_features})
        targets.append(int(target))

    X = pd.DataFrame(rows)
    y = np.array(targets, dtype=int)
    return X, y


def stratified_sample(df: pd.DataFrame, max_rows: int) -> pd.DataFrame:
    if len(df) <= max_rows:
        return df
    return df.groupby(["source_dataset", "target"], group_keys=False).apply(
        lambda g: g.sample(
            n=max(1, round(len(g) * max_rows / len(df))),
            random_state=RANDOM_SEED,
        )
    ).reset_index(drop=True)


def main() -> None:
    ensure_output_dirs(MODELS_DIR, REPORTS_DIR)

    embeddings = load_embeddings()
    raw_df = load_risk_model_input(EXPLOITATION_DUCKDB_PATH)
    df = stratified_sample(raw_df, MAX_SAMPLES)
    log.info("Building hybrid features for %d records", len(df))

    X, y = build_hybrid_features(df, embeddings)
    log.info("Hybrid feature matrix: %d rows x %d features", X.shape[0], X.shape[1])

    # Count feature types
    tab_cols = [c for c in X.columns if c.startswith("tab_")]
    kg_cols = [c for c in X.columns if c.startswith("kg_")]
    log.info("Tabular features: %d, KG features: %d", len(tab_cols), len(kg_cols))

    # Train/test split
    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=TEST_SIZE, random_state=RANDOM_SEED, stratify=y
    )

    # Candidate models
    candidates = {
        "logreg_hybrid": Pipeline([
            ("scaler", StandardScaler()),
            ("clf", LogisticRegression(max_iter=2000, class_weight="balanced", random_state=RANDOM_SEED)),
        ]),
        "rf_hybrid": Pipeline([
            ("scaler", StandardScaler()),
            ("clf", RandomForestClassifier(
                n_estimators=150, max_depth=12, min_samples_leaf=8,
                class_weight="balanced_subsample", random_state=RANDOM_SEED, n_jobs=-1,
            )),
        ]),
        "gbm_hybrid": Pipeline([
            ("scaler", StandardScaler()),
            ("clf", GradientBoostingClassifier(
                n_estimators=150, max_depth=5, min_samples_leaf=10,
                random_state=RANDOM_SEED,
            )),
        ]),
    }

    cv = StratifiedKFold(n_splits=3, shuffle=True, random_state=RANDOM_SEED)
    model_scores = {}
    fitted = {}

    for name, pipe in candidates.items():
        scores = cross_val_score(pipe, X_train, y_train, cv=cv, scoring="average_precision", n_jobs=1)
        model_scores[name] = {
            "cv_pr_auc_mean": float(np.mean(scores)),
            "cv_pr_auc_std": float(np.std(scores)),
        }
        pipe.fit(X_train, y_train)
        fitted[name] = pipe
        log.info("  %s: PR-AUC=%.4f ± %.4f", name, np.mean(scores), np.std(scores))

    best_name = max(model_scores, key=lambda n: model_scores[n]["cv_pr_auc_mean"])
    best_model = fitted[best_name]

    train_prob = best_model.predict_proba(X_train)[:, 1]
    threshold = best_f1_threshold(y_train, train_prob)
    test_prob = best_model.predict_proba(X_test)[:, 1]
    test_pred = (test_prob >= threshold).astype(int)

    metrics = classification_metrics(y_test, test_pred, test_prob)

    # Feature importance for interpretability
    clf = best_model.named_steps["clf"]
    importance_list = []
    if hasattr(clf, "feature_importances_"):
        for fname, imp in sorted(zip(X.columns, clf.feature_importances_), key=lambda x: -x[1])[:25]:
            importance_list.append({"feature": fname, "importance": float(imp)})
    elif hasattr(clf, "coef_"):
        abs_coef = np.abs(clf.coef_[0])
        for fname, imp in sorted(zip(X.columns, abs_coef), key=lambda x: -x[1])[:25]:
            importance_list.append({"feature": fname, "importance": float(imp)})

    report = {
        "pipeline": "hybrid_tabular_kg_pipeline",
        "run_at_utc": utc_now_iso(),
        "description": (
            "Combines traditional tabular features with KG-derived node embeddings "
            "(population group + dataset + interaction) to demonstrate the boosting "
            "effect of semantic graph context on predictive performance."
        ),
        "feature_composition": {
            "tabular_features": len(tab_cols),
            "kg_embedding_features": len(kg_cols),
            "total_features": X.shape[1],
        },
        "rows_total": int(len(X)),
        "rows_train": int(len(X_train)),
        "rows_test": int(len(X_test)),
        "target_positive_rate": float(y.mean()),
        "candidate_models": model_scores,
        "selected_model": best_name,
        "decision_threshold": float(threshold),
        "test_metrics": metrics,
        "top_feature_importance": importance_list,
    }

    save_json(REPORTS_DIR / "hybrid_tabular_kg_report.json", report)
    save_model(MODELS_DIR / "hybrid_tabular_kg_model.pkl", best_model)
    log.info("Hybrid model: %s | F1=%.4f | ROC-AUC=%.4f | PR-AUC=%.4f",
             best_name, metrics["f1"], metrics.get("roc_auc", 0), metrics.get("pr_auc", 0))
    log.info("Report saved to %s", REPORTS_DIR / "hybrid_tabular_kg_report.json")


if __name__ == "__main__":
    main()
