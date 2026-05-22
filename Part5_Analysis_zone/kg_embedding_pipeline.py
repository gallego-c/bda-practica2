import json
import logging
import re
import sys
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
from rdflib import Graph, URIRef
from sklearn.ensemble import RandomForestClassifier
from sklearn.linear_model import LogisticRegression
from sklearn.model_selection import StratifiedKFold, cross_val_score, train_test_split
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler
from sklearn.decomposition import TruncatedSVD

from analysis_config import EXPLOITATION_DUCKDB_PATH, MODELS_DIR, PROJECT_ROOT, RANDOM_SEED, REPORTS_DIR, TEST_SIZE
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


BASE_URI = "https://example.org/bda/health-risk/"
HR = URIRef(BASE_URI)
KG_ROOT = PROJECT_ROOT / "Part4_Exploitation_zone" / "exploitation_zone" / "kg"
KG_MANIFEST_PATH = KG_ROOT / "kg_manifest.json"
DEFAULT_ANALYTICS_GRAPH_PATH = KG_ROOT / "health_risk_analytics_kg.ttl"
OUTCOME_INDICATOR = URIRef(BASE_URI + "indicator/heart-disease-outcome")
MIN_OBSERVATIONS_PER_SAMPLE = 25
EMBEDDING_DIMENSIONS = 16
MAX_RECORD_SAMPLES = 60000

RISK_FACTOR_COLUMNS = {
    "high_blood_pressure_flag": "high-blood-pressure",
    "high_cholesterol_flag": "high-cholesterol",
    "glucose_risk_flag": "glucose-risk",
    "smoking_flag": "smoking",
    "heavy_alcohol_flag": "heavy-alcohol",
    "difficulty_walking_flag": "difficulty-walking",
    "exercise_induced_angina": "exercise-induced-angina",
}

PROTECTIVE_FACTOR_COLUMNS = {
    "physical_activity_flag": "physical-activity",
}

OBSERVED_INDICATOR_COLUMNS = {
    "age_years_proxy": "age-years",
    "bmi": "body-mass-index",
    "systolic_bp": "systolic-blood-pressure",
    "diastolic_bp": "diastolic-blood-pressure",
    "high_blood_pressure_flag": "high-blood-pressure",
    "high_cholesterol_flag": "high-cholesterol",
    "glucose_risk_flag": "glucose-risk",
    "smoking_flag": "smoking",
    "physical_activity_flag": "physical-activity",
    "heavy_alcohol_flag": "heavy-alcohol",
    "general_health_score": "general-health-score",
    "mental_unhealthy_days": "mental-unhealthy-days",
    "physical_unhealthy_days": "physical-unhealthy-days",
    "difficulty_walking_flag": "difficulty-walking",
    "max_heart_rate": "maximum-heart-rate",
    "exercise_induced_angina": "exercise-induced-angina",
}


def hr(name: str) -> URIRef:
    return URIRef(BASE_URI + name)


def term_to_float(value: Any) -> float | None:
    if value is None:
        return None
    try:
        return float(value.toPython() if hasattr(value, "toPython") else value)
    except (TypeError, ValueError):
        return None


def term_to_int(value: Any) -> int | None:
    numeric = term_to_float(value)
    return None if numeric is None else int(numeric)


def is_missing(value: Any) -> bool:
    try:
        return bool(pd.isna(value))
    except TypeError:
        return value is None


def as_bool(value: Any) -> bool | None:
    if is_missing(value):
        return None
    if isinstance(value, str):
        text = value.strip().lower()
        if text in {"true", "1", "1.0", "yes", "y"}:
            return True
        if text in {"false", "0", "0.0", "no", "n"}:
            return False
        return None
    return bool(value)


def slug(value: Any, fallback: str = "unknown") -> str:
    if is_missing(value):
        return fallback
    text = str(value).strip().lower()
    text = re.sub(r"[^a-z0-9]+", "-", text).strip("-")
    return text or fallback


def dataset_uri_text(dataset_name: str) -> str:
    return BASE_URI + f"dataset/{slug(dataset_name)}"


def indicator_uri_text(indicator_id: str) -> str:
    return BASE_URI + f"indicator/{indicator_id}"


def population_group_uri_text(age_group_code: Any, gender: Any) -> str:
    age_value = term_to_int(age_group_code)
    age_slug = "unknown" if age_value is None else str(age_value)
    return BASE_URI + f"population-group/age-{age_slug}/gender-{slug(gender)}"


def parse_format(path: Path) -> str:
    return "nt" if path.suffix.lower() == ".nt" else "turtle"


def load_manifest() -> dict:
    if not KG_MANIFEST_PATH.exists():
        raise FileNotFoundError(f"KG manifest not found at {KG_MANIFEST_PATH}. Run Part4 first.")
    return json.loads(KG_MANIFEST_PATH.read_text(encoding="utf-8"))


def resolve_analytics_graph_path(manifest: dict) -> Path:
    manifest_path = Path(manifest["storage"]["analytics_graph_file"])
    if manifest_path.exists():
        return manifest_path
    if DEFAULT_ANALYTICS_GRAPH_PATH.exists():
        return DEFAULT_ANALYTICS_GRAPH_PATH
    raise FileNotFoundError(
        f"KG analytics graph not found at {manifest_path} or {DEFAULT_ANALYTICS_GRAPH_PATH}. Run Part4 first."
    )


def load_graph() -> tuple[Graph, Path]:
    manifest = load_manifest()
    graph_path = resolve_analytics_graph_path(manifest)
    graph = Graph()
    graph.parse(str(graph_path), format=parse_format(graph_path))
    return graph, graph_path


def outcome_measurement_nodes(graph: Graph) -> set[URIRef]:
    return {
        measurement
        for measurement in graph.subjects(hr("measuresIndicator"), OUTCOME_INDICATOR)
        if isinstance(measurement, URIRef)
    }


def collect_embedding_edges(graph: Graph, held_out_nodes: set[URIRef]) -> list[tuple[str, str]]:
    edges: list[tuple[str, str]] = []
    for subject, predicate, obj in graph:
        if subject in held_out_nodes or obj in held_out_nodes:
            continue
        if not isinstance(subject, URIRef) or not isinstance(obj, URIRef):
            continue

        predicate_node = f"{predicate}#predicate"
        subject_text = str(subject)
        object_text = str(obj)
        edges.append((subject_text, predicate_node))
        edges.append((predicate_node, object_text))
    return edges


def build_node_embeddings(edges: list[tuple[str, str]], dimensions: int) -> tuple[pd.DataFrame, dict[str, np.ndarray]]:
    if not edges:
        raise ValueError("No RDF URI-to-URI edges were available for KG embedding generation.")

    node_names = sorted({node for edge in edges for node in edge})
    node_to_index = {node: index for index, node in enumerate(node_names)}
    adjacency = np.zeros((len(node_names), len(node_names)), dtype=np.float32)

    for left, right in edges:
        left_index = node_to_index[left]
        right_index = node_to_index[right]
        adjacency[left_index, right_index] = 1.0
        adjacency[right_index, left_index] = 1.0

    adjacency += np.eye(len(node_names), dtype=np.float32)
    degree = np.maximum(adjacency.sum(axis=1), 1.0)
    adjacency = adjacency / np.sqrt(np.outer(degree, degree))

    n_components = min(dimensions, max(1, len(node_names) - 1))
    svd = TruncatedSVD(n_components=n_components, random_state=RANDOM_SEED)
    values = svd.fit_transform(adjacency)
    if n_components < dimensions:
        values = np.pad(values, ((0, 0), (0, dimensions - n_components)))

    columns = [f"kg_emb_{index:02d}" for index in range(dimensions)]
    frame = pd.DataFrame(values, columns=columns)
    frame.insert(0, "node", node_names)
    embeddings = {
        node_name: frame.loc[row_index, columns].to_numpy(dtype=float)
        for row_index, node_name in enumerate(node_names)
    }
    return frame, embeddings


def first_object(graph: Graph, subject: URIRef, predicate: URIRef) -> URIRef | None:
    value = next(graph.objects(subject, predicate), None)
    return value if isinstance(value, URIRef) else None


def zero_embedding() -> np.ndarray:
    return np.zeros(EMBEDDING_DIMENSIONS, dtype=float)


def mean_embedding(node_ids: list[str], embeddings: dict[str, np.ndarray]) -> np.ndarray:
    vectors = [embeddings[node_id] for node_id in node_ids if node_id in embeddings]
    if not vectors:
        return zero_embedding()
    return np.mean(vectors, axis=0)


def stratified_record_sample(frame: pd.DataFrame, max_rows: int) -> pd.DataFrame:
    if len(frame) <= max_rows:
        return frame.reset_index(drop=True)

    parts = []
    grouped = frame.groupby(["source_dataset", "target"], dropna=False)
    for _, group in grouped:
        group_size = max(1, round(len(group) * max_rows / len(frame)))
        group_size = min(group_size, len(group))
        parts.append(group.sample(n=group_size, random_state=RANDOM_SEED))

    sampled = pd.concat(parts, axis=0)
    if len(sampled) > max_rows:
        sampled = sampled.sample(n=max_rows, random_state=RANDOM_SEED)
    return sampled.sample(frac=1.0, random_state=RANDOM_SEED).reset_index(drop=True)


def build_record_training_samples(risk_df: pd.DataFrame, embeddings: dict[str, np.ndarray]) -> pd.DataFrame:
    frame = risk_df.dropna(subset=["source_dataset", "age_group_code", "gender", "target"]).copy()
    frame["target"] = frame["target"].map(as_bool)
    frame = frame.dropna(subset=["target"])
    frame = stratified_record_sample(frame, MAX_RECORD_SAMPLES)

    rows = []
    for row in frame.to_dict("records"):
        dataset_node = dataset_uri_text(row["source_dataset"])
        group_node = population_group_uri_text(row["age_group_code"], row["gender"])
        dataset_embedding = embeddings.get(dataset_node)
        group_embedding = embeddings.get(group_node)
        if dataset_embedding is None or group_embedding is None:
            continue

        observed_nodes = [
            indicator_uri_text(indicator_id)
            for column_name, indicator_id in OBSERVED_INDICATOR_COLUMNS.items()
            if column_name in row and not is_missing(row[column_name])
        ]
        risk_nodes = [
            indicator_uri_text(indicator_id)
            for column_name, indicator_id in RISK_FACTOR_COLUMNS.items()
            if column_name in row and as_bool(row[column_name]) is True
        ]
        protective_nodes = [
            indicator_uri_text(indicator_id)
            for column_name, indicator_id in PROTECTIVE_FACTOR_COLUMNS.items()
            if column_name in row and as_bool(row[column_name]) is True
        ]

        observed_embedding = mean_embedding(observed_nodes, embeddings)
        risk_embedding = mean_embedding(risk_nodes, embeddings)
        protective_embedding = mean_embedding(protective_nodes, embeddings)
        feature_vector = np.concatenate(
            [
                dataset_embedding,
                group_embedding,
                observed_embedding,
                risk_embedding,
                protective_embedding,
                dataset_embedding * group_embedding,
                np.abs(dataset_embedding - group_embedding),
                np.array([len(observed_nodes), len(risk_nodes), len(protective_nodes)], dtype=float),
            ]
        )
        rows.append(
            {
                "source_dataset": row["source_dataset"],
                "source_record_id": row.get("source_record_id"),
                "population_group": group_node,
                "target": int(row["target"]),
                "observed_indicator_count": len(observed_nodes),
                "risk_indicator_count": len(risk_nodes),
                "protective_indicator_count": len(protective_nodes),
                **{f"feature_{index:03d}": float(value) for index, value in enumerate(feature_vector)},
            }
        )

    samples = pd.DataFrame(rows)
    if samples.empty:
        raise ValueError("No record-level KG embedding training samples were generated.")
    return samples


def build_training_samples(graph: Graph, embeddings: dict[str, np.ndarray], held_out_nodes: set[URIRef]) -> pd.DataFrame:
    rows = []
    for measurement in sorted(held_out_nodes, key=str):
        population_group = first_object(graph, measurement, hr("forPopulationGroup"))
        dataset = first_object(graph, measurement, hr("fromDataset"))
        positive_rate = term_to_float(next(graph.objects(measurement, hr("positiveRate")), None))
        observations = term_to_int(next(graph.objects(measurement, hr("observationCount")), None))

        if population_group is None or dataset is None or positive_rate is None or observations is None:
            continue
        if observations < MIN_OBSERVATIONS_PER_SAMPLE:
            continue

        group_embedding = embeddings.get(str(population_group))
        dataset_embedding = embeddings.get(str(dataset))
        if group_embedding is None or dataset_embedding is None:
            continue

        feature_vector = np.concatenate(
            [
                group_embedding,
                dataset_embedding,
                group_embedding * dataset_embedding,
                np.abs(group_embedding - dataset_embedding),
            ]
        )
        rows.append(
            {
                "measurement": str(measurement),
                "population_group": str(population_group),
                "dataset": str(dataset),
                "positive_rate": positive_rate,
                "observations": observations,
                **{f"feature_{index:02d}": float(value) for index, value in enumerate(feature_vector)},
            }
        )

    frame = pd.DataFrame(rows)
    if frame.empty:
        raise ValueError("No eligible KG embedding training samples were generated.")

    frame["source_median_positive_rate"] = frame.groupby("dataset")["positive_rate"].transform("median")
    frame["high_risk_label"] = (frame["positive_rate"] > frame["source_median_positive_rate"]).astype(int)
    return frame


def fit_embedding_model(samples: pd.DataFrame) -> tuple[str, Pipeline, dict, dict]:
    feature_cols = [column for column in samples.columns if column.startswith("feature_")]
    X = samples[feature_cols]
    y = samples["target"].to_numpy(dtype=int)

    class_counts = np.bincount(y, minlength=2)
    if np.count_nonzero(class_counts) < 2:
        raise ValueError(
            "KG embedding labels contain a single class. Adjust the prediction task or sample filter before training."
        )
    stratify = y if class_counts.min() >= 2 else None
    X_train, X_test, y_train, y_test = train_test_split(
        X,
        y,
        test_size=TEST_SIZE,
        random_state=RANDOM_SEED,
        stratify=stratify,
    )

    candidates = {
        "logreg_embedding": LogisticRegression(max_iter=2000, class_weight="balanced", random_state=RANDOM_SEED),
        "rf_embedding": RandomForestClassifier(
            n_estimators=140,
            max_depth=10,
            min_samples_leaf=8,
            class_weight="balanced_subsample",
            random_state=RANDOM_SEED,
            n_jobs=-1,
        ),
    }

    fitted_models: dict[str, Pipeline] = {}
    model_scores: dict[str, dict] = {}
    min_train_class = int(np.bincount(y_train, minlength=2).min())
    cv_folds = min(3, min_train_class)

    for name, estimator in candidates.items():
        pipeline = Pipeline(
            steps=[
                ("scaler", StandardScaler()),
                ("clf", estimator),
            ]
        )
        if cv_folds >= 2:
            cv = StratifiedKFold(n_splits=cv_folds, shuffle=True, random_state=RANDOM_SEED)
            scores = cross_val_score(pipeline, X_train, y_train, cv=cv, scoring="average_precision", n_jobs=1)
            model_scores[name] = {
                "cv_pr_auc_mean": float(np.mean(scores)),
                "cv_pr_auc_std": float(np.std(scores)),
                "cv_folds": int(cv_folds),
            }
        else:
            model_scores[name] = {
                "cv_pr_auc_mean": None,
                "cv_pr_auc_std": None,
                "cv_folds": 0,
            }
        pipeline.fit(X_train, y_train)
        fitted_models[name] = pipeline

    best_name = max(
        model_scores,
        key=lambda model_name: model_scores[model_name]["cv_pr_auc_mean"]
        if model_scores[model_name]["cv_pr_auc_mean"] is not None
        else -1.0,
    )
    best_model = fitted_models[best_name]
    train_prob = best_model.predict_proba(X_train)[:, 1]
    threshold = best_f1_threshold(y_train, train_prob)
    test_prob = best_model.predict_proba(X_test)[:, 1]
    test_pred = (test_prob >= threshold).astype(int)

    evaluation = {
        "rows_train": int(len(X_train)),
        "rows_test": int(len(X_test)),
        "decision_threshold": float(threshold),
        "test_metrics": classification_metrics(y_test, test_pred, test_prob),
    }
    return best_name, best_model, model_scores, evaluation


def main() -> None:
    ensure_output_dirs(MODELS_DIR, REPORTS_DIR)
    graph, graph_path = load_graph()
    log.info("Loaded analytics KG with %s triples", f"{len(graph):,}")

    held_out_outcome_nodes = outcome_measurement_nodes(graph)
    edges = collect_embedding_edges(graph, held_out_outcome_nodes)
    embeddings_frame, embeddings = build_node_embeddings(edges, EMBEDDING_DIMENSIONS)
    risk_df = load_risk_model_input(EXPLOITATION_DUCKDB_PATH)
    samples = build_record_training_samples(risk_df, embeddings)

    embeddings_path = REPORTS_DIR / "kg_node_embeddings.csv"
    samples_path = REPORTS_DIR / "kg_embedding_training_data.csv"
    embeddings_frame.to_csv(embeddings_path, index=False)
    sample_audit_columns = [
        "source_dataset",
        "source_record_id",
        "population_group",
        "target",
        "observed_indicator_count",
        "risk_indicator_count",
        "protective_indicator_count",
    ]
    samples[sample_audit_columns].to_csv(samples_path, index=False)

    best_name, best_model, model_scores, evaluation = fit_embedding_model(samples)
    save_model(MODELS_DIR / "kg_embedding_model.pkl", best_model)

    report = {
        "pipeline": "kg_embedding_pipeline",
        "run_at_utc": utc_now_iso(),
        "analytics_graph": str(graph_path),
        "embedding_method": "TruncatedSVD over a typed RDF adjacency matrix",
        "leakage_control": "Outcome aggregate measurement nodes are held out while node embeddings are generated.",
        "prediction_task": "Classify record-level heart-disease outcomes using only KG-derived node embeddings and graph indicator context.",
        "embedding_dimensions_per_node": EMBEDDING_DIMENSIONS,
        "feature_construction": "dataset embedding + population-group embedding + observed/risk/protective indicator embeddings + graph interactions",
        "max_record_samples": MAX_RECORD_SAMPLES,
        "graph": {
            "triples": int(len(graph)),
            "held_out_outcome_measurements": int(len(held_out_outcome_nodes)),
            "embedding_edges": int(len(edges)),
            "embedded_nodes": int(len(embeddings_frame)),
        },
        "samples": {
            "rows_total": int(len(samples)),
            "positive_labels": int(samples["target"].sum()),
            "negative_labels": int((samples["target"] == 0).sum()),
            "source_distribution": samples["source_dataset"].value_counts().to_dict(),
        },
        "candidate_models": model_scores,
        "selected_model": best_name,
        **evaluation,
        "artifacts": {
            "model": str(MODELS_DIR / "kg_embedding_model.pkl"),
            "node_embeddings": str(embeddings_path),
            "training_sample_audit": str(samples_path),
        },
    }
    save_json(REPORTS_DIR / "kg_embedding_report.json", report)
    log.info("KG embedding model report written to %s", REPORTS_DIR / "kg_embedding_report.json")


if __name__ == "__main__":
    main()
