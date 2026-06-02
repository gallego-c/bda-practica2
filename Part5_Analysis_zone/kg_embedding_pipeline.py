"""KG Embedding pipeline using PyKEEN (TransE, DistMult, ComplEx).

Generates Knowledge Graph Embeddings from the RDF graph using standard KGE
methods via the PyKEEN library. Falls back to TruncatedSVD if PyKEEN is
unavailable or training fails.

The embeddings are then used as features for a downstream heart-disease
classification task, demonstrating the value of graph-derived representations.
"""

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

# ---------------------------------------------------------------------------
# Try to import PyKEEN for proper KGE methods
# ---------------------------------------------------------------------------
PYKEEN_AVAILABLE = False
try:
    import torch
    from pykeen.triples import TriplesFactory
    from pykeen.pipeline import pipeline as pykeen_pipeline
    PYKEEN_AVAILABLE = True
    log.info("PyKEEN available — will use TransE/DistMult/ComplEx for KG embeddings.")
except ImportError:
    log.warning("PyKEEN not available — falling back to TruncatedSVD embeddings.")

BASE_URI = "https://example.org/bda/health-risk/"
HR = URIRef(BASE_URI)
KG_ROOT = PROJECT_ROOT / "Part4_Exploitation_zone" / "exploitation_zone" / "kg"
KG_MANIFEST_PATH = KG_ROOT / "kg_manifest.json"
DEFAULT_ANALYTICS_GRAPH_PATH = KG_ROOT / "health_risk_analytics_kg.ttl"
OUTCOME_INDICATOR = URIRef(BASE_URI + "indicator/heart-disease-outcome")
MIN_OBSERVATIONS_PER_SAMPLE = 25
EMBEDDING_DIMENSIONS = 32
MAX_RECORD_SAMPLES = 60000

# PyKEEN training configuration
PYKEEN_MODELS = ["TransE", "DistMult", "ComplEx"]
PYKEEN_EPOCHS = 100
PYKEEN_EMBEDDING_DIM = 32

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


def collect_triples_for_pykeen(graph: Graph, held_out_nodes: set[URIRef]) -> list[tuple[str, str, str]]:
    """Extract (head, relation, tail) triples suitable for PyKEEN."""
    triples = []
    for subject, predicate, obj in graph:
        if subject in held_out_nodes or obj in held_out_nodes:
            continue
        if not isinstance(subject, URIRef) or not isinstance(obj, URIRef):
            continue
        triples.append((str(subject), str(predicate), str(obj)))
    return triples


def train_pykeen_embeddings(
    triples: list[tuple[str, str, str]], dimensions: int, model_name: str = "TransE"
) -> tuple[dict[str, np.ndarray], dict[str, Any]]:
    """Train a KGE model using PyKEEN and return entity embeddings."""
    from pykeen.triples import TriplesFactory
    from pykeen.pipeline import pipeline as pykeen_pipeline
    import torch

    log.info("Training PyKEEN model: %s (dim=%d, epochs=%d)", model_name, dimensions, PYKEEN_EPOCHS)

    triples_array = np.array(triples, dtype=str)
    tf = TriplesFactory.from_labeled_triples(triples_array)

    # Train/test split for PyKEEN internal evaluation
    training, testing = tf.split([0.9, 0.1], random_state=RANDOM_SEED)

    result = pykeen_pipeline(
        training=training,
        testing=testing,
        model=model_name,
        model_kwargs={"embedding_dim": dimensions},
        training_kwargs={"num_epochs": PYKEEN_EPOCHS, "batch_size": 256},
        optimizer_kwargs={"lr": 0.01},
        random_seed=RANDOM_SEED,
        device="cpu",
    )

    # Extract entity embeddings
    entity_representation = result.model.entity_representations[0]
    entity_embeddings_tensor = entity_representation(
        indices=torch.arange(tf.num_entities)
    ).detach().numpy()

    entity_to_id = tf.entity_to_id
    embeddings: dict[str, np.ndarray] = {}
    for entity_label, entity_id in entity_to_id.items():
        embeddings[entity_label] = entity_embeddings_tensor[entity_id]

    # Collect training metadata
    training_info = {
        "model": model_name,
        "embedding_dim": dimensions,
        "epochs": PYKEEN_EPOCHS,
        "num_entities": tf.num_entities,
        "num_relations": tf.num_relations,
        "num_training_triples": training.num_triples,
        "num_testing_triples": testing.num_triples,
        "hits_at_10": float(result.metric_results.get_metric("hits@10") or 0.0),
        "mean_rank": float(result.metric_results.get_metric("mean_rank") or 0.0),
        "mrr": float(result.metric_results.get_metric("inverse_harmonic_mean_rank") or 0.0),
    }
    log.info(
        "PyKEEN %s training done: entities=%d, relations=%d, MRR=%.4f, Hits@10=%.4f",
        model_name, tf.num_entities, tf.num_relations,
        training_info["mrr"], training_info["hits_at_10"],
    )
    return embeddings, training_info


def train_best_pykeen_model(
    triples: list[tuple[str, str, str]], dimensions: int
) -> tuple[dict[str, np.ndarray], dict[str, Any]]:
    """Try multiple PyKEEN models and keep the one with the best MRR."""
    best_embeddings = None
    best_info = None
    best_mrr = -1.0

    for model_name in PYKEEN_MODELS:
        try:
            embeddings, info = train_pykeen_embeddings(triples, dimensions, model_name)
            mrr = info.get("mrr", 0.0)
            if mrr > best_mrr:
                best_mrr = mrr
                best_embeddings = embeddings
                best_info = info
        except Exception as exc:
            log.warning("PyKEEN model %s failed: %s — skipping", model_name, exc)

    if best_embeddings is None:
        raise RuntimeError("All PyKEEN models failed.")

    log.info("Best KGE model: %s (MRR=%.4f)", best_info["model"], best_mrr)
    return best_embeddings, best_info


def build_node_embeddings_pykeen(
    triples: list[tuple[str, str, str]], dimensions: int
) -> tuple[pd.DataFrame, dict[str, np.ndarray], dict[str, Any]]:
    """Build node embeddings using PyKEEN KGE models."""
    embeddings, training_info = train_best_pykeen_model(triples, dimensions)

    node_names = sorted(embeddings.keys())
    columns = [f"kg_emb_{i:02d}" for i in range(dimensions)]
    rows = [embeddings[node] for node in node_names]
    frame = pd.DataFrame(rows, columns=columns)
    frame.insert(0, "node", node_names)
    return frame, embeddings, training_info


def build_node_embeddings_svd(edges: list[tuple[str, str]], dimensions: int) -> tuple[pd.DataFrame, dict[str, np.ndarray]]:
    """Fallback: build node embeddings using TruncatedSVD over adjacency matrix."""
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

    # Try PyKEEN first, fall back to SVD
    embedding_method = "svd_fallback"
    pykeen_training_info: dict[str, Any] = {}

    if PYKEEN_AVAILABLE:
        try:
            triples = collect_triples_for_pykeen(graph, held_out_outcome_nodes)
            log.info("Collected %d triples for PyKEEN training", len(triples))
            embeddings_frame, embeddings, pykeen_training_info = build_node_embeddings_pykeen(
                triples, PYKEEN_EMBEDDING_DIM
            )
            embedding_method = f"pykeen_{pykeen_training_info['model']}"
            log.info("KG embeddings generated via PyKEEN (%s)", pykeen_training_info["model"])
        except Exception as exc:
            log.warning("PyKEEN training failed (%s), falling back to SVD.", exc)
            pass  # will fall through to SVD below
    
    if embedding_method == "svd_fallback":
        edges = collect_embedding_edges(graph, held_out_outcome_nodes)
        embeddings_frame, embeddings = build_node_embeddings_svd(edges, EMBEDDING_DIMENSIONS)
        log.info("KG embeddings generated via TruncatedSVD (fallback).")

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
        "embedding_method": embedding_method,
        "embedding_method_detail": (
            f"PyKEEN {pykeen_training_info.get('model', '')} — a standard KG embedding method that learns "
            "entity and relation representations by optimizing a scoring function over (h, r, t) triples."
            if pykeen_training_info
            else "TruncatedSVD over normalized adjacency matrix (fallback when PyKEEN unavailable)."
        ),
        "pykeen_training": pykeen_training_info if pykeen_training_info else None,
        "leakage_control": "Outcome aggregate measurement nodes are held out while node embeddings are generated.",
        "prediction_task": "Classify record-level heart-disease outcomes using only KG-derived node embeddings and graph indicator context.",
        "embedding_dimensions_per_node": PYKEEN_EMBEDDING_DIM if pykeen_training_info else EMBEDDING_DIMENSIONS,
        "feature_construction": "dataset embedding + population-group embedding + observed/risk/protective indicator embeddings + graph interactions",
        "max_record_samples": MAX_RECORD_SAMPLES,
        "graph": {
            "triples": int(len(graph)),
            "held_out_outcome_measurements": int(len(held_out_outcome_nodes)),
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
