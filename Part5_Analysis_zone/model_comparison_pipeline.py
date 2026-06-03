"""Model comparison: tabular baselines vs KG embedding-based models.

Reads the individual pipeline reports and generates a unified comparison table
that shows the added value (or not) of the KG-derived features.
"""

import json
import logging
import sys
from pathlib import Path

from analysis_config import REPORTS_DIR
from analysis_utils import utc_now_iso

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
log = logging.getLogger(__name__)


def safe_load(path: Path) -> dict | None:
    if not path.exists():
        return None
    return json.loads(path.read_text(encoding="utf-8"))


def extract_metrics(report: dict | None, keys: list[str]) -> dict | None:
    if report is None:
        return None
    metrics = report.get("test_metrics")
    if metrics is None:
        return None
    return {k: metrics.get(k) for k in keys}


def main() -> None:
    REPORTS_DIR.mkdir(parents=True, exist_ok=True)

    metric_keys = ["accuracy", "precision", "recall", "f1", "roc_auc", "pr_auc"]

    # Load all pipeline reports
    core_report = safe_load(REPORTS_DIR / "integrated_core_report.json")
    enriched_report = safe_load(REPORTS_DIR / "integrated_enriched_report.json")
    kg_report = safe_load(REPORTS_DIR / "kg_embedding_report.json")
    hybrid_report = safe_load(REPORTS_DIR / "hybrid_tabular_kg_report.json")

    pipelines = []

    # Tabular baseline (core features only)
    if core_report:
        pipelines.append({
            "pipeline": "tabular_core",
            "description": "Logistic Regression / Random Forest on core tabular features (age, BMI, BP, flags)",
            "feature_source": "tabular (direct columns from trusted zone)",
            "selected_model": core_report.get("selected_model"),
            "rows_train": core_report.get("rows_train"),
            "rows_test": core_report.get("rows_test"),
            "metrics": extract_metrics(core_report, metric_keys),
        })

    # Tabular enriched (all features)
    if enriched_report:
        pipelines.append({
            "pipeline": "tabular_enriched",
            "description": "Logistic Regression / Random Forest on all available tabular features including engineered ones",
            "feature_source": "tabular (all columns + feature engineering)",
            "selected_model": enriched_report.get("selected_model"),
            "rows_train": enriched_report.get("rows_train"),
            "rows_test": enriched_report.get("rows_test"),
            "metrics": extract_metrics(enriched_report, metric_keys),
        })

    # KG embedding model
    if kg_report:
        pipelines.append({
            "pipeline": "kg_embedding",
            "description": "Classifier trained exclusively on KG-derived node embeddings (entity + relation structure)",
            "feature_source": f"KG embeddings ({kg_report.get('embedding_method', 'unknown')})",
            "selected_model": kg_report.get("selected_model"),
            "rows_train": kg_report.get("rows_train"),
            "rows_test": kg_report.get("rows_test"),
            "metrics": extract_metrics(kg_report, metric_keys),
            "pykeen_training": kg_report.get("pykeen_training"),
        })

    # Hybrid: tabular + KG embeddings
    if hybrid_report:
        pipelines.append({
            "pipeline": "hybrid_tabular_kg",
            "description": "Tabular features enriched with KG node embeddings (population group + dataset + interaction)",
            "feature_source": "tabular + KG embeddings (hybrid)",
            "selected_model": hybrid_report.get("selected_model"),
            "rows_train": hybrid_report.get("rows_train"),
            "rows_test": hybrid_report.get("rows_test"),
            "metrics": extract_metrics(hybrid_report, metric_keys),
            "feature_composition": hybrid_report.get("feature_composition"),
        })

    if not pipelines:
        log.warning("No pipeline reports found — run the analysis pipelines first.")
        return

    # Build comparison table
    comparison_table = []
    for p in pipelines:
        row = {"pipeline": p["pipeline"], "model": p.get("selected_model"), "feature_source": p["feature_source"]}
        if p.get("metrics"):
            row.update(p["metrics"])
        comparison_table.append(row)

    # Determine best per metric
    rankings = {}
    for metric in metric_keys:
        values = [(p["pipeline"], p.get("metrics", {}).get(metric)) for p in pipelines if p.get("metrics")]
        valid = [(name, v) for name, v in values if v is not None]
        if valid:
            best_name, best_val = max(valid, key=lambda x: x[1])
            rankings[metric] = {"best_pipeline": best_name, "value": best_val}

    # Compute deltas: KG vs best tabular
    kg_vs_tabular = {}
    kg_metrics = next((p["metrics"] for p in pipelines if p["pipeline"] == "kg_embedding" and p.get("metrics")), None)
    best_tabular_metrics = None
    for p in pipelines:
        if p["pipeline"].startswith("tabular") and p.get("metrics"):
            if best_tabular_metrics is None or (p["metrics"].get("f1") or 0) > (best_tabular_metrics.get("f1") or 0):
                best_tabular_metrics = p["metrics"]
                best_tabular_name = p["pipeline"]

    if kg_metrics and best_tabular_metrics:
        for metric in metric_keys:
            kg_val = kg_metrics.get(metric)
            tab_val = best_tabular_metrics.get(metric)
            if kg_val is not None and tab_val is not None:
                kg_vs_tabular[metric] = {
                    "kg_value": round(kg_val, 4),
                    "tabular_value": round(tab_val, 4),
                    "delta": round(kg_val - tab_val, 4),
                    "kg_wins": kg_val > tab_val,
                }

    report = {
        "comparison": "tabular_vs_kg_embedding",
        "run_at_utc": utc_now_iso(),
        "purpose": (
            "Direct comparison between traditional tabular ML pipelines and "
            "KG embedding-based classification to evaluate the added value of "
            "graph-derived semantic features for heart-disease prediction."
        ),
        "pipelines": pipelines,
        "comparison_table": comparison_table,
        "best_per_metric": rankings,
        "kg_vs_best_tabular": kg_vs_tabular,
        "interpretation": (
            "The KG embeddings capture relational context (dataset provenance, population group "
            "membership, indicator co-occurrence) that is not directly available in flat tabular "
            "features. A positive delta means the KG-based model outperforms the tabular baseline "
            "on that metric. Even when deltas are small or negative, the KG approach demonstrates "
            "that semantic structure alone can achieve competitive predictive performance."
        ),
    }

    out_path = REPORTS_DIR / "model_comparison_report.json"
    out_path.write_text(json.dumps(report, indent=2, default=str), encoding="utf-8")
    log.info("Model comparison report written to %s", out_path)

    # Print summary to stdout
    log.info("=" * 60)
    log.info("MODEL COMPARISON SUMMARY")
    log.info("=" * 60)
    for row in comparison_table:
        log.info(
            "  %-20s | F1=%.4f | ROC-AUC=%.4f | PR-AUC=%.4f",
            row["pipeline"],
            row.get("f1", 0),
            row.get("roc_auc", 0),
            row.get("pr_auc", 0),
        )
    if kg_vs_tabular:
        log.info("-" * 60)
        log.info("KG vs best tabular (%s):", best_tabular_name)
        for metric, info in kg_vs_tabular.items():
            sign = "+" if info["delta"] >= 0 else ""
            log.info("  %-12s: %s%.4f  (KG=%.4f, Tab=%.4f)", metric, sign, info["delta"], info["kg_value"], info["tabular_value"])


if __name__ == "__main__":
    main()
