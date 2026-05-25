import logging
import json
import sys

from analysis_config import EXPLOITATION_DUCKDB_PATH, FIGURES_DIR, MODELS_DIR, REPORTS_DIR
from analysis_utils import (
    ensure_output_dirs,
    load_risk_model_input,
    save_json,
    save_model,
    utc_now_iso,
)
from model_pipeline_integrated_core import run_integrated_core_pipeline
from model_pipeline_integrated_enriched import run_integrated_enriched_pipeline


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
log = logging.getLogger(__name__)


def main() -> None:
    if not EXPLOITATION_DUCKDB_PATH.exists():
        raise FileNotFoundError(
            f"Exploitation DB not found at {EXPLOITATION_DUCKDB_PATH}. Run Part4 first."
        )

    ensure_output_dirs(MODELS_DIR, REPORTS_DIR, FIGURES_DIR)

    integrated_df = load_risk_model_input(EXPLOITATION_DUCKDB_PATH)

    source_names = set(integrated_df["source_dataset"].dropna().unique().tolist())
    required_sources = {
        "cardiovascular_disease",
        "heart_disease_health_indicators",
        "heart_disease_cleveland",
    }
    missing_sources = sorted(required_sources - source_names)
    if missing_sources:
        raise ValueError(
            f"Missing integrated sources in exploitation.risk_model_input: {missing_sources}"
        )

    log.info("Running integrated core pipeline")
    integrated_core_report = run_integrated_core_pipeline(
        integrated_df,
        REPORTS_DIR,
        MODELS_DIR,
        save_json,
        save_model,
    )

    log.info("Running integrated enriched pipeline")
    integrated_enriched_report = run_integrated_enriched_pipeline(
        integrated_df,
        REPORTS_DIR,
        MODELS_DIR,
        save_json,
        save_model,
    )

    kg_embedding_report_path = REPORTS_DIR / "kg_embedding_report.json"
    kg_embedding_summary = None
    if kg_embedding_report_path.exists():
        kg_embedding_report = json.loads(kg_embedding_report_path.read_text(encoding="utf-8"))
        kg_embedding_summary = {
            "pipeline": kg_embedding_report.get("pipeline"),
            "selected_model": kg_embedding_report.get("selected_model"),
            "rows_train": kg_embedding_report.get("rows_train"),
            "rows_test": kg_embedding_report.get("rows_test"),
            "test_metrics": kg_embedding_report.get("test_metrics"),
        }

    summary = {
        "run_at_utc": utc_now_iso(),
        "pipelines": {
            "integrated_core": integrated_core_report,
            "integrated_enriched": integrated_enriched_report,
            "kg_embedding": kg_embedding_summary,
        },
        "artifacts": {
            "models": [
                "integrated_core_model.pkl",
                "integrated_enriched_model.pkl",
                "kg_embedding_model.pkl",
            ],
            "reports": [
                "integrated_core_report.json",
                "integrated_enriched_report.json",
                "kg_embedding_report.json",
                "kg_analysis_report.json",
                "summary_report.json",
            ],
        },
    }

    save_json(REPORTS_DIR / "summary_report.json", summary)
    log.info("Analysis Zone completed successfully.")


if __name__ == "__main__":
    main()
