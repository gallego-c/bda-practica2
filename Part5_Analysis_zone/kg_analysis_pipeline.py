import json
import logging
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from rdflib import Graph

from analysis_config import PROJECT_ROOT, REPORTS_DIR


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
log = logging.getLogger(__name__)


KG_MANIFEST_PATH = PROJECT_ROOT / "Part4_Exploitation_zone" / "exploitation_zone" / "kg" / "kg_manifest.json"


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def term_to_json(value: Any) -> Any:
    if value is None:
        return None
    if hasattr(value, "toPython"):
        converted = value.toPython()
        if isinstance(converted, Path):
            return str(converted)
        return converted
    return str(value)


def load_manifest() -> dict:
    if not KG_MANIFEST_PATH.exists():
        raise FileNotFoundError(f"KG manifest not found at {KG_MANIFEST_PATH}. Run Part4 first.")
    return json.loads(KG_MANIFEST_PATH.read_text(encoding="utf-8"))


def run_query(graph: Graph, query_path: Path) -> dict:
    query_text = query_path.read_text(encoding="utf-8")
    results = graph.query(query_text)
    rows = [
        {str(var): term_to_json(row[var]) for var in results.vars}
        for row in results
    ]
    return {
        "query": query_path.name,
        "rows_returned": len(rows),
        "rows": rows,
    }


def main() -> None:
    REPORTS_DIR.mkdir(parents=True, exist_ok=True)
    manifest = load_manifest()
    analytics_graph_path = Path(manifest["storage"]["analytics_graph_file"])
    if not analytics_graph_path.exists():
        raise FileNotFoundError(f"KG analytics graph not found at {analytics_graph_path}. Run Part4 first.")

    graph = Graph()
    graph.parse(str(analytics_graph_path), format="turtle")
    log.info("Loaded analytics KG with %s triples", f"{len(graph):,}")

    sparql_dir = Path(manifest["storage"]["sparql_query_dir"])
    analytical_queries = [
        sparql_dir / "02_indicators_shared_by_datasets.rq",
        sparql_dir / "03_rank_population_groups_by_outcome_rate.rq",
        sparql_dir / "04_combine_indicators_same_group.rq",
    ]

    report = {
        "run_at_utc": utc_now_iso(),
        "kg_manifest": str(KG_MANIFEST_PATH),
        "analytics_graph": str(analytics_graph_path),
        "triple_count": len(graph),
        "queries": [run_query(graph, query_path) for query_path in analytical_queries],
    }

    out_path = REPORTS_DIR / "kg_analysis_report.json"
    out_path.write_text(json.dumps(report, indent=2, default=str), encoding="utf-8")
    log.info("KG analysis report written to %s", out_path)


if __name__ == "__main__":
    main()
