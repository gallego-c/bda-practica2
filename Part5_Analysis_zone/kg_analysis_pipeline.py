"""SPARQL-based graph analysis pipeline.

Operates directly on the RDF/RDFS Knowledge Graph produced by the Exploitation
Zone. Demonstrates graph-native value through pattern-matching queries that
would be cumbersome over flat tables.

The pipeline runs two families of queries:

- Aggregate-level queries on the compact ``health_risk_analytics_kg.ttl``:
  indicator consistency, ranking population groups by outcome rate, and
  combining indicators per group.
- Record-level queries on the full ``health_risk_kg.ttl``: population groups
  linked to multiple datasets and risk-factor co-occurrence in positive
  records.
"""

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


KG_ROOT = PROJECT_ROOT / "Part4_Exploitation_zone" / "exploitation_zone" / "kg"
KG_MANIFEST_PATH = KG_ROOT / "kg_manifest.json"

# Queries that only need the compact analytics graph (aggregate measurements,
# datasets, indicators, population groups, outcomes, age groups, genders).
ANALYTICS_QUERIES = [
    "02_indicators_shared_by_datasets.rq",
    "03_rank_population_groups_by_outcome_rate.rq",
    "04_combine_indicators_same_group.rq",
    "06_indicator_consistency_across_datasets.rq",
    "07_outcome_rate_by_age_group.rq",
]
# Queries that need the full graph (HealthRecord, hasObservedRiskFactor, ...).
FULL_GRAPH_QUERIES = [
    "05_population_groups_connected_to_multiple_sources.rq",
    "08_top_risk_factor_cooccurrence.rq",
]

QUERY_DESCRIPTIONS = {
    "02_indicators_shared_by_datasets.rq": "Indicators appearing in measurements from more than one source dataset",
    "03_rank_population_groups_by_outcome_rate.rq": "Population groups with the highest heart-disease positive rate (n>=25)",
    "04_combine_indicators_same_group.rq": "Heart disease, blood pressure and cholesterol rates per group and dataset",
    "05_population_groups_connected_to_multiple_sources.rq": "Population groups linked to records from multiple datasets",
    "06_indicator_consistency_across_datasets.rq": "Indicator positive rates across datasets: min / max / avg / spread",
    "07_outcome_rate_by_age_group.rq": "Average outcome rate per age group and dataset",
    "08_top_risk_factor_cooccurrence.rq": "Risk-factor pairs most frequently co-occurring in positive heart-disease records",
}


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


def parse_format(path: Path) -> str:
    return "nt" if path.suffix.lower() == ".nt" else "turtle"


def load_graph(graph_path: Path) -> Graph:
    if not graph_path.exists():
        raise FileNotFoundError(f"KG file not found at {graph_path}. Run Part4 first.")
    graph = Graph()
    graph.parse(str(graph_path), format=parse_format(graph_path))
    return graph


def run_query(graph: Graph, query_path: Path, source_label: str, row_cap: int = 200) -> dict:
    query_text = query_path.read_text(encoding="utf-8")
    results = graph.query(query_text)
    rows = [
        {str(var): term_to_json(row[var]) for var in results.vars}
        for row in results
    ]
    return {
        "query": query_path.name,
        "description": QUERY_DESCRIPTIONS.get(query_path.name, ""),
        "graph": source_label,
        "rows_returned": len(rows),
        "rows": rows[:row_cap],
        "row_cap_applied": len(rows) > row_cap,
    }


def graph_structure_summary(graph: Graph) -> dict:
    """Cheap summary of graph counts that helps reviewers understand graph density."""
    summaries: dict[str, int] = {}
    for label, sparql in {
        "datasets": "SELECT (COUNT(DISTINCT ?d) AS ?n) WHERE { ?d a hr:Dataset }",
        "indicators": "SELECT (COUNT(DISTINCT ?i) AS ?n) WHERE { ?i a hr:Indicator }",
        "population_groups": "SELECT (COUNT(DISTINCT ?g) AS ?n) WHERE { ?g a hr:PopulationGroup }",
        "age_groups": "SELECT (COUNT(DISTINCT ?g) AS ?n) WHERE { ?g a hr:AgeGroup }",
        "outcomes": "SELECT (COUNT(DISTINCT ?o) AS ?n) WHERE { ?o a hr:Outcome }",
        "aggregate_measurements": "SELECT (COUNT(DISTINCT ?m) AS ?n) WHERE { ?m a hr:AggregateMeasurement }",
    }.items():
        query = f"PREFIX hr: <https://example.org/bda/health-risk/> {sparql}"
        result = list(graph.query(query))
        summaries[label] = int(result[0][0]) if result else 0
    return summaries


def main() -> None:
    REPORTS_DIR.mkdir(parents=True, exist_ok=True)
    manifest = load_manifest()

    analytics_path = Path(manifest["storage"]["analytics_graph_file"])
    full_path = Path(manifest["storage"]["graph_file"])
    sparql_dir = Path(manifest["storage"]["sparql_query_dir"])

    log.info("Loading analytics graph: %s", analytics_path)
    analytics_graph = load_graph(analytics_path)
    log.info("Analytics KG loaded: %s triples", f"{len(analytics_graph):,}")

    structure = graph_structure_summary(analytics_graph)
    log.info("Graph structure: %s", structure)

    query_results = []
    for name in ANALYTICS_QUERIES:
        query_path = sparql_dir / name
        if not query_path.exists():
            log.warning("Skipping missing query: %s", query_path)
            continue
        log.info("Running SPARQL query on analytics graph: %s", name)
        query_results.append(run_query(analytics_graph, query_path, "analytics"))

    log.info("Loading full graph: %s", full_path)
    full_graph = load_graph(full_path)
    log.info("Full KG loaded: %s triples", f"{len(full_graph):,}")

    for name in FULL_GRAPH_QUERIES:
        query_path = sparql_dir / name
        if not query_path.exists():
            log.warning("Skipping missing query: %s", query_path)
            continue
        log.info("Running SPARQL query on full graph: %s", name)
        query_results.append(run_query(full_graph, query_path, "full"))

    report = {
        "pipeline": "kg_analysis_pipeline",
        "run_at_utc": utc_now_iso(),
        "kg_manifest": str(KG_MANIFEST_PATH),
        "analytics_graph": str(analytics_path),
        "full_graph": str(full_path),
        "analytics_triple_count": len(analytics_graph),
        "full_triple_count": len(full_graph),
        "graph_structure": structure,
        "queries": query_results,
    }

    out_path = REPORTS_DIR / "kg_analysis_report.json"
    out_path.write_text(json.dumps(report, indent=2, default=str), encoding="utf-8")
    log.info("KG analysis report written to %s", out_path)


if __name__ == "__main__":
    main()
