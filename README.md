# BDA Practice 2 — Medical Data Pipeline with Knowledge Graphs

End-to-end Data Science pipeline that ingests medical datasets from Kaggle,
progressively refines them across five zones (Landing → Formatted → Trusted →
Exploitation → Analysis), generates a semantic RDF/RDFS **Knowledge Graph** for
cross-dataset analysis, and runs two analytical pipelines on top of it:

1. **SPARQL pattern-matching** queries over the KG.
2. **ML over KG embeddings**: graph-derived features feed a record-level
   classifier of the heart-disease outcome.

## Quick Start

### Prerequisites
- Python 3.11 or 3.12
- Java 17 (Spark 3.5 is incompatible with Java 25)
- Kaggle credentials (optional, only for re-downloading datasets)

### Setup

**Windows** (PowerShell):
```powershell
.\scripts\setup_env_windows.ps1
.\.venv-win\Scripts\Activate.ps1
```

**Linux / WSL**:
```bash
chmod +x scripts/setup_env.sh
./scripts/setup_env.sh
source .venv/bin/activate
```

See [ENVIRONMENT.md](ENVIRONMENT.md) for full details.

### Run the Complete Pipeline

If you already have Landing Zone snapshots:

```bash
python run_all_pipeline.py --skip-landing --strict
```

Including the Landing Zone (needs Kaggle credentials):

```bash
python run_all_pipeline.py --strict
```

Typical runtime: 8–15 minutes.

## Repository Layout

```
bda-practica2/
├── Part1_Landing_zone/          Data collectors (Kaggle, Airflow DAG)
├── Part2_Formatting_zone/       Structural formatting (Spark → DuckDB)
├── Part3_Trusted_zone/          Quality rules, denial constraints, cleaning
├── Part4_Exploitation_zone/     RDF/RDFS Knowledge Graph + DuckDB compat
├── Part5_Analysis_zone/         SPARQL analysis + KG embedding ML + baselines
├── notebooks/                   Validation, KG exploration, model explainability
├── scripts/                     Cross-platform setup helpers
├── run_all_pipeline.py          Single orchestrator (with --strict / --skip-landing)
└── spark_runtime.py             Spark Java / Hadoop runtime detection
```

## Architecture Overview

| Zone | Purpose | Engine | Output |
|------|---------|--------|--------|
| **Part1** Landing | Periodic download of raw Kaggle datasets | PySpark + Airflow DAG | Raw Parquet snapshots |
| **Part2** Formatted | Structural normalisation only (no cleaning) | PySpark + SparkSQL | DuckDB tables |
| **Part3** Trusted | Standardisation, denial constraints, quarantine | PySpark + SparkSQL | DuckDB + quality report |
| **Part4** Exploitation | RDF/RDFS KG generation + DuckDB compatibility table | PySpark + RDFLib | Turtle KG + DuckDB |
| **Part5** Analysis | SPARQL pipeline + KG embedding ML + ML baselines | RDFLib + scikit-learn | Reports, models, SPARQL queries |

### Addressing P1 Feedback

- **Formatted now only formats**: BMI, age conversion, gender normalisation and
  all derivations moved to Trusted.
- **Denial constraints formalised**: every quality rule is documented as a
  logical denial constraint of the form `¬∃ t : φ(t)` in
  [Part3_Trusted_zone/README.md](Part3_Trusted_zone/README.md).
- **No more parquet round-trip**: the Trusted Zone reads the Formatted DuckDB
  and the Exploitation Zone reads the Trusted DuckDB **directly through Arrow**
  into Spark, removing the intermediate disk writes that were called out in the
  P1 review (`read_formatted_tables`, `read_trusted_tables`).
- **Two truly different analyses**: SPARQL pattern matching versus ML on KG
  embeddings, instead of "both pipelines being inference models".

## Data Sources

Three cardiovascular health datasets from Kaggle:

- [`sulianova/cardiovascular-disease-dataset`](https://www.kaggle.com/datasets/sulianova/cardiovascular-disease-dataset)
- [`alexteboul/heart-disease-health-indicators-dataset`](https://www.kaggle.com/datasets/alexteboul/heart-disease-health-indicators-dataset)
- [`cherngs/heart-disease-cleveland-uci`](https://www.kaggle.com/datasets/cherngs/heart-disease-cleveland-uci)

## Knowledge Graph

- **Language**: RDF/RDFS.
- **Generator**: [RDFLib](https://rdflib.dev/).
- **Namespace**: `https://example.org/bda/health-risk/` (prefix `hr:`).
- **Schema**: see [Part4_Exploitation_zone/KG_METAMODEL.md](Part4_Exploitation_zone/KG_METAMODEL.md)
  and the visual diagram in [`kg_schema_visualization.html`](Part4_Exploitation_zone/kg_schema_visualization.html).
- **Integration strategy**: shared semantic concepts (age band, gender,
  population group, indicators, outcomes) — no false person-level merging.
- **External graph store**: optionally load the Turtle file into [GraphDB](https://www.ontotext.com/products/graphdb/).

## Analytical Pipelines (Part5)

### 1. SPARQL pattern matching (`kg_analysis_pipeline.py`)

Runs a curated set of SPARQL queries on the compact analytics graph:

- Indicators present in more than one dataset.
- Population groups ranked by heart-disease positive rate.
- Combined indicators per group (heart disease, hypertension, cholesterol).
- Indicator consistency across datasets (min / max / avg / spread).
- Outcome rate by age group across datasets.
- Risk-factor co-occurrence in positive heart-disease records (full KG).

Output: `Part5_Analysis_zone/reports/kg_analysis_report.json` and SPARQL files
in `Part4_Exploitation_zone/exploitation_zone/kg/sparql/`.

### 2. KG embedding ML (`kg_embedding_pipeline.py`)

- Builds a typed RDF triple set from the analytics graph.
- Holds out the outcome aggregate nodes during embedding generation to reduce
  label leakage.
- Learns canonical Knowledge Graph Embeddings with **PyKEEN** (TransE,
  DistMult, ComplEx), selecting the best model by MRR. Falls back to
  `TruncatedSVD` if PyKEEN is unavailable.
- Builds record-level samples from dataset, population-group, indicator and
  protective-factor node embeddings, plus graph interactions.
- Trains a `LogisticRegression` / `RandomForestClassifier` and selects the
  best by stratified-CV PR-AUC.

Output: `Part5_Analysis_zone/reports/kg_embedding_report.json`,
`models/kg_embedding_model.pkl`, plus the audit CSVs
`kg_node_embeddings.csv` and `kg_embedding_training_data.csv`.

### 3. Hybrid ML (`model_pipeline_hybrid.py`)

Combines the best tabular features with the KG node embeddings in a single
feature space, testing whether graph-derived semantic context complements raw
clinical indicators.

Output: `Part5_Analysis_zone/reports/hybrid_tabular_kg_report.json` and
`models/hybrid_tabular_kg_model.pkl`.

### 4. Baseline ML (`analysis_pipeline.py`)

Two integrated baselines on the same exploitation table:

- **Integrated Core**: small, robust feature set (age, BMI, gender, risk flags).
- **Integrated Enriched**: adds clinical features (BP, mental/physical
  unhealthy days, max heart rate, etc.).

These act as references for the KG embedding model and demonstrate that the
KG carries comparable predictive signal on its own.

### 5. Model comparison (`model_comparison_pipeline.py`)

Reads the individual pipeline reports and produces a unified comparison
(`model_comparison_report.json`) of tabular, KG-only and hybrid models,
quantifying the added value of graph-derived features.

## Notebooks

Four Jupyter notebooks present the project narratively and reproducibly:

1. [`notebooks/01_data_pipeline_validation.ipynb`](notebooks/01_data_pipeline_validation.ipynb)
   — Trusted and Exploitation tables, quality reports, integration coverage.
2. [`notebooks/02_knowledge_graph_exploration.ipynb`](notebooks/02_knowledge_graph_exploration.ipynb)
   — RDF/RDFS schema, KG structure summary, SPARQL queries with charts.
3. [`notebooks/03_model_comparison_and_explainability.ipynb`](notebooks/03_model_comparison_and_explainability.ipynb)
   — Baseline vs KG-embedding model comparison, confusion matrices, feature
   importance, 2D node-embedding projection.
4. [`notebooks/04_hybrid_model_and_full_comparison.ipynb`](notebooks/04_hybrid_model_and_full_comparison.ipynb)
   — Full comparison of tabular, KG-only and hybrid pipelines, with the
   improvement analysis of adding the KG on top of the tabular baseline.

## Main Outputs

After a successful run:

```text
Part2_Formatting_zone/formatted_zone/formatted.duckdb
Part3_Trusted_zone/trusted_zone/trusted.duckdb
Part4_Exploitation_zone/exploitation_zone/exploitation.duckdb
Part4_Exploitation_zone/exploitation_zone/kg/health_risk_kg.ttl
Part4_Exploitation_zone/exploitation_zone/kg/health_risk_analytics_kg.ttl
Part4_Exploitation_zone/exploitation_zone/kg/schema/health_risk_schema.ttl
Part4_Exploitation_zone/exploitation_zone/kg/sparql/*.rq
Part4_Exploitation_zone/exploitation_zone/kg/kg_manifest.json
Part5_Analysis_zone/models/integrated_core_model.pkl
Part5_Analysis_zone/models/integrated_enriched_model.pkl
Part5_Analysis_zone/models/kg_embedding_model.pkl
Part5_Analysis_zone/models/hybrid_tabular_kg_model.pkl
Part5_Analysis_zone/reports/integrated_core_report.json
Part5_Analysis_zone/reports/integrated_enriched_report.json
Part5_Analysis_zone/reports/kg_embedding_report.json
Part5_Analysis_zone/reports/hybrid_tabular_kg_report.json
Part5_Analysis_zone/reports/model_comparison_report.json
Part5_Analysis_zone/reports/kg_analysis_report.json
Part5_Analysis_zone/reports/summary_report.json
Part5_Analysis_zone/reports/kg_node_embeddings.csv
```

## Zone Documentation

- [Part1: Landing Zone](Part1_Landing_zone/README.md)
- [Part2: Formatting Zone](Part2_Formatting_zone/README.md)
- [Part3: Trusted Zone](Part3_Trusted_zone/README.md)
- [Part4: Exploitation Zone](Part4_Exploitation_zone/README.md)
- [Part4: KG Meta-model](Part4_Exploitation_zone/KG_METAMODEL.md)
- [Part5: Analysis Zone](Part5_Analysis_zone/README.md)
