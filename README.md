# BDA Practice 2: Medical Data Pipeline with Knowledge Graph

A comprehensive data pipeline that ingests medical datasets from Kaggle, performs progressive data refinement across five zones, and generates a semantic Knowledge Graph for cross-dataset analysis and predictive modeling.

## 🚀 Quick Start

### Prerequisites
- Python 3.11 or 3.12
- Java 17 (not Java 25)
- PySpark 3.5.0
- Kaggle credentials (optional, for new downloads)

### Setup (Choose your OS)

**Windows:**
```powershell
.\scripts\setup_env_windows.ps1
```

**Linux/WSL:**
```bash
chmod +x scripts/setup_env.sh
./scripts/setup_env.sh
source .venv/bin/activate
```

### Run the Complete Pipeline
```bash
python run_all_pipeline.py --skip-landing --strict
```

See [ENVIRONMENT.md](ENVIRONMENT.md) for detailed setup instructions.

## 📊 Repository Structure

The project is organized as a five-zone data architecture:

```
bda-practica2/
├── Part1_Landing_zone/         Data ingestion from Kaggle
├── Part2_Formatting_zone/       Structural normalization
├── Part3_Trusted_zone/          Data quality and standardization
├── Part4_Exploitation_zone/     Knowledge Graph generation
├── Part5_Analysis_zone/         Predictive models and KG analysis
├── notebooks/                   Validation and visualization
└── scripts/                      Setup helpers
```

## 🏗️ Architecture Overview

Each zone has a specific responsibility:

| Zone | Purpose | Output |
|------|---------|--------|
| **Part1: Landing** | Download raw datasets from Kaggle | Raw Parquet snapshots |
| **Part2: Formatting** | Normalize column names and structure | Structured DuckDB tables |
| **Part3: Trusted** | Quality rules, cleaning, validation | Clean trusted datasets |
| **Part4: Exploitation** | RDF/RDFS Knowledge Graph generation | KG + compatibility tables |
| **Part5: Analysis** | ML pipelines + SPARQL graph queries | Models, predictions, reports |

## 📥 Data Sources

Three integrated cardiovascular health datasets:
- **Cardiovascular Disease Dataset** (`sulianova/cardiovascular-disease-dataset`)
- **Heart Disease Health Indicators** (`alexteboul/heart-disease-health-indicators-dataset`)
- **Heart Disease Cleveland** (`cherngs/heart-disease-cleveland-uci`)

## 🧠 Knowledge Graph

The Exploitation Zone creates an RDF/RDFS semantic layer using:
- **Namespace:** `https://example.org/bda/health-risk/`
- **Schema:** RDF/RDFS classes and properties
- **Query Language:** SPARQL
- **Technology:** RDFLib for generation, optional GraphDB for external storage

Datasets are integrated through **shared semantic concepts** (age group, gender, indicators, outcomes) rather than false person-level merging.

See [Part4_Exploitation_zone/KG_METAMODEL.md](Part4_Exploitation_zone/KG_METAMODEL.md) for detailed schema documentation.

## 📖 Zone Documentation

Each zone has detailed usage instructions in its README:
- [Part1: Landing Zone](Part1_Landing_zone/README.md)
- [Part2: Formatting Zone](Part2_Formatting_zone/README.md)
- [Part3: Trusted Zone](Part3_Trusted_zone/README.md)
- [Part4: Exploitation Zone](Part4_Exploitation_zone/README.md)
- [Part5: Analysis Zone](Part5_Analysis_zone/README.md)

## 🛠️ Key Files

- `run_all_pipeline.py` - Main orchestrator, runs all zones in sequence
- `spark_runtime.py` - Spark session manager
- `notebooks/` - Jupyter validation notebooks

## 💾 Data Storage

- **DuckDB** databases in each zone for efficient columnar access
- **Parquet** versioned snapshots for traceability
- **Turtle (.ttl) files** for the Knowledge Graph

```bash
sudo apt update
sudo apt install -y python3-venv openjdk-17-jdk
export JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
```

```bash
python -m venv .venv
source .venv/bin/activate
python -m pip install --upgrade pip
python -m pip install -r requirements.txt
```

### Windows setup

Use a Windows Python environment and install the same requirements. On Windows, `requirements.txt` installs `jdk4py==17.0.9.2` so Spark can use a local Java 17 runtime when `JAVA_HOME` is not already set.

```powershell
.\scripts\setup_env_windows.ps1
```

The helper creates `.venv-win`, installs `requirements.txt`, and downloads Hadoop native binaries for Spark local file access:

```text
.hadoop/bin/winutils.exe
.hadoop/bin/hadoop.dll
```

These files are local runtime dependencies and are ignored by Git.

## Run the full pipeline

If landing snapshots already exist:

```bash
python run_all_pipeline.py --skip-landing --strict
```

Including Landing Zone:

```bash
python run_all_pipeline.py --strict
```

## Main outputs

- Formatted DB:
  `Part2_Formatting_zone/formatted_zone/formatted.duckdb`
- Trusted DB:
  `Part3_Trusted_zone/trusted_zone/trusted.duckdb`
- Exploitation DB:
  `Part4_Exploitation_zone/exploitation_zone/exploitation.duckdb`
- Full KG:
  `Part4_Exploitation_zone/exploitation_zone/kg/health_risk_kg.ttl`
- Compact analytics KG:
  `Part4_Exploitation_zone/exploitation_zone/kg/health_risk_analytics_kg.ttl`
- RDFS schema:
  `Part4_Exploitation_zone/exploitation_zone/kg/schema/health_risk_schema.ttl`
- SPARQL examples:
  `Part4_Exploitation_zone/exploitation_zone/kg/sparql/`
- KG manifest:
  `Part4_Exploitation_zone/exploitation_zone/kg/kg_manifest.json`
- KG analysis report:
  `Part5_Analysis_zone/reports/kg_analysis_report.json`
- KG embedding ML report:
  `Part5_Analysis_zone/reports/kg_embedding_report.json`

## Exploitation Zone assets

The KG is the main P2 exploitation asset. The pipeline also writes:

- `exploitation.risk_model_input`
- `exploitation.dataset_profile`
- `kg.graph_manifest`

The flat table is retained for compatibility with the P1 machine-learning analysis, while `kg.graph_manifest` exposes the semantic artifacts to downstream tools.

## SPARQL analysis examples

Generated queries demonstrate how to:

- retrieve records connected to a population group
- find indicators shared by multiple datasets
- rank population groups by heart disease outcome rate
- combine risk indicators for the same age/gender group
- find graph concepts connected to multiple sources

The P2 graph analysis pipeline runs selected SPARQL queries and writes:

```text
Part5_Analysis_zone/reports/kg_analysis_report.json
```

The second graph-native analysis pipeline generates node embeddings from the RDF
analytics graph, holds out outcome aggregate nodes to reduce label leakage, and
trains a record-level classifier over dataset, population-group, and indicator
embeddings:

```text
Part5_Analysis_zone/reports/kg_embedding_report.json
Part5_Analysis_zone/reports/kg_node_embeddings.csv
Part5_Analysis_zone/models/kg_embedding_model.pkl
```

## Zone documentation

- `Part1_Landing_zone/README.md`
- `Part2_Formatting_zone/README.md`
- `Part3_Trusted_zone/README.md`
- `Part4_Exploitation_zone/README.md`
- `Part4_Exploitation_zone/KG_METAMODEL.md`
- `Part5_Analysis_zone/README.md`
