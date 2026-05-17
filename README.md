# BDA Practice 2

This project extends the P1 cardiovascular-risk data pipeline with a Knowledge Graph-based Exploitation Zone.

The five-zone architecture is preserved:

1. `Part1_Landing_zone`
2. `Part2_Formatting_zone`
3. `Part3_Trusted_zone`
4. `Part4_Exploitation_zone`
5. `Part5_Analysis_zone`

P2 focuses on the flow from Formatted to Exploitation. The main change is that the Exploitation Zone is no longer only a flat consolidated table: it now generates an RDF/RDFS Knowledge Graph from cleaned Trusted Zone datasets.

## Data sources

- `sulianova/cardiovascular-disease-dataset`
- `alexteboul/heart-disease-health-indicators-dataset`
- `cherngs/heart-disease-cleveland-uci`

## P2 architecture

- `Part1_Landing_zone/`
  Downloads raw source snapshots and stores raw Parquet data.
- `Part2_Formatting_zone/`
  Performs only structural formatting: column-name normalization, table creation, technical fingerprints, and DuckDB persistence.
- `Part3_Trusted_zone/`
  Performs semantic standardization, quality rules, denial constraints, cleaning, quarantine, and quality reports.
- `Part4_Exploitation_zone/`
  Builds the Knowledge Graph semantic layer from Trusted data using RDF/RDFS and RDFLib. It also keeps a compatibility table for the existing ML pipelines.
- `Part5_Analysis_zone/`
  Runs the legacy predictive pipelines and a SPARQL-based KG analysis pipeline.

## Main P2 fixes

Formatting Zone was corrected so it no longer performs:

- age conversion
- BMI calculation
- gender normalization
- boolean normalization
- risk-factor derivation
- cleaning, validation, filtering, deduplication, or quarantine

Those tasks now belong to the Trusted Zone, where the cleaned datasets become the input for the KG generation pipeline.

## Knowledge Graph design

The KG uses RDF/RDFS with the namespace:

```text
https://example.org/bda/health-risk/
```

Main classes:

- `hr:HealthRecord`
- `hr:Dataset`
- `hr:PopulationGroup`
- `hr:AgeGroup`
- `hr:Gender`
- `hr:Indicator`
- `hr:AggregateMeasurement`
- `hr:Outcome`
- `hr:KnowledgeGraphRun`

The datasets do not share a true person identifier, location, or timestamp. The KG therefore integrates them through shared semantic concepts that are present in all or several sources:

- age group
- gender
- population group
- health indicator
- risk factor
- outcome
- dataset provenance

This gives semantic homogenization without falsely merging unrelated people across datasets.

Detailed schema and mapping documentation is in:

- `Part4_Exploitation_zone/KG_METAMODEL.md`

## KG technologies

- RDF/RDFS for schema and graph modelling
- RDFLib for triple generation and SPARQL validation
- SPARQL for analytical graph queries
- GraphDB as an optional external store for visualization and querying
- DuckDB for formatted/trusted tables, compatibility views, and KG manifest discovery

## Supported environments

The pipeline is verified in both WSL/Linux and native Windows. Spark 3.5 requires Java 17 for this project; Java 25 is not supported.

- Python 3.11 or 3.12
- PySpark 3.5.0
- Java 17 for Spark
- Kaggle credentials for Landing Zone if new downloads are required

### Linux/WSL setup

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

## Zone documentation

- `Part1_Landing_zone/README.md`
- `Part2_Formatting_zone/README.md`
- `Part3_Trusted_zone/README.md`
- `Part4_Exploitation_zone/README.md`
- `Part4_Exploitation_zone/KG_METAMODEL.md`
- `Part5_Analysis_zone/README.md`
