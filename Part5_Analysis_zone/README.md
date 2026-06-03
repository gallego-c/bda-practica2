# Part 5: Analysis Zone

Two analytical pipelines are required by the P2 spec: one based on
**pattern-matching SPARQL queries** and one based on **ML over KG embeddings**.
Two additional ML baselines on the integrated exploitation table act as
references, and a hybrid model plus a comparison stage quantify the added value
of the graph-derived features.

## Pipelines

### 1. SPARQL analysis — `kg_analysis_pipeline.py`

Reads the compact analytics graph
(`Part4_Exploitation_zone/exploitation_zone/kg/health_risk_analytics_kg.ttl`)
and executes the curated SPARQL queries written by Part4. Produces
`reports/kg_analysis_report.json` with the query results.

### 2. KG embedding ML — `kg_embedding_pipeline.py`

Builds a typed RDF triple set from the analytics graph, holds out the outcome
aggregate measurement nodes to avoid label leakage, learns canonical Knowledge
Graph Embeddings with **PyKEEN** (TransE, DistMult, ComplEx — best model
selected by MRR, with a `TruncatedSVD` fallback), composes record-level feature
vectors from dataset, population-group and indicator embeddings, and trains
`LogisticRegression` / `RandomForestClassifier`.

Output:
- `models/kg_embedding_model.pkl`
- `reports/kg_embedding_report.json`
- `reports/kg_node_embeddings.csv`
- `reports/kg_embedding_training_data.csv`

### 3. Hybrid ML — `model_pipeline_hybrid.py`

Concatenates the best tabular features with the KG node embeddings in a single
feature space to test whether graph-derived semantic context complements raw
clinical indicators.

Output:
- `models/hybrid_tabular_kg_model.pkl`
- `reports/hybrid_tabular_kg_report.json`

### 4. Integrated baselines — `analysis_pipeline.py`

Two reference models trained on the same integrated `exploitation.risk_model_input`:

- **Integrated Core**: age, BMI, gender, risk-factor flags (model selected by CV ROC-AUC).
- **Integrated Enriched**: adds clinical variables — BP, max heart rate,
  general health score, mental/physical unhealthy days, difficulty walking,
  exercise-induced angina (model selected by CV PR-AUC).

Each report includes accuracy, precision, recall, F1, ROC-AUC, PR-AUC,
confusion matrix, decision threshold and top feature importance.

### 5. Model comparison — `model_comparison_pipeline.py`

Reads the individual pipeline reports and produces
`reports/model_comparison_report.json`, a unified comparison of the tabular,
KG-only and hybrid models that quantifies the added value of the graph-derived
features.

## Usage

```bash
python Part5_Analysis_zone/kg_analysis_pipeline.py
python Part5_Analysis_zone/kg_embedding_pipeline.py
python Part5_Analysis_zone/model_pipeline_hybrid.py
python Part5_Analysis_zone/analysis_pipeline.py
python Part5_Analysis_zone/model_comparison_pipeline.py
```

The combined orchestrator `run_all_pipeline.py` calls them in this exact
order so the `kg_embedding_report.json` is available when the baseline
summary is written.

## Preprocessing

Tabular pipelines use a scikit-learn `Pipeline`:

- numeric: median imputation + `StandardScaler`
- categorical: most-frequent imputation + `OneHotEncoder(handle_unknown="ignore")`

Training uses stratified train/test split (`test_size=0.20`,
`random_state=42`) and stratified `StratifiedKFold` cross-validation
(`CV_FOLDS=3`). Decision threshold is the F1-optimal threshold on the
training set.

`source_dataset` is kept in the table for traceability but is **not** a model
feature; the three sources train jointly without that leak.

## KG Embedding Details

- Embeddings are learned with **PyKEEN** by training standard KGE models
  (TransE, DistMult, ComplEx) on the RDF triples and selecting the best by MRR;
  a `TruncatedSVD` factorisation of the typed adjacency is used as a fallback
  when PyKEEN is unavailable.
- The held-out outcome aggregate nodes do not contribute to the embeddings
  used for training, reducing label leakage.
- Embedding dimension: 32 (`EMBEDDING_DIMENSIONS`).
- Maximum record samples: 30 000 (`MAX_RECORD_SAMPLES`).
- The final per-record feature vector concatenates dataset, population-group
  and indicator-group mean embeddings, element-wise products and absolute
  differences, plus three indicator counts.

## Inputs

- `Part4_Exploitation_zone/exploitation_zone/exploitation.duckdb`
  - `exploitation.risk_model_input` — integrated training table.
- `Part4_Exploitation_zone/exploitation_zone/kg/health_risk_analytics_kg.ttl`
  — compact RDF analytics graph.
- `Part4_Exploitation_zone/exploitation_zone/kg/kg_manifest.json`
  — manifest used to resolve paths reliably.

## Outputs

```
Part5_Analysis_zone/
├── models/
│   ├── integrated_core_model.pkl
│   ├── integrated_enriched_model.pkl
│   ├── kg_embedding_model.pkl
│   └── hybrid_tabular_kg_model.pkl
└── reports/
    ├── integrated_core_report.json
    ├── integrated_enriched_report.json
    ├── kg_embedding_report.json
    ├── hybrid_tabular_kg_report.json
    ├── model_comparison_report.json
    ├── kg_analysis_report.json
    ├── kg_node_embeddings.csv
    ├── kg_embedding_training_data.csv
    └── summary_report.json
```

## Notebooks

The Jupyter notebooks under `notebooks/` document and visualise the results:

- `02_knowledge_graph_exploration.ipynb` — runs the SPARQL queries and plots
  the analytical outputs.
- `03_model_comparison_and_explainability.ipynb` — compares the core, enriched
  and KG-embedding models, shows feature importance and projects the KG node
  embeddings into 2D for inspection.
- `04_hybrid_model_and_full_comparison.ipynb` — full comparison of the tabular,
  KG-only and hybrid pipelines, with the improvement analysis of adding the KG
  on top of the tabular baseline.
