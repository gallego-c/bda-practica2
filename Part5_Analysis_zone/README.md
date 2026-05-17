# Part 5: Analysis Zone

The Analysis Zone implements two complementary predictive pipelines on integrated datasets, plus a graph-based SPARQL analysis pipeline. All pipelines consume data from the Exploitation Zone.

**Purpose:** Demonstrate actionable ML models and semantic graph analysis on integrated cross-dataset information.

## 🎯 Analysis Approach

All pipelines use a single integrated data source rather than training separate models per dataset:
- Data is loaded from the compatibility table in Exploitation Zone
- Three datasets are combined without false person-level merging
- Two model variants are trained and compared

## 📊 Model Pipelines

### 1. Integrated Core Model
**Feature Set:** Conservative, widely-available indicators

**Numeric Features:**
- `age_years_proxy` - Derived age
- `bmi` - Body Mass Index

**Categorical Features:**
- `gender`
- `high_blood_pressure_flag`
- `high_cholesterol_flag`
- `glucose_risk_flag`
- `smoking_flag`
- `physical_activity_flag`
- `heavy_alcohol_flag`

**Model Selection:** ROC-AUC cross-validation metric

### 2. Integrated Enriched Model
**Feature Set:** Extended with additional clinical variables

**Additional Numeric Features:**
- `systolic_bp` - Systolic blood pressure
- `diastolic_bp` - Diastolic blood pressure
- `general_health_score` - Self-reported health
- `mental_unhealthy_days` - Mental health indicator
- `physical_unhealthy_days` - Physical health indicator
- `max_heart_rate` - Cardiovascular capacity

**Additional Categorical Features:**
- `difficulty_walking_flag`
- `exercise_induced_angina`

**Model Selection:** PR-AUC cross-validation metric

### Candidate Models (Both Pipelines)
- Logistic Regression (balanced)
- Gradient Boosting (tuned)

## 🧠 Graph Analysis Pipeline

`kg_analysis_pipeline.py` operates directly on the RDF Knowledge Graph:
- Loads the compact analytics graph (Turtle format)
- Executes SPARQL queries without flattening
- Provides graph-native analysis and insights
- Demonstrates semantic integration benefits

## 🚀 Usage

### Run ML Pipelines

```bash
python Part5_Analysis_zone/analysis_pipeline.py
```

This trains both integrated models and produces comparison reports.

### Run Graph Analysis

```bash
python Part5_Analysis_zone/kg_analysis_pipeline.py
```

Executes SPARQL queries on the Knowledge Graph and generates analytical insights.

### Processing Time
- ML pipelines: 2-5 minutes (depending on cross-validation folds)
- Graph analysis: 1-2 minutes

## 📥 Inputs

From Exploitation Zone:
- `Part4_Exploitation_zone/exploitation_zone/exploitation.duckdb`
  - `exploitation.risk_model_input` - ML training data table
- `Part4_Exploitation_zone/exploitation_zone/kg/health_risk_analytics_kg.ttl` - Knowledge Graph

## 📤 Outputs

### Trained Models
```
Part5_Analysis_zone/models/
├── integrated_core_model.pkl         # Pickled model object
└── integrated_enriched_model.pkl     # Pickled model object
```

### Reports
```
Part5_Analysis_zone/reports/
├── integrated_core_report.json       # Core model performance metrics
├── integrated_enriched_report.json   # Enriched model performance metrics
├── summary_report.json               # Side-by-side comparison
└── kg_analysis_report.json           # Graph analysis findings
```

### Report Contents
Each model report includes:
- Accuracy, Precision, Recall, F1-Score
- ROC-AUC or PR-AUC (depending on model)
- Confusion matrix
- Feature importance rankings
- Cross-validation fold details
- Training duration

## 🛠️ Configuration

Key scripts:
- `analysis_pipeline.py` - ML orchestrator
- `kg_analysis_pipeline.py` - SPARQL-based graph analysis
- `analysis_utils.py` - Data loading, preprocessing, validation
- `model_pipeline_integrated_core.py` - Core model training
- `model_pipeline_integrated_enriched.py` - Enriched model training
- `analysis_config.py` - Shared configuration

## 📈 Feature Engineering

Both pipelines apply:
- **Scaling:** StandardScaler for numeric features
- **Encoding:** LabelEncoder for categorical features
- **Validation:** Train/test split stratified by outcome
- **Cross-validation:** 5-fold stratified CV

## 🔍 Model Comparison

The `summary_report.json` compares:
- Model accuracy on held-out test sets
- Cross-validation score distributions
- Feature set complexity (core vs. enriched)
- Computational cost
- Interpretability vs. performance tradeoff

This enables data-driven decisions about which model variant is appropriate for your use case.

## 📚 Further Reading

For implementation details, hyperparameter tuning, and validation methodology, see the script docstrings in each `model_pipeline_*.py` file.
  - `LogisticRegression`
  - `max_iter=2000`
  - `class_weight="balanced"`
  - `random_state=42`
- `rf_balanced`
  - `RandomForestClassifier`
  - `n_estimators=120`
  - `max_depth=12`
  - `min_samples_leaf=12`
  - `class_weight="balanced_subsample"`
  - `random_state=42`
  - `n_jobs=-1`

## Preprocessing

Preprocessing is performed inside a scikit-learn `Pipeline`:

- numeric variables:
  median imputation + `StandardScaler`
- categorical variables:
  most-frequent imputation + `OneHotEncoder(handle_unknown="ignore")`

In addition:

- stratified train/test split by target
- `test_size=0.20`
- `random_state=42`
- stratified cross-validation with `3` folds
- decision threshold selected by best `F1` on the training set

## Important Interpretation Notes

- `source_dataset` is kept for traceability and later analysis
- `source_dataset` is not used as a model feature
- training uses all three sources together in the same training matrix

## Reported Metrics

- accuracy
- precision
- recall
- F1
- ROC-AUC
- PR-AUC
- confusion matrix

## Related Notebooks

- `notebooks/01_data_pipeline_validation.ipynb`
- `notebooks/02_model_results_and_comparison.ipynb`
