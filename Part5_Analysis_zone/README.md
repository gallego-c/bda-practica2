# Part5 Analysis Zone

This zone implements two predictive analysis pipelines on top of the same integrated table generated in `Part4_Exploitation_zone`.

## Goal

To satisfy the analytical requirement of the project with at least two reproducible classification pipelines using a single integrated base:

- the model is not trained separately per original dataset
- the three sources are loaded together from `exploitation.risk_model_input`
- two pipelines with different feature sets are compared

## Main Scripts

- `analysis_pipeline.py`
  Part5 orchestrator.
- `analysis_utils.py`
  Data loading, shared preprocessing, validation, and persistence.
- `model_pipeline_integrated_core.py`
  Baseline pipeline with compact and robust shared variables.
- `model_pipeline_integrated_enriched.py`
  Alternative pipeline with additional reconciled signal.

## Execution

```bash
conda activate bda_practica
cd /path/to/bda-practica1
python Part5_Analysis_zone/analysis_pipeline.py
```

## Input

- `Part4_Exploitation_zone/exploitation_zone/exploitation.duckdb`
- table: `exploitation.risk_model_input`

## Outputs

- `Part5_Analysis_zone/models/integrated_core_model.pkl`
- `Part5_Analysis_zone/models/integrated_enriched_model.pkl`
- `Part5_Analysis_zone/reports/integrated_core_report.json`
- `Part5_Analysis_zone/reports/integrated_enriched_report.json`
- `Part5_Analysis_zone/reports/summary_report.json`

## How the Two Pipelines Work

### 1. `integrated_core`

Uses variables that are widely available and relatively robust across sources:

- numeric:
  `age_years_proxy`, `bmi`
- categorical:
  `gender`, `high_blood_pressure_flag`, `high_cholesterol_flag`,
  `glucose_risk_flag`, `smoking_flag`, `physical_activity_flag`,
  `heavy_alcohol_flag`

Model selection:

- cross-validation selection metric: `ROC-AUC`

### 2. `integrated_enriched`

Extends the previous feature set with additional reconciled variables:

- numeric:
  `age_years_proxy`, `bmi`, `systolic_bp`, `diastolic_bp`,
  `general_health_score`, `mental_unhealthy_days`,
  `physical_unhealthy_days`, `max_heart_rate`
- categorical:
  `gender`, `high_blood_pressure_flag`, `high_cholesterol_flag`,
  `glucose_risk_flag`, `smoking_flag`, `physical_activity_flag`,
  `heavy_alcohol_flag`, `difficulty_walking_flag`,
  `exercise_induced_angina`

Model selection:

- cross-validation selection metric: `PR-AUC`

## Candidate Models in Both Pipelines

Both pipelines evaluate the same two candidate models:

- `logreg_balanced`
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
