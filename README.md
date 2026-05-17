# BDA Practice 1

This project is a cardiovascular risk Data Engineering and Data Analysis workflow organized into five zones:

1. `Part1_Landing_zone`
2. `Part2_Formatting_zone`
3. `Part3_Trusted_zone`
4. `Part4_Exploitation_zone`
5. `Part5_Analysis_zone`

The project integrates three Kaggle data sources into a single analytical base and trains two different classification pipelines on top of that unified dataset.

## Data Sources

- `sulianova/cardiovascular-disease-dataset`
- `alexteboul/heart-disease-health-indicators-dataset`
- `cherngs/heart-disease-cleveland-uci`

## Project Architecture

- `Part1_Landing_zone/`
  Downloads data from Kaggle and stores raw Parquet snapshots.
- `Part2_Formatting_zone/`
  Standardizes schema and data types with Spark.
- `Part3_Trusted_zone/`
  Applies data quality rules, cleaning, quarantine, and quality reporting.
- `Part4_Exploitation_zone/`
  Integrates and reconciles the three trusted sources into a canonical analytical table.
- `Part5_Analysis_zone/`
  Runs two predictive pipelines on the same integrated dataset.
- `notebooks/`
  Data validation and model result review.
- `run_all_pipeline.py`
  End-to-end orchestrator.

## Recommended Environment

The most stable setup for this project is WSL Ubuntu.

- WSL2 with Ubuntu
- Miniconda or Anaconda
- Python 3.11
- Java JDK for Spark
- Kaggle credentials for `Part1`

## Environment Setup

The instructions below assume the project lives at:

```bash
/path/to/bda-practica1
```

### 1. Install Java in WSL

```bash
sudo apt update
sudo apt install -y default-jdk
```

### 2. Create the conda environment

```bash
conda create -n bda_practica python=3.11 -y
conda activate bda_practica
python -m pip install --upgrade pip
```

### 3. Install project dependencies

```bash
cd /path/to/bda-practica1
pip install kaggle pyspark==3.5.0 pyarrow pandas duckdb numpy scikit-learn matplotlib seaborn jupyterlab ipykernel
```

### 4. Register the Jupyter kernel

```bash
python -m ipykernel install --user --name bda_practica --display-name "Python (bda_practica)"
```

## Kaggle Configuration

To run the Landing Zone you need `~/.kaggle/kaggle.json`.

```bash
mkdir -p ~/.kaggle
cp /path/to/your/kaggle.json ~/.kaggle/kaggle.json
chmod 600 ~/.kaggle/kaggle.json
```

## Quick Start

### Run the full pipeline without Landing

Useful when snapshots already exist under `Part1_Landing_zone/landing_zone/`.

```bash
conda activate bda_practica
cd /path/to/bda-practica1
python run_all_pipeline.py --skip-landing --strict
```

### Run the full pipeline including Landing

```bash
conda activate bda_practica
cd /path/to/bda-practica1
python run_all_pipeline.py --strict
```

### Run an individual stage

```bash
conda activate bda_practica
cd /path/to/bda-practica1

python Part1_Landing_zone/data_collector.py
python Part2_Formatting_zone/formatting_pipeline.py
python Part3_Trusted_zone/trusted_pipeline.py
python Part4_Exploitation_zone/exploitation_pipeline.py
python Part5_Analysis_zone/analysis_pipeline.py
```

## Airflow

Landing orchestration is implemented with an Airflow DAG:

- DAG id: `landing_zone_data_collector`
- file: `Part1_Landing_zone/airflow_dag.py`
- schedule: `0 2 * * *`

High-level setup steps:

1. Activate the `bda_practica` environment
2. Install Airflow with constraints
3. Define `AIRFLOW_HOME`
4. Symlink `airflow_dag.py` and `data_collector.py` into `dags/`
5. Set the `KAGGLE_USERNAME` and `KAGGLE_KEY` Airflow variables
6. Run `airflow standalone`

The detailed guide is available in [Part1_Landing_zone/README.md](Part1_Landing_zone/README.md).

## Main Outputs

- Landing raw data:
  `Part1_Landing_zone/landing_zone/`
- Formatted DB:
  `Part2_Formatting_zone/formatted_zone/formatted.duckdb`
- Trusted DB:
  `Part3_Trusted_zone/trusted_zone/trusted.duckdb`
- Exploitation DB:
  `Part4_Exploitation_zone/exploitation_zone/exploitation.duckdb`
- Models:
  `Part5_Analysis_zone/models/`
- Reports:
  `Part5_Analysis_zone/reports/`

## What Gets Integrated

The final integration does not keep the sources separated for model training. The three sources are harmonized into a single canonical table:

- `exploitation.risk_model_input`

This unified table contains reconciled shared variables plus lineage fields:

- `source_dataset`
- `source_record_id`
- `age_years_proxy`
- `age_group_code`
- `gender`
- `bmi`
- `systolic_bp`
- `diastolic_bp`
- `high_blood_pressure_flag`
- `high_cholesterol_flag`
- `glucose_risk_flag`
- `smoking_flag`
- `physical_activity_flag`
- `heavy_alcohol_flag`
- `general_health_score`
- `mental_unhealthy_days`
- `physical_unhealthy_days`
- `difficulty_walking_flag`
- `max_heart_rate`
- `exercise_induced_angina`
- `target`

## Analytical Layer

Two analytical pipelines are implemented on top of the same integrated table:

- `integrated_core`
  Uses a compact and robust subset of shared variables.
- `integrated_enriched`
  Uses a broader set of reconciled variables.

In both cases:

- the model is not trained separately per original dataset
- `source_dataset` is not used as a training feature
- two candidate models are evaluated and the best one is selected through cross-validation

## Included Notebooks

- `notebooks/01_data_pipeline_validation.ipynb`
  Reviews data quality, zone outputs, and generated artifacts.
- `notebooks/02_model_results_and_comparison.ipynb`
  Compares the two integrated analytical pipelines.

## Zone Documentation

- [Part1_Landing_zone/README.md](Part1_Landing_zone/README.md)
- [Part2_Formatting_zone/README.md](Part2_Formatting_zone/README.md)
- [Part3_Trusted_zone/README.md](Part3_Trusted_zone/README.md)
- [Part4_Exploitation_zone/README.md](Part4_Exploitation_zone/README.md)
- [Part5_Analysis_zone/README.md](Part5_Analysis_zone/README.md)
- [RUN_PIPELINE.md](RUN_PIPELINE.md)
