# Part1 Landing Zone

The Landing Zone downloads the agreed Kaggle datasets and stores them as raw Parquet snapshots without business transformations.

## Downloaded Datasets

- `sulianova/cardiovascular-disease-dataset`
- `alexteboul/heart-disease-health-indicators-dataset`
- `cherngs/heart-disease-cleveland-uci`

## Main Scripts

- `data_collector.py`
  Downloads the datasets and writes raw snapshots.
- `airflow_dag.py`
  Orchestrates periodic ingestion with Airflow.

## Generated Structure

```text
Part1_Landing_zone/
|- airflow_dag.py
|- data_collector.py
|- requirements.txt
\- landing_zone/
   |- cardiovascular_disease/
   |- heart_disease_health_indicators/
   \- heart_disease_cleveland/
```

Each dataset directory contains `ingested_<timestamp>/` folders with the corresponding raw Parquet snapshot.

## Requirements

- WSL Ubuntu
- `conda` environment with Python 3.11
- Java for Spark
- Kaggle credentials

## Manual Execution

```bash
conda activate bda_practica
cd /path/to/bda-practica2
python Part1_Landing_zone/data_collector.py
```

## What `data_collector.py` Does

For each dataset it:

1. downloads the zip file from Kaggle
2. locates the CSV files
3. reads them with Spark
4. writes a raw Parquet snapshot
5. removes temporary download files

## Kaggle Configuration

Store your credentials at:

```bash
~/.kaggle/kaggle.json
```

Example:

```bash
mkdir -p ~/.kaggle
cp /path/to/your/kaggle.json ~/.kaggle/kaggle.json
chmod 600 ~/.kaggle/kaggle.json
```

## Airflow Usage

The defined DAG is:

- `landing_zone_data_collector`

Schedule:

- `0 2 * * *`

## Recommended Airflow Installation

Activate the environment and use official constraints:

```bash
conda activate bda_practica
export AIRFLOW_HOME=~/airflow
export AIRFLOW_VERSION=2.10.5
export PYTHON_VERSION=3.11
export CONSTRAINT_URL="https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"

pip install "apache-airflow==${AIRFLOW_VERSION}" --constraint "${CONSTRAINT_URL}"
```

## Prepare the DAG

```bash
mkdir -p "$AIRFLOW_HOME/dags"

ln -sf /path/to/bda-practica2/Part1_Landing_zone/airflow_dag.py "$AIRFLOW_HOME/dags/airflow_dag.py"
ln -sf /path/to/bda-practica2/Part1_Landing_zone/data_collector.py "$AIRFLOW_HOME/dags/data_collector.py"
```

## Start Airflow

```bash
conda activate bda_practica
export AIRFLOW_HOME=~/airflow
airflow standalone
```

The UI is typically available at:

```text
http://localhost:8080
```

## Configure Airflow Variables

In a second WSL terminal:

```bash
conda activate bda_practica
export AIRFLOW_HOME=~/airflow

airflow variables set KAGGLE_USERNAME "your_username"
airflow variables set KAGGLE_KEY "your_api_key"
```

## Trigger the DAG Manually

```bash
airflow dags unpause landing_zone_data_collector
airflow dags trigger landing_zone_data_collector
```

## Expected Output

The `landing_zone/` folder will contain raw Parquet snapshots for:

- `cardiovascular_disease`
- `heart_disease_health_indicators`
- `heart_disease_cleveland`

These outputs are the input for `Part2_Formatting_zone`.
