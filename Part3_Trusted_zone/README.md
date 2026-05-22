# Part3 Trusted Zone

The Trusted Zone applies semantic standardization, data quality rules, denial constraints, quarantine, and audit reporting to the structurally formatted tables. It produces clean, traceable datasets ready for Knowledge Graph generation.

## Goal

- receive structurally formatted tables from `Part2`
- normalize source values into trusted canonical fields
- validate ranges, domains, and logical consistency
- send problematic records to quarantine
- generate an auditable quality report
- materialize clean trusted tables in DuckDB as the main input for the KG pipeline

## Main Scripts

- `trusted_pipeline.py`
  Trusted Zone orchestrator.
- `trusted_rules.py`
  Dataset-specific cleaning and validation rules.
- `trusted_storage.py`
  Parquet export and DuckDB materialization.
- `trusted_config.py`
  Shared paths and utilities.

## Execution

```bash
conda activate bda_practica
cd /path/to/bda-practica1
python Part3_Trusted_zone/trusted_pipeline.py
```

## Input

- `Part2_Formatting_zone/formatted_zone/formatted.duckdb`

## Outputs

Database:

- `Part3_Trusted_zone/trusted_zone/trusted.duckdb`

Main tables:

- `trusted.cardiovascular_disease`
- `trusted.heart_disease_health_indicators`
- `trusted.heart_disease_cleveland`
- `metadata.quality_report`
- `metadata.quarantine`

Versioned snapshots:

- `trusted_zone/parquet/cardiovascular_disease/<run_id>/`
- `trusted_zone/parquet/heart_disease_health_indicators/<run_id>/`
- `trusted_zone/parquet/heart_disease_cleveland/<run_id>/`
- `trusted_zone/parquet/quality_report/<run_id>/`
- `trusted_zone/parquet/quarantine/<run_id>/`

As in Part2, obsolete DuckDB tables are removed when the current trusted state is materialized.

## P2 changes

The cleaning that previously lived too early in the Formatting Zone is now performed here. Trusted now owns:

- age conversion and age-group derivation
- gender and boolean normalization
- BMI and health-risk derivations
- controlled categorical mappings
- range, domain, uniqueness, and consistency checks
- quarantine and quality metrics

The output tables are reliable enough to be transformed into RDF resources in `Part4_Exploitation_zone`.

## Denial constraint formalization

The implemented quality rules can be read as denial constraints of the form
`not exists r in Dataset: condition(r)`. The main constraints are:

- Cardiovascular age: `not exists r: r.age_years < 18 or r.age_years > 100`
- Cardiovascular blood pressure order: `not exists r: r.systolic_bp < r.diastolic_bp`
- Cardiovascular patient key: `not exists r1, r2: r1.patient_id = r2.patient_id and r1 != r2`
- CDC age group: `not exists r: r.age_group_code < 1 or r.age_group_code > 13`
- CDC binary indicators: `not exists r: high_blood_pressure_flag, high_cholesterol_flag, smoking_flag, physical_activity_flag, heavy_alcohol_flag, difficulty_walking_flag not in {true,false}`
- Cleveland clinical ranges: `not exists r: resting_bp not in [70,250] or serum_cholesterol not in [50,700] or max_heart_rate not in [40,250]`
- Cleveland categorical domains: `not exists r: chest_pain_type not in {0,1,2,3} or resting_ecg not in {0,1,2} or st_slope not in {0,1,2}`
- Cleveland record key: `not exists r1, r2: r1.record_key = r2.record_key and r1 != r2`

Rows violating hard constraints are not silently repaired; they are written to
`metadata.quarantine` with the violated rule names, while aggregate rule metrics
are stored in `metadata.quality_report`.

## Dataset-specific logic

### `cardiovascular_disease`

The pipeline validates:

- plausible ranges for age, height, weight, and blood pressure
- consistency between systolic and diastolic pressure
- valid cholesterol and glucose domains
- uniqueness of `patient_id`

Rows that fail hard rules are sent to `metadata.quarantine`.

### `heart_disease_health_indicators`

The pipeline validates:

- valid binary domains
- basic consistency of ranges and derived categories
- format of general health and unhealthy-day variables

Important: `respondent_id` is not treated as a unique person-level entity key for deduplication. In this dataset it behaves as a technical record fingerprint rather than a reconciled person identifier.

### `heart_disease_cleveland`

The pipeline validates:

- ranges for age, blood pressure, cholesterol, max heart rate, and ST depression
- controlled categorical domains
- uniqueness of `record_key`

If unknown categorical codes appear, they are handled as controlled missing values with traceability instead of being silently merged into valid categories.

## Generated Metadata

### `metadata.quality_report`

Per execution it includes:

- dataset
- applied rule
- number of violations
- violation percentage
- action taken

### `metadata.quarantine`

It includes:

- `run_id`
- dataset
- record key
- quarantine reason
- quarantine timestamp

## Result of This Stage

The Trusted Zone leaves three clean and comparable tables, but they are still not integrated. Cross-source integration and reconciliation happen in `Part4_Exploitation_zone`.
