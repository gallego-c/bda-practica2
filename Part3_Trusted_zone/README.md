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
