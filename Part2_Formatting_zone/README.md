# Part2 Formatting Zone

The Formatting Zone converts raw Landing snapshots into one standardized table per dataset, with normalized column names and consistent data types.

## Goal

- read the latest snapshot of each source from `Part1_Landing_zone/landing_zone/`
- standardize schema and types with Spark
- persist one formatted table per dataset in DuckDB

## Main Script

- `formatting_pipeline.py`

## Execution

```bash
conda activate bda_practica
cd /path/to/bda-practica1
python Part2_Formatting_zone/formatting_pipeline.py
```

## Inputs

- `landing_zone/cardiovascular_disease/...`
- `landing_zone/heart_disease_health_indicators/...`
- `landing_zone/heart_disease_cleveland/...`

## Outputs

DuckDB database:

- `Part2_Formatting_zone/formatted_zone/formatted.duckdb`

Created tables:

- `formatted.cardiovascular_disease`
- `formatted.heart_disease_health_indicators`
- `formatted.heart_disease_cleveland`

The pipeline also removes obsolete tables from the `formatted` schema if they no longer belong to the current project configuration.

## What Is Standardized in This Stage

### `cardiovascular_disease`

- converts age from days to years
- derives `age_group_code`
- normalizes gender values
- computes `bmi`
- casts blood pressure, cholesterol, glucose, and target fields

### `heart_disease_health_indicators`

- normalizes binary and categorical CDC survey variables
- maps age ranges to `age_group_code`
- derives a numeric proxy age from the age band
- standardizes health, activity, alcohol, and target fields

### `heart_disease_cleveland`

- casts clinical variables
- normalizes gender
- derives `age_group_code`
- standardizes the heart disease target

## What This Stage Does Not Do

- it does not apply hard quality rules
- it does not quarantine rows
- it does not integrate the three sources yet

Those tasks are handled in `Part3_Trusted_zone` and `Part4_Exploitation_zone`.
