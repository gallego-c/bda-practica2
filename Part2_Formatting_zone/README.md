# Part2 Formatting Zone

The Formatting Zone is now strictly structural. It reads the latest raw snapshots from the Landing Zone, normalizes the physical table structure, and stores one structured table per dataset in DuckDB.

## Goal

- read raw Parquet snapshots from `Part1_Landing_zone/landing_zone/`
- normalize column names into stable machine-friendly names
- expand malformed single-column ingestions when a delimiter was preserved in the header
- add technical lineage fields
- persist one formatted table per dataset in DuckDB

## What belongs here

- schema inference or schema definition
- column-name normalization
- table creation
- storage format conversion
- technical record fingerprints

## What does not belong here

The Formatting Zone must not apply cleaning or semantic transformations. In P2 the following tasks were moved to the Trusted Zone:

- age conversion from days to years
- age-band derivation
- BMI calculation
- gender normalization
- boolean normalization
- risk-factor derivation
- hard range/domain validation
- filtering, deduplication, and quarantine

## Main script

- `formatting_pipeline.py`

## Execution

```bash
python Part2_Formatting_zone/formatting_pipeline.py
```

## Inputs

- `Part1_Landing_zone/landing_zone/cardiovascular_disease/...`
- `Part1_Landing_zone/landing_zone/heart_disease_health_indicators/...`
- `Part1_Landing_zone/landing_zone/heart_disease_cleveland/...`

## Outputs

DuckDB database:

- `Part2_Formatting_zone/formatted_zone/formatted.duckdb`

Created tables:

- `formatted.cardiovascular_disease`
- `formatted.heart_disease_health_indicators`
- `formatted.heart_disease_cleveland`

Each table preserves the source meaning and adds:

- `_source_dataset`
- `_formatted_record_id`
- `_formatted_at_utc`
