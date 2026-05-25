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
cd /path/to/bda-practica2
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

Each quality rule is implemented as a **denial constraint** of the general form

```text
¬∃ t ∈ Dataset .  φ(t)
```

(no tuple should satisfy the forbidden predicate `φ`). The full list per
dataset, in the standard `¬∃ t : φ(t)` syntax requested by the P1 review:

### `cardiovascular_disease`
| Constraint name | Logical form |
|-----------------|--------------|
| `patient_id_present` | `¬∃ t : t.patient_id IS NULL` |
| `age_years_between_18_100` | `¬∃ t : t.age_years < 18 ∨ t.age_years > 100` |
| `age_group_code_between_1_13` | `¬∃ t : t.age_group_code < 1 ∨ t.age_group_code > 13` |
| `gender_domain_male_female` | `¬∃ t : t.gender ∉ {female, male}` |
| `height_cm_between_120_250` | `¬∃ t : t.height_cm < 120 ∨ t.height_cm > 250` |
| `weight_kg_between_30_300` | `¬∃ t : t.weight_kg < 30 ∨ t.weight_kg > 300` |
| `bmi_between_10_80` | `¬∃ t : t.bmi < 10 ∨ t.bmi > 80` |
| `systolic_bp_between_70_250` | `¬∃ t : t.systolic_bp < 70 ∨ t.systolic_bp > 250` |
| `diastolic_bp_between_40_150` | `¬∃ t : t.diastolic_bp < 40 ∨ t.diastolic_bp > 150` |
| `systolic_bp_gte_diastolic_bp` | `¬∃ t : t.systolic_bp < t.diastolic_bp` |
| `cholesterol_level_domain_1_2_3` | `¬∃ t : t.cholesterol_level ∉ {1,2,3}` |
| `glucose_level_domain_1_2_3` | `¬∃ t : t.glucose_level ∉ {1,2,3}` |
| `is_smoker_present` | `¬∃ t : t.is_smoker IS NULL` |
| `drinks_alcohol_present` | `¬∃ t : t.drinks_alcohol IS NULL` |
| `is_active_present` | `¬∃ t : t.is_active IS NULL` |
| `target_present` | `¬∃ t : t.has_cardiovascular_disease IS NULL` |
| `patient_id_unique` | `¬∃ t₁, t₂ : t₁.patient_id = t₂.patient_id ∧ t₁ ≠ t₂` |

### `heart_disease_health_indicators`
| Constraint name | Logical form |
|-----------------|--------------|
| `respondent_id_present` | `¬∃ t : t.respondent_id IS NULL` |
| `age_group_code_between_1_13` | `¬∃ t : t.age_group_code < 1 ∨ t.age_group_code > 13` |
| `age_years_proxy_between_18_90` | `¬∃ t : t.age_years_proxy < 18 ∨ t.age_years_proxy > 90` |
| `gender_domain_male_female` | `¬∃ t : t.gender ∉ {female, male}` |
| `bmi_between_10_100` | `¬∃ t : t.bmi < 10 ∨ t.bmi > 100` |
| `high_blood_pressure_present` | `¬∃ t : t.high_blood_pressure_flag IS NULL` |
| `high_cholesterol_present` | `¬∃ t : t.high_cholesterol_flag IS NULL` |
| `smoking_flag_present` | `¬∃ t : t.smoking_flag IS NULL` |
| `physical_activity_present` | `¬∃ t : t.physical_activity_flag IS NULL` |
| `heavy_alcohol_present` | `¬∃ t : t.heavy_alcohol_flag IS NULL` |
| `general_health_between_1_5_or_null` | `¬∃ t : t.general_health_score IS NOT NULL ∧ (t.general_health_score < 1 ∨ t.general_health_score > 5)` |
| `mental_unhealthy_days_between_0_30_or_null` | `¬∃ t : t.mental_unhealthy_days IS NOT NULL ∧ (t.mental_unhealthy_days < 0 ∨ t.mental_unhealthy_days > 30)` |
| `physical_unhealthy_days_between_0_30_or_null` | analogous for `physical_unhealthy_days` |
| `difficulty_walking_present` | `¬∃ t : t.difficulty_walking_flag IS NULL` |
| `education_between_1_6_or_null` | `¬∃ t : t.education_level IS NOT NULL ∧ t.education_level ∉ [1,6]` |
| `income_between_1_8_or_null` | `¬∃ t : t.income_level IS NOT NULL ∧ t.income_level ∉ [1,8]` |
| `target_present` | `¬∃ t : t.has_heart_disease IS NULL` |

### `heart_disease_cleveland`
| Constraint name | Logical form |
|-----------------|--------------|
| `record_key_present` | `¬∃ t : t.record_key IS NULL` |
| `age_years_between_18_100` | `¬∃ t : t.age_years < 18 ∨ t.age_years > 100` |
| `age_group_code_between_1_13` | `¬∃ t : t.age_group_code < 1 ∨ t.age_group_code > 13` |
| `gender_domain_male_female` | `¬∃ t : t.gender ∉ {female, male}` |
| `chest_pain_type_domain_0_3` | `¬∃ t : t.chest_pain_type ∉ {0,1,2,3}` |
| `resting_bp_between_70_250` | `¬∃ t : t.resting_bp < 70 ∨ t.resting_bp > 250` |
| `serum_cholesterol_between_50_700` | `¬∃ t : t.serum_cholesterol < 50 ∨ t.serum_cholesterol > 700` |
| `fasting_blood_sugar_present` | `¬∃ t : t.fasting_blood_sugar_high IS NULL` |
| `resting_ecg_domain_0_2` | `¬∃ t : t.resting_ecg ∉ {0,1,2}` |
| `max_heart_rate_between_40_250` | `¬∃ t : t.max_heart_rate < 40 ∨ t.max_heart_rate > 250` |
| `exercise_induced_angina_present` | `¬∃ t : t.exercise_induced_angina IS NULL` |
| `st_depression_between_0_10` | `¬∃ t : t.st_depression < 0 ∨ t.st_depression > 10` |
| `st_slope_domain_0_2` | `¬∃ t : t.st_slope ∉ {0,1,2}` |
| `num_major_vessels_between_0_3_or_null` | `¬∃ t : t.num_major_vessels IS NOT NULL ∧ t.num_major_vessels ∉ [0,3]` |
| `thalassemia_type_domain_0_2` | `¬∃ t : t.thalassemia_type ∉ {0,1,2}` |
| `target_present` | `¬∃ t : t.has_heart_disease IS NULL` |
| `record_key_unique` | `¬∃ t₁, t₂ : t₁.record_key = t₂.record_key ∧ t₁ ≠ t₂` |

Rows violating hard constraints are **not** silently repaired; they are
written to `metadata.quarantine` with the violated rule names, while
aggregate rule metrics are stored in `metadata.quality_report`. The exact rule
implementations live in `trusted_rules.py`.

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
