# Quick Start Guide

Get the complete pipeline running end-to-end in minutes.

## 📋 Prerequisites

- Environment set up ([see ENVIRONMENT.md](ENVIRONMENT.md))
- Python 3.11 or 3.12 activated
- Java 17 available
- ✅ Recommended: Use existing Landing Zone snapshots (skip downloads)

## ⚡ Run Everything

From the project root:

```bash
python run_all_pipeline.py --skip-landing --strict
```

**Flags:**
- `--skip-landing` - Don't re-download datasets, use existing snapshots
- `--strict` - Fail on first error (recommended for development)

**Expected runtime:** 8-15 minutes depending on hardware

## 🔍 Run Individual Zones

Process each zone separately for debugging or incremental work:

```bash
# Part 1: Download datasets (optional, requires Kaggle credentials)
python Part1_Landing_zone/data_collector.py

# Part 2: Structural formatting
python Part2_Formatting_zone/formatting_pipeline.py

# Part 3: Data quality and validation
python Part3_Trusted_zone/trusted_pipeline.py

# Part 4: Knowledge Graph generation
python Part4_Exploitation_zone/exploitation_pipeline.py

# Part 5: ML models and analysis
python Part5_Analysis_zone/analysis_pipeline.py
python Part5_Analysis_zone/kg_analysis_pipeline.py
```

## ✅ Verify Success

After running the full pipeline, check for these outputs:

**DuckDB Databases:**
```
Part2_Formatting_zone/formatted_zone/formatted.duckdb
Part3_Trusted_zone/trusted_zone/trusted.duckdb
Part4_Exploitation_zone/exploitation_zone/exploitation.duckdb
```

**Knowledge Graph:**
```
Part4_Exploitation_zone/exploitation_zone/kg/health_risk_kg.ttl
Part4_Exploitation_zone/exploitation_zone/kg/schema/health_risk_schema.ttl
```

**ML Models and Reports:**
```
Part5_Analysis_zone/models/integrated_core_model.pkl
Part5_Analysis_zone/models/integrated_enriched_model.pkl
Part5_Analysis_zone/reports/summary_report.json
```

## 🎯 What Each Zone Produces

| Zone | Main Output | Records |
|------|-------------|---------|
| Part 2 | formatted.duckdb | Raw structured records |
| Part 3 | trusted.duckdb | Cleaned, validated records |
| Part 4 | KG Turtle files | RDF triples for semantic analysis |
| Part 5 | Models + Reports | Trained classifiers + predictions |

## 🐛 Troubleshooting

### Python version wrong
```bash
python --version  # Should be 3.11 or 3.12
# Re-run environment setup if needed
```

### Java not found
```bash
java -version  # Should be "openjdk version 17"
# Linux/WSL: sudo apt install openjdk-17-jdk
# Windows: Script installs it automatically
```

### Spark startup fails
```
java.lang.UnsupportedOperationException: getSubject is not supported
```
→ You have Java 25 installed. Reinstall Java 17:
```bash
# Linux/WSL:
sudo apt install openjdk-17-jdk
update-alternatives --set java /usr/lib/jvm/java-17-openjdk-amd64/bin/java
```

### Landing Zone missing
Ensure `Part1_Landing_zone/landing_zone/` directory exists with at least one timestamped dataset folder from a previous run.

## 📖 Next Steps

- View zone documentation: each `Part*/README.md`
- Explore DuckDB databases: query with SQL
- Inspect Knowledge Graph: see `Part4_Exploitation_zone/KG_METAMODEL.md`
- Review ML results: open `Part5_Analysis_zone/reports/summary_report.json`
- Run Jupyter notebooks: `notebooks/`
update-alternatives --set javac /usr/lib/jvm/java-17-openjdk-amd64/bin/javac
export JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
```

Successful WSL command:

```powershell
wsl -d Ubuntu-Codex -u root -- bash -lc "cd /mnt/c/Users/Claudia/Documents/Github/bda-practica2 && source .venv/bin/activate && export JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64 && python run_all_pipeline.py --skip-landing --strict"
```

WSL result:

- Part2 Formatting: OK
- Part3 Trusted: OK
- Part4 Exploitation: OK
- Part5 KG Analysis: OK
- Part5 Analysis: OK
- Final status: `Pipeline finished`

The WSL command was run again after the shared cross-platform Spark runtime helper was added. The final-code WSL run also completed with `Pipeline finished`.

Key WSL output counts:

- Trusted cardiovascular disease rows: 68,610
- Trusted health indicators rows: 253,680
- Trusted Cleveland rows: 297
- Trusted quarantine rows: 1,390
- Exploitation risk model rows: 322,587
- KG generated records: 298,688
- KG generated aggregate measurements: 645
- KG generated triples: 3,081,855
- Analytics KG triples: 5,568

## Windows Attempt And Fixes

Native Windows was adapted after the WSL run passed. The runtime helper now accepts:

- Linux/WSL system Java 17
- Windows `JAVA_HOME`
- Windows `java` on `PATH`
- Windows `jdk4py==17.0.9.2`

Windows Python dependencies were installed with:

```powershell
C:\Users\Claudia\.cache\codex-runtimes\codex-primary-runtime\dependencies\python\python.exe -m pip install -r requirements.txt
```

The first Windows Spark run reached Spark startup but failed because Hadoop native Windows binaries were missing:

```text
Did not find winutils.exe
java.lang.UnsatisfiedLinkError: org.apache.hadoop.io.nativeio.NativeIO$Windows.access0
```

Fix:

```powershell
New-Item -ItemType Directory -Force -Path .hadoop\bin
Invoke-WebRequest -Uri "https://github.com/cdarlint/winutils/raw/master/hadoop-3.3.5/bin/winutils.exe" -OutFile .hadoop\bin\winutils.exe
Invoke-WebRequest -Uri "https://github.com/cdarlint/winutils/raw/master/hadoop-3.3.5/bin/hadoop.dll" -OutFile .hadoop\bin\hadoop.dll
```

Successful Windows command:

```powershell
C:\Users\Claudia\.cache\codex-runtimes\codex-primary-runtime\dependencies\python\python.exe run_all_pipeline.py --skip-landing --strict
```

Windows result:

- Part2 Formatting: OK
- Part3 Trusted: OK
- Part4 Exploitation: OK
- Part5 KG Analysis: OK
- Part5 Analysis: OK
- Final status: `Pipeline finished`

Key Windows output counts:

- Trusted cardiovascular disease rows: 68,610
- Trusted health indicators rows: 253,680
- Trusted Cleveland rows: 297
- Trusted quarantine rows: 1,390
- Exploitation risk model rows: 322,587
- KG generated records: 298,688
- KG generated aggregate measurements: 645
- KG generated triples: 3,081,855
- Analytics KG triples: 5,568

Windows emitted some non-fatal Spark/Hadoop warnings such as `Acceso denegado` and transient Netty connection reset warnings after successful stages. The process exited with code 0 and all pipeline stages reported OK.

## Reproducibility Notes

- Use Java 17 for Spark 3.5. Java 25 fails during Spark startup.
- On Windows, keep `.hadoop/bin/winutils.exe` and `.hadoop/bin/hadoop.dll` as local ignored runtime files.
- Landing was skipped in both verified runs because existing snapshots were already present.
- Report timestamps change on each run. Model metrics remained stable between repeated analysis runs after artifacts were fixed.
