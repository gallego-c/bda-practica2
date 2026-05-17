# Pipeline Run Log

Date: 2026-05-17

## Goal

Verify that the complete medical data pipeline runs end to end, first in WSL and then on native Windows, using existing Landing Zone snapshots.

Command under test:

```bash
python run_all_pipeline.py --skip-landing --strict
```

## WSL Attempt And Fixes

Initial non-elevated WSL calls reported no registered distributions:

```powershell
wsl --list --all --verbose
wsl -e bash -lc "echo WSL_OK"
```

The elevated WSL context showed existing distributions and a new test distribution was registered:

```powershell
wsl --install Ubuntu --name Ubuntu-Codex --no-launch
wsl -d Ubuntu-Codex -u root -- bash -lc "echo WSL_OK && uname -a"
```

Ubuntu-Codex started successfully, but its default Python was 3.14 and Java was missing. The existing project `.venv` had the needed Python packages available, so only Java needed correction.

Java setup:

```bash
apt-get update
apt-get install -y default-jdk
```

First WSL pipeline run failed at Spark startup because `default-jdk` on Ubuntu 26.04 installed Java 25:

```text
java.lang.UnsupportedOperationException: getSubject is not supported
```

Fix:

```bash
apt-get install -y openjdk-17-jdk
update-alternatives --set java /usr/lib/jvm/java-17-openjdk-amd64/bin/java
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
