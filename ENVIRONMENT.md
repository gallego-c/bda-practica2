Running this project in Linux/WSL or Windows

The pipeline has been verified in WSL/Linux and native Windows. Spark 3.5 should run with Java 17. Avoid Java 25 because Hadoop/Spark fails during startup with `Subject.getSubject is not supported`.

## WSL / Linux

1) Update packages and install system dependencies:

```bash
sudo apt update && sudo apt upgrade -y
sudo apt install -y python3-pip python3-venv build-essential openjdk-17-jdk
export JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
```

`apache-airflow==2.10.5` requires Python `<3.13`, so use Python 3.11 or 3.12 if you install the Landing Zone Airflow dependencies.

If your distro is Ubuntu and `python3.12` is not available yet, install it with the deadsnakes PPA:

```bash
sudo apt install -y software-properties-common
sudo add-apt-repository ppa:deadsnakes/ppa -y
sudo apt update
sudo apt install -y python3.12 python3.12-venv
```

If you already have `python3.11` or `python3.12`, that is also fine for this project.

2) Make the helper script executable and run it from the project root:

```bash
chmod +x scripts/setup_env.sh
./scripts/setup_env.sh
```

If your distro has more than one compatible Python version installed, you can force it explicitly:

```bash
PYTHON_BIN=python3.12 ./scripts/setup_env.sh
```

3) Activate the environment in your shell:

```bash
source .venv/bin/activate
```

## Windows

1) Create a Windows virtual environment:

```powershell
.\scripts\setup_env_windows.ps1
```

The helper creates `.venv-win`, installs `requirements.txt`, and downloads the Hadoop native files listed below. `requirements.txt` includes `jdk4py==17.0.9.2` only on Windows, which provides a local Java 17 runtime when no system Java is configured.

2) If you prefer to install Hadoop native binaries manually for Windows Spark local file access:

```powershell
New-Item -ItemType Directory -Force -Path .hadoop\bin
Invoke-WebRequest -Uri "https://github.com/cdarlint/winutils/raw/master/hadoop-3.3.5/bin/winutils.exe" -OutFile .hadoop\bin\winutils.exe
Invoke-WebRequest -Uri "https://github.com/cdarlint/winutils/raw/master/hadoop-3.3.5/bin/hadoop.dll" -OutFile .hadoop\bin\hadoop.dll
```

3) Run the pipeline:

```powershell
.\.venv-win\Scripts\python.exe run_all_pipeline.py --skip-landing --strict
```

## Common notes

- The script creates a local virtual environment in `.venv` and installs requirements from `requirements.txt` and `Part1_Landing_zone/requirements.txt` when present.
- If you prefer to manually manage dependencies, run the commands in the script step-by-step instead of executing it.
- If `python3.12` is not available in your distro, install a compatible Python first and rerun the script with `PYTHON_BIN=python3.12` or another Python `<3.13` such as `python3.11`.
