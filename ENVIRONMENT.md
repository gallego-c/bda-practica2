# Environment Setup

Choose the setup instructions for your operating system.

## ⚙️ System Requirements

- **Python:** 3.11 or 3.12
- **Java:** 17 (required for Spark; **do not use Java 25**)
- **PySpark:** 3.5.0
- **Kaggle credentials:** Only needed if downloading new datasets

## 🐧 Linux / WSL Setup

### 1. Install System Dependencies

```bash
sudo apt update && sudo apt upgrade -y
sudo apt install -y python3-pip python3-venv build-essential openjdk-17-jdk
export JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
```

**Note on Python version:** If your distro has Python 3.13+, install Python 3.11 or 3.12:

```bash
sudo apt install -y software-properties-common
sudo add-apt-repository ppa:deadsnakes/ppa -y
sudo apt update
sudo apt install -y python3.12 python3.12-venv
```

### 2. Run Setup Script

From the project root:

```bash
chmod +x scripts/setup_env.sh
./scripts/setup_env.sh
```

To force a specific Python version:

```bash
PYTHON_BIN=python3.12 ./scripts/setup_env.sh
```

### 3. Activate Environment

```bash
source .venv/bin/activate
```

## 🪟 Windows Setup

### 1. Run PowerShell Setup Script

From the project root in PowerShell:

```powershell
.\scripts\setup_env_windows.ps1
```

This script:
- Creates `.venv-win` virtual environment
- Installs all requirements
- Downloads Hadoop native binaries for Windows Spark
- Installs Java 17 locally via `jdk4py` package

### 2. Run the Pipeline

```powershell
.\.venv-win\Scripts\python.exe run_all_pipeline.py --skip-landing --strict
```

### (Optional) Manual Hadoop Setup for Windows

If you prefer to install Hadoop binaries manually instead of letting the script do it:

```powershell
New-Item -ItemType Directory -Force -Path .hadoop\bin
Invoke-WebRequest -Uri "https://github.com/cdarlint/winutils/raw/master/hadoop-3.3.5/bin/winutils.exe" -OutFile .hadoop\bin\winutils.exe
Invoke-WebRequest -Uri "https://github.com/cdarlint/winutils/raw/master/hadoop-3.3.5/bin/hadoop.dll" -OutFile .hadoop\bin\hadoop.dll
```

## 🔑 Kaggle Credentials (Optional)

Required only if you need to download new datasets.

### Linux/WSL Setup

```bash
mkdir -p ~/.kaggle
cp /path/to/your/kaggle.json ~/.kaggle/kaggle.json
chmod 600 ~/.kaggle/kaggle.json
```

### Windows Setup

Place your `kaggle.json` at:
```
C:\Users\<YourUsername>\.kaggle\kaggle.json
```

Get your credentials from [Kaggle Settings](https://www.kaggle.com/settings).

## ✅ Verify Installation

After setup, verify everything is working:

```bash
# Activate environment (adjust for your OS)
source .venv/bin/activate          # Linux/WSL
.\.venv-win\Scripts\Activate.ps1   # Windows PowerShell

# Check Python
python --version  # Should be 3.11 or 3.12

# Check Java
java -version  # Should be openjdk-17

# Check Spark
python -c "import pyspark; print(pyspark.__version__)"  # Should be 3.5.0

# Try running a zone
python Part2_Formatting_zone/formatting_pipeline.py
```

## 📝 Environment Details

- Virtual environment location: `.venv` (Linux/WSL) or `.venv-win` (Windows)
- Requirements file: `requirements.txt`
- Landing Zone additional requirements: `Part1_Landing_zone/requirements.txt`
- Hadoop native files (Windows): `.hadoop/bin/`
