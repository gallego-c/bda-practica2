#!/usr/bin/env bash
set -euo pipefail

echo "Creating Python virtual environment at .venv"

if [ "$(uname -s)" != "Linux" ]; then
  echo "ERROR: This project is supported only inside Linux/WSL."
  echo "Open your WSL distro, cd to the repo, and rerun ./scripts/setup_env.sh there."
  exit 1
fi

PYTHON_BIN="${PYTHON_BIN:-}"
if [ -z "$PYTHON_BIN" ]; then
  for candidate in python3.12 python3.11 python3.10 python3; do
    if command -v "$candidate" >/dev/null 2>&1; then
      PYTHON_BIN="$candidate"
      break
    fi
  done
fi

if [ -z "$PYTHON_BIN" ]; then
  echo "ERROR: No Python interpreter found. Install Python 3.12 in WSL and rerun with PYTHON_BIN=python3.12."
  echo "On Ubuntu WSL you can use the deadsnakes PPA: sudo add-apt-repository ppa:deadsnakes/ppa -y && sudo apt install python3.12 python3.12-venv"
  exit 1
fi

PYTHON_VERSION="$($PYTHON_BIN -c 'import sys; print(f"{sys.version_info.major}.{sys.version_info.minor}")')"
case "$PYTHON_VERSION" in
  3.12|3.11|3.10|3.9|3.8)
    ;;
  *)
    echo "ERROR: $PYTHON_BIN is version $PYTHON_VERSION. apache-airflow==2.10.5 requires Python >=3.8,<3.13."
    echo "Install Python 3.12 in WSL and rerun with: PYTHON_BIN=python3.12 ./scripts/setup_env.sh"
    exit 1
    ;;
esac

echo "Using $PYTHON_BIN ($PYTHON_VERSION)"

if ! command -v java >/dev/null 2>&1; then
  echo "ERROR: Java was not found. Install a JDK before running the Spark pipeline."
  echo "On Ubuntu/Debian WSL: sudo apt update && sudo apt install -y default-jdk"
  exit 1
fi

echo "Using Java: $(java -version 2>&1 | head -n 1)"

"$PYTHON_BIN" -m venv .venv

# Activate venv for remainder of script
# shellcheck disable=SC1091
. .venv/bin/activate

echo "Upgrading pip and installing packages from requirements files..."
python -m pip install --upgrade pip wheel setuptools

REQS=("requirements.txt" "Part1_Landing_zone/requirements.txt")
for f in "${REQS[@]}"; do
  if [ -f "$f" ]; then
    echo "Installing from $f"
    pip install -r "$f"
  else
    echo "Not found: $f"
  fi
done

echo "Setup complete. Activate the venv with: source .venv/bin/activate"
