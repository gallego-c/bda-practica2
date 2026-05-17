Running this project in Linux/WSL (Ubuntu / Debian)

Native Windows Python is not supported for this project. Run all setup and pipeline commands inside your Linux/WSL shell.

1) Start WSL (Windows Terminal → your distro) or run:

```powershell
wsl
```

2) Update packages and install system deps (inside WSL):

```bash
sudo apt update && sudo apt upgrade -y
sudo apt install -y python3-pip build-essential default-jdk
```

Note: `default-jdk` is recommended for `pyspark`.
`apache-airflow==2.10.5` requires Python `<3.13`, so use Python 3.12 in WSL.

If your distro is Ubuntu and `python3.12` is not available yet, install it with the deadsnakes PPA:

```bash
sudo apt install -y software-properties-common
sudo add-apt-repository ppa:deadsnakes/ppa -y
sudo apt update
sudo apt install -y python3.12 python3.12-venv
```

If you already have `python3.11` or `python3.12`, that is also fine for this project.

3) Make the helper script executable and run it from the project root:

```bash
chmod +x scripts/setup_env.sh
./scripts/setup_env.sh
```

If your distro has more than one compatible Python version installed, you can force it explicitly:

```bash
PYTHON_BIN=python3.12 ./scripts/setup_env.sh
```

4) Activate the environment in your shell:

```bash
source .venv/bin/activate
```

5) Common notes
- The script creates a local virtual environment in `.venv` and installs requirements from `requirements.txt` and `Part1_Landing_zone/requirements.txt` when present.
- If you prefer to manually manage dependencies, run the commands in the script step-by-step instead of executing it.
- If `python3.12` is not available in your distro, install a compatible Python first and rerun the script with `PYTHON_BIN=python3.12` or another Python `<3.13` such as `python3.11`.
