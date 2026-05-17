import argparse
import os
import shutil
import subprocess
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent


def java_available() -> bool:
    if os.environ.get("JAVA_HOME") and (Path(os.environ["JAVA_HOME"]) / "bin" / "java").exists():
        return True
    return shutil.which("java") is not None


def require_linux_runtime() -> None:
    if sys.platform == "linux":
        return

    print("[ERROR] This project is supported only inside Linux/WSL.")
    print("[ERROR] Open your WSL distro, cd to the repo, activate .venv, and rerun the pipeline there.")
    raise SystemExit(1)


def require_java_for_spark_steps() -> None:
    if java_available():
        return

    print("[ERROR] Java was not found. Spark-based steps require a JDK.")
    print("[ERROR] In WSL Ubuntu/Debian, install it with:")
    print("[ERROR]   sudo apt update && sudo apt install -y default-jdk")
    print("[ERROR] Then rerun:")
    print("[ERROR]   source .venv/bin/activate && python run_all_pipeline.py --skip-landing --strict")
    raise SystemExit(1)


def run_step(name: str, script_path: Path, cwd: Path, strict: bool = True) -> bool:
    print(f"\n=== [{name}] Running: {script_path} (cwd={cwd}) ===")
    cmd = [sys.executable, str(script_path)]
    proc = subprocess.run(cmd, cwd=str(cwd), check=False)
    if proc.returncode != 0:
        print(f"[ERROR] Step '{name}' failed with code {proc.returncode}")
        if strict:
            return False
        print(f"[WARN] Continuing despite failure in '{name}' (strict=False)")
    else:
        print(f"[OK] Step '{name}' completed")
    return True


def kaggle_config_available() -> bool:
    kaggle_json = Path.home() / ".kaggle" / "kaggle.json"
    return kaggle_json.exists()


def main() -> None:
    parser = argparse.ArgumentParser(description="Run complete BDA pipeline end-to-end.")
    parser.add_argument("--skip-landing", action="store_true", help="Skip Part1 landing collector")
    parser.add_argument("--strict", action="store_true", help="Stop on first failing step")
    args = parser.parse_args()

    strict = args.strict

    require_linux_runtime()

    steps = []

    landing_script = PROJECT_ROOT / "Part1_Landing_zone" / "data_collector.py"
    if args.skip_landing:
        print("[INFO] Landing step skipped by flag.")
    elif not kaggle_config_available():
        print("[WARN] Landing skipped: ~/.kaggle/kaggle.json not found.")
        print("[WARN] Use --strict with configured kaggle credentials if you want to enforce Part1.")
    else:
        steps.append(("Part1_Landing", landing_script, PROJECT_ROOT / "Part1_Landing_zone"))

    require_java_for_spark_steps()

    steps.extend([
        ("Part2_Formatting", PROJECT_ROOT / "Part2_Formatting_zone" / "formatting_pipeline.py", PROJECT_ROOT),
        ("Part3_Trusted", PROJECT_ROOT / "Part3_Trusted_zone" / "trusted_pipeline.py", PROJECT_ROOT),
        ("Part4_Exploitation", PROJECT_ROOT / "Part4_Exploitation_zone" / "exploitation_pipeline.py", PROJECT_ROOT),
        ("Part5_KG_Analysis", PROJECT_ROOT / "Part5_Analysis_zone" / "kg_analysis_pipeline.py", PROJECT_ROOT),
        ("Part5_Analysis", PROJECT_ROOT / "Part5_Analysis_zone" / "analysis_pipeline.py", PROJECT_ROOT),
    ])

    for name, script, cwd in steps:
        ok = run_step(name, script, cwd, strict=strict)
        if not ok:
            raise SystemExit(1)

    print("\n=== Pipeline finished ===")


if __name__ == "__main__":
    main()
