import argparse
import os
import subprocess
import sys
from pathlib import Path

from spark_runtime import require_spark_compatible_java

PROJECT_ROOT = Path(__file__).resolve().parent


def require_java_for_spark_steps() -> None:
    try:
        require_spark_compatible_java()
    except RuntimeError as exc:
        print(f"[ERROR] {exc}")
        print("[ERROR] Linux/WSL: sudo apt update && sudo apt install -y openjdk-17-jdk")
        print("[ERROR] Windows: install OpenJDK 17 or run python -m pip install -r requirements.txt")
        raise SystemExit(1) from exc


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

    steps = []

    landing_script = PROJECT_ROOT / "Part1_Landing_zone" / "data_collector.py"
    if args.skip_landing:
        print("[INFO] Landing step skipped by flag.")
    elif not kaggle_config_available():
        if strict:
            print("[ERROR] Landing cannot run: ~/.kaggle/kaggle.json not found.")
            print("[ERROR] Configure Kaggle credentials or rerun with --skip-landing --strict.")
            raise SystemExit(1)
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
        ("Part5_KG_Embedding_ML", PROJECT_ROOT / "Part5_Analysis_zone" / "kg_embedding_pipeline.py", PROJECT_ROOT),
        ("Part5_Analysis", PROJECT_ROOT / "Part5_Analysis_zone" / "analysis_pipeline.py", PROJECT_ROOT),
    ])

    for name, script, cwd in steps:
        ok = run_step(name, script, cwd, strict=strict)
        if not ok:
            raise SystemExit(1)

    print("\n=== Pipeline finished ===")


if __name__ == "__main__":
    main()
