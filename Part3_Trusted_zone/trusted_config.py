from datetime import datetime, timezone
from pathlib import Path

SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = SCRIPT_DIR.parent

FORMATTED_DUCKDB_PATH = PROJECT_ROOT / "Part2_Formatting_zone" / "formatted_zone" / "formatted.duckdb"
TRUSTED_ZONE_ROOT = SCRIPT_DIR / "trusted_zone"
TRUSTED_PARQUET_ROOT = TRUSTED_ZONE_ROOT / "parquet"
TRUSTED_DUCKDB_PATH = TRUSTED_ZONE_ROOT / "trusted.duckdb"
STAGING_ROOT = SCRIPT_DIR / "_staging" / "formatted_exports"


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def build_run_id() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
