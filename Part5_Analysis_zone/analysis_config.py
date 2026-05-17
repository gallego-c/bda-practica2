from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
SCRIPT_DIR = Path(__file__).resolve().parent

EXPLOITATION_DUCKDB_PATH = PROJECT_ROOT / "Part4_Exploitation_zone" / "exploitation_zone" / "exploitation.duckdb"
MODELS_DIR = SCRIPT_DIR / "models"
REPORTS_DIR = SCRIPT_DIR / "reports"
FIGURES_DIR = SCRIPT_DIR / "figures"

RANDOM_SEED = 42
TEST_SIZE = 0.20
CV_FOLDS = 3
