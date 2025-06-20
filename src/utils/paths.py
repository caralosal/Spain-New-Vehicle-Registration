from pathlib import Path

BASE_DIR = Path(__file__).resolve().parents[2]
DATA_DIR = BASE_DIR / "data"

BRONZE = DATA_DIR / "bronze"
SILVER = DATA_DIR / "silver"
GOLD   = DATA_DIR / "gold"