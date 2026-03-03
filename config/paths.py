import os
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1] # tp_hospital/

DATA_DIR = ROOT / "data"
DWH_DB = ROOT / "dwh" / "clinic_dwh.db"
