from pathlib import Path
import pandas as pd

def read_csv_safe(path: Path) -> pd.DataFrame:
    """
    Lecture CSV robuste (types en string au départ, nettoyage minimal).
    On cast ensuite proprement (dates / numériques).
    """
    return pd.read_csv(path, sep=";", dtype=str, keep_default_na=True, na_values=["", "NA", "N/A", "null", "None"])
