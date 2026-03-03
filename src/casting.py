import pandas as pd

def to_int(series: pd.Series) -> pd.Series:
    return pd.to_numeric(series, errors="coerce").astype("Int64")

def to_float(series: pd.Series) -> pd.Series:
    return pd.to_numeric(series, errors="coerce").astype("Float64")

def to_bool(series: pd.Series) -> pd.Series:
    s = pd.to_numeric(series, errors="coerce")
    return s.map({0: False, 1: True}).astype("boolean")

def to_date(series: pd.Series) -> pd.Series:
    return pd.to_datetime(series, errors="coerce", dayfirst=True).dt.date