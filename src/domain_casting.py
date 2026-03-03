import pandas as pd
import numpy as np
from .casting import to_int, to_float, to_bool, to_date


def cast_patients(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()

    out["age"] = to_int(out["age"])
    out["arrival_yearweek"] = to_int(out["arrival_yearweek"])
    out["departure_yearweek"] = to_int(out["departure_yearweek"])
    out["los"] = to_int(out["los"])

    out["satisfaction"] = to_float(
        out["satisfaction"].str.replace(",", ".", regex=False)
    )

    out["arrival_date"] = to_date(out["arrival_date"])
    out["departure_date"] = to_date(out["departure_date"])

    out["service"] = out["service"].str.strip().str.lower()
    out["patient_id"] = out["patient_id"].str.strip()
    out["request_id"] = out["request_id"].str.strip()

    return out


def cast_staff(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    out["staff_name"] = out["staff_name"].str.strip()
    out["role"] = out["role"].str.strip().str.lower()
    out["service"] = out["service"].str.strip().str.lower()
    return out


def cast_staff_schedule(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    out["yearweek"] = to_int(out["yearweek"])
    out["staff_name"] = out["staff_name"].str.strip()
    out["present"] = to_bool(out["present"])
    return out


def cast_admissions_requests(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    out["request_id"] = out["request_id"].str.strip()
    out["yearweek"] = to_int(out["yearweek"])
    out["service"] = out["service"].str.strip().str.lower()
    out["accepted"] = to_bool(out["accepted"])
    out["reason"] = out["reason"].replace("", np.nan).astype("string").str.lower()
    return out