import argparse
from pathlib import Path
from datetime import datetime, timezone

import pandas as pd
from sqlalchemy import create_engine, text

DATA_DIR = Path("data")
DB_PATH = Path("dwh") / "clinic_dwh.db"


# 🔑 Clés métier par table
BUSINESS_KEYS = {
    "bronze_patients": ["patient_id"],
    "bronze_admissions_requests": ["request_id"],
    "bronze_staff": ["staff_name", "role"],
    "bronze_staff_schedule": ["yearweek", "staff_name"],
}


def ensure_dirs():
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)


def get_engine():
    return create_engine(f"sqlite:///{DB_PATH.as_posix()}")


def infer_table_name(csv_path: Path) -> str:
    return "bronze_" + csv_path.stem.lower()


def load_csv(csv_path: Path) -> pd.DataFrame:
    return pd.read_csv(
        csv_path,
        sep=";",
        dtype=str,
        keep_default_na=False
    )


def get_next_batch_id(engine) -> int:
    with engine.begin() as conn:
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS bronze_ingestion_batches (
                batch_id INTEGER PRIMARY KEY,
                ingestion_ts TEXT
            )
        """))
        result = conn.execute(text("SELECT COALESCE(MAX(batch_id), 0) + 1 FROM bronze_ingestion_batches"))
        batch_id = int(result.scalar())

        conn.execute(
            text("INSERT INTO bronze_ingestion_batches (batch_id, ingestion_ts) VALUES (:b, :ts)"),
            {"b": batch_id, "ts": datetime.now(timezone.utc).isoformat()}
        )

    return batch_id


def get_existing_keys(engine, table_name: str, key_cols: list[str]) -> pd.DataFrame:
    try:
        query = f"SELECT {', '.join(key_cols)} FROM {table_name}"
        return pd.read_sql(query, engine)
    except Exception:
        # Table n'existe pas encore
        return pd.DataFrame(columns=key_cols)


def filter_new_records(df: pd.DataFrame, existing: pd.DataFrame, keys: list[str]) -> pd.DataFrame:
    if existing.empty:
        return df

    merged = df.merge(existing.drop_duplicates(), on=keys, how="left", indicator=True)
    return merged[merged["_merge"] == "left_only"].drop(columns=["_merge"])


def ingest_one(engine, csv_path: Path, batch_id: int):
    table = infer_table_name(csv_path)
    df = load_csv(csv_path)

    keys = BUSINESS_KEYS.get(table)
    if not keys:
        raise ValueError(f"No business keys defined for {table}")

    existing_keys = get_existing_keys(engine, table, keys)
    df_new = filter_new_records(df, existing_keys, keys)

    if df_new.empty:
        print(f"[SKIP] {csv_path.name} → no new records")
        return

    ingestion_ts = datetime.now(timezone.utc).isoformat()
    df_new["batch_id"] = batch_id
    df_new["ingestion_ts"] = ingestion_ts
    df_new["source_file_name"] = csv_path.name

    df_new.to_sql(table, engine, if_exists="append", index=False)

    print(f"[OK] {csv_path.name} → {len(df_new)} new rows (batch {batch_id})")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--data-dir", default=str(DATA_DIR))
    args = parser.parse_args()

    ensure_dirs()
    engine = get_engine()

    batch_id = get_next_batch_id(engine)

    data_dir = Path(args.data_dir)
    for csv_path in sorted(data_dir.glob("*.csv")):
        ingest_one(engine, csv_path, batch_id)


if __name__ == "__main__":
    main()