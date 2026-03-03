import argparse
import os
from pathlib import Path
from datetime import datetime, timezone

import pandas as pd
from sqlalchemy import create_engine, text

DATA_DIR = Path("data")
DB_PATH = Path("dwh") / "clinic_dwh.db"


def ensure_dirs():
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)


def get_engine():
    return create_engine(f"sqlite:///{DB_PATH.as_posix()}")


def infer_table_name(csv_path: Path) -> str:
    # admissions_requests.csv -> bronze_admissions_requests
    return "bronze_" + csv_path.stem.lower()


def load_csv(csv_path: Path) -> pd.DataFrame:
    # TP: séparateur ; ; garder brut (pas de parsing de dates ici)
    df = pd.read_csv(csv_path, sep=";", dtype=str, keep_default_na=False)
    # keep_default_na=False => évite que "" devienne NaN automatiquement
    return df


def get_next_batch_id(engine) -> int:
    with engine.connect() as conn:
        conn.execute(text("CREATE TABLE IF NOT EXISTS bronze_ingestion_batches (batch_id INTEGER PRIMARY KEY, ingestion_ts TEXT)"))
        res = conn.execute(text("SELECT COALESCE(MAX(batch_id), 0) + 1 FROM bronze_ingestion_batches"))
        next_id = int(res.scalar())
        conn.execute(
            text("INSERT INTO bronze_ingestion_batches (batch_id, ingestion_ts) VALUES (:batch_id, :ts)"),
            {"batch_id": next_id, "ts": datetime.now(timezone.utc).isoformat()},
        )
        conn.commit()
        return next_id


def ingest_one(engine, csv_path: Path, batch_id: int):
    table = infer_table_name(csv_path)
    df = load_csv(csv_path)

    # colonnes techniques
    ingestion_ts = datetime.now(timezone.utc).isoformat()
    df["batch_id"] = str(batch_id)
    df["ingestion_ts"] = ingestion_ts
    df["source_file_name"] = csv_path.name

    # append: historisation native en bronze
    df.to_sql(table, engine, if_exists="append", index=False)
    print(f"[OK] {csv_path.name} -> {table} ({len(df)} rows) batch_id={batch_id}")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--data-dir", default=str(DATA_DIR))
    args = parser.parse_args()

    ensure_dirs()
    engine = get_engine()

    data_dir = Path(args.data_dir)
    csv_files = sorted(data_dir.glob("*.csv"))
    if not csv_files:
        raise FileNotFoundError(f"No CSV files found in {data_dir.resolve()}")

    batch_id = get_next_batch_id(engine)
    for csv_path in csv_files:
        ingest_one(engine, csv_path, batch_id)


if __name__ == "__main__":
    main()