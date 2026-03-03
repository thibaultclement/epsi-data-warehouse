import pandas as pd
import numpy as np
import matplotlib.pyplot as plt

#fonction spécifique à IPython / Jupyter qui permet d’afficher “proprement” des objets riches (DataFrame, graphiques, HTML, etc.) dans un notebook
from IPython.display import display 


def basic_profile(df: pd.DataFrame, name: str) -> None:
    """
    Affiche un profil rapide pour l'exploration.
    """
    print("=" * 80)
    print(f"DATASET: {name}")
    print("-" * 80)
    print(f"Shape: {df.shape[0]} lignes x {df.shape[1]} colonnes")
    print("\nAperçu:")
    display(df.head(5))

    print("\nTypes (pandas dtypes):")
    display(df.dtypes.to_frame("dtype"))

    print("\nValeurs manquantes (top 20):")
    missing = df.isna().sum().sort_values(ascending=False)
    display(missing.head(20).to_frame("missing_count"))

    print("\nStatistiques (numériques si déjà castés):")
    num_cols = df.select_dtypes(include=[np.number, "Int64", "Float64"]).columns.tolist()
    if num_cols:
        display(df[num_cols].describe().T)
    else:
        print("Aucune colonne numérique castée pour l'instant.")

    print("\nValeurs distinctes (colonnes catégorielles - top 10):")
    cat_cols = [c for c in df.columns if c not in num_cols]
    for c in cat_cols[:10]:
        nunique = df[c].nunique(dropna=True)
        print(f"- {c}: {nunique} valeurs distinctes")
    print()



def build_data_dictionary(df: pd.DataFrame, selected_cols: list, definitions: dict | None = None) -> pd.DataFrame:
    """
    Construit un dictionnaire de données simple :
    nom, type, définition (si fournie), exemple, nb manquants, nb distincts.
    """
    definitions = definitions or {}

    rows = []
    for col in selected_cols:
        if col not in df.columns:
            continue
        s = df[col]
        example = s.dropna().iloc[0] if s.dropna().shape[0] else None
        rows.append({
            "column": col,
            "dtype": str(s.dtype),
            "definition": definitions.get(col, ""),
            "example": example,
            "missing_count": int(s.isna().sum()),
            "distinct_count": int(s.nunique(dropna=True)),
        })
    return pd.DataFrame(rows)



def suggest_interesting_columns(dfs: dict) -> dict:
    return {
        "patients": ["patient_id", "name", "age", "arrival_yearweek", "departure_yearweek", "arrival_date", "departure_date", "service", "satisfaction", "los", "request_id"],
        "staff": ["staff_name", "role", "service"],
        "staff_schedule": ["yearweek", "staff_name", "present"],
        "admissions_requests": ["request_id", "yearweek", "service", "accepted", "reason"],
    }



def plot_histogram(series, title, bins=20):
    """
    Trace un histogramme propre pour une variable numérique.
    """
    data = series.dropna()

    plt.figure(figsize=(8, 4))
    plt.hist(data, bins=bins)
    plt.axvline(data.mean(), linestyle="--")
    plt.title(title)
    plt.xlabel(series.name)
    plt.ylabel("Fréquence")
    plt.grid(axis="y", linestyle="--", alpha=0.6)
    plt.tight_layout()
    plt.show()

    print(f"{series.name} -> mean={round(data.mean(),2)} | std={round(data.std(),2)}")