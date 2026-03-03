# 🏥 Projet Datawarehouse - Clinique

Projet pédagogique de mise en place d’un système décisionnel complet
dans le contexte d’une clinique privée.

Le projet est structuré en 3 TP successifs :

- **TP1** : Pipeline Data Engineering (RAW → BRONZE → SILVER)
- **TP2** : Modélisation décisionnelle (GOLD) + Dashboard
- **TP3** : Industrialisation / spécialisation (IA, CYBER, DEV, INFRA)

---

## 🎯 Contexte et objectif

La clinique souhaite mieux piloter son activité :

- Identifier les services en tension
- Suivre le taux d’occupation
- Analyser les refus de patients
- Étudier la satisfaction
- Comprendre l’impact de la présence du personnel
- Anticiper les semaines critiques

L’objectif du projet est de construire une architecture décisionnelle
complète permettant de répondre à ces problématiques.

---

## 📂 Structure du projet

```

data/                   # Fichiers CSV (couche RAW)
scripts/                # Scripts d’ingestion
models/
	silver/             # Modèles dbt SILVER
	gold/               # Modèles dbt GOLD
dwh/
	clinic_dwh.db       # Base SQLite
hospital_dwh/           # Projet dbt
dbt_profiles/           # Configuration dbt

````

---

## 🗄️ Architecture des données

Les couches sont séparées par convention de nommage :

- `bronze_*` : données brutes historisées
- `silver_*` : données nettoyées et typées
- `gold_*`   : datamart décisionnel (faits et dimensions)

---

## ⚙️ Prérequis

- Python ≥ 3.10
- SQLite
- dbt-core + dbt-sqlite
- (optionnel) Power BI
- (optionnel TP3) Docker, Airflow, FastAPI

Installation des dépendances Python :

```bash
pip install -r requirements.txt
````

---

## ▶️ Exécution du projet

### 1️⃣ Ingestion des données (BRONZE)

```bash
make ingest
```

ou

```bash
python scripts/ingest_bronze.py
```

---

### 2️⃣ Transformation SILVER

```bash
make silver
```

ou

```bash
dbt run --select silver --profiles-dir .
dbt test --select silver --profiles-dir .
```

---

### 3️⃣ Construction GOLD

```bash
make gold
```

ou

```bash
dbt run --select gold --profiles-dir .
dbt test --select gold --profiles-dir .
```

---

### 4️⃣ Exécution complète du pipeline

```bash
make all
```

---

## 📊 Indicateurs produits (couche GOLD)

* Nombre de patients présents
* Nombre de demandes
* Nombre de refus
* Taux de refus
* Taux d’occupation
* Satisfaction moyenne
* Durée moyenne de séjour
* Présence du personnel

Grain principal :

> 1 ligne = 1 service × 1 semaine

---

## 🧠 TP3 - Extensions possibles

Selon la spécialité :

* IA : prédiction des semaines en tension
* CYBER : sécurisation et gouvernance des données
* DEV : API REST + Docker
* INFRA : orchestration Airflow + industrialisation

---

## 📝 Choix techniques

* SQLite pour simplifier l’environnement
* dbt pour structurer les transformations
* Architecture médaillon (Bronze / Silver / Gold)
* Séparation logique par convention de nommage
* Dashboard basé uniquement sur la couche GOLD

---

## 🚀 Améliorations possibles

* Migration vers PostgreSQL
* Ajout de CI/CD
* Déploiement cloud
* Monitoring automatisé
