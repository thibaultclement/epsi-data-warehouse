# ===============================
# Datawarehouse - Clinique
# ===============================

DBT_PROJECT = hospital_dwh
DBT_PROFILES = dbt_profiles

.PHONY: ingest silver gold all clean

ingest:
	python scripts/ingest_bronze.py

silver:
	dbt run --project-dir $(DBT_PROJECT) --profiles-dir $(DBT_PROFILES) --select silver
	dbt test --project-dir $(DBT_PROJECT) --profiles-dir $(DBT_PROFILES) --select silver

gold:
	dbt run --project-dir $(DBT_PROJECT) --profiles-dir $(DBT_PROFILES) --select gold
	dbt test --project-dir $(DBT_PROJECT) --profiles-dir $(DBT_PROFILES) --select gold

all: ingest silver gold

clean:
	rm -f dwh/clinic_dwh.db