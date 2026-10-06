-- Satu instance Postgres untuk dua database:
--   airflow   -> metadata Airflow (dibuat lewat POSTGRES_DB)
--   warehouse -> data warehouse (raw, staging, marts)
create database warehouse;
