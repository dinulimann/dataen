.PHONY: help setup up down clean logs trigger test lint dbt-local psql

help:  ## Tampilkan daftar perintah
	@grep -E '^[a-zA-Z_-]+:.*?## ' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "  %-12s %s\n", $$1, $$2}'

setup:  ## Install dependensi untuk development lokal
	pip install -e ".[dev]"

up:  ## Jalankan seluruh stack (Postgres, MinIO, Airflow)
	@test -f .env || cp .env.example .env
	docker compose up -d --build

down:  ## Hentikan stack
	docker compose down

clean:  ## Hentikan stack dan hapus semua data
	docker compose down -v

logs:  ## Lihat log scheduler
	docker compose logs -f airflow-scheduler

trigger:  ## Jalankan DAG secara manual
	docker compose exec airflow-scheduler airflow dags unpause weather_elt
	docker compose exec airflow-scheduler airflow dags trigger weather_elt

test:  ## Jalankan unit test
	pytest -q

lint:  ## Cek gaya kode
	ruff check src dags tests scripts
	ruff format --check src dags tests scripts

dbt-local:  ## Muat data contoh lalu dbt build (butuh Postgres di localhost:5432)
	python scripts/load_fixtures.py
	cd dbt && dbt build --profiles-dir .

psql:  ## Buka psql ke database warehouse
	docker compose exec postgres psql -U postgres -d warehouse
