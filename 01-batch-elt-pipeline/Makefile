.PHONY: help setup up down clean logs trigger trigger-lakehouse health test lint dbt-local docs psql

LAKEHOUSE_HOST_PATH ?= $(abspath ../03-lakehouse)
MONTH ?= 2024-01
SOURCE ?= sample

help:  ## Tampilkan daftar perintah
	@grep -E '^[a-zA-Z_-]+:.*?## ' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "  %-12s %s\n", $$1, $$2}'

setup:  ## Install dependensi untuk development lokal
	pip install -e ".[dev]"

up:  ## Jalankan seluruh stack (Postgres, MinIO, Airflow)
	@test -f .env || cp .env.example .env
	LAKEHOUSE_HOST_PATH=$(LAKEHOUSE_HOST_PATH) docker compose up -d --build

down:  ## Hentikan stack
	docker compose down

clean:  ## Hentikan stack dan hapus semua data
	docker compose down -v

logs:  ## Lihat log scheduler
	docker compose logs -f airflow-scheduler

trigger:  ## Jalankan DAG secara manual
	docker compose exec airflow-scheduler airflow dags unpause weather_elt
	docker compose exec airflow-scheduler airflow dags trigger weather_elt

trigger-lakehouse:  ## Jalankan DAG lakehouse (proyek 03), mis. make trigger-lakehouse MONTH=2024-01 SOURCE=download
	@mkdir -p $(LAKEHOUSE_HOST_PATH)/data $(LAKEHOUSE_HOST_PATH)/lakehouse
	docker compose exec airflow-scheduler airflow dags unpause lakehouse_monthly
	docker compose exec airflow-scheduler airflow dags trigger lakehouse_monthly \
		-c '{"month": "$(MONTH)", "source": "$(SOURCE)"}'

health:  ## Aktifkan pemantauan kesehatan streaming (proyek 02)
	docker compose exec airflow-scheduler airflow dags unpause streaming_health

test:  ## Jalankan unit test
	pytest -q

lint:  ## Cek gaya kode
	ruff check src dags tests scripts
	ruff format --check src dags tests scripts

dbt-local:  ## Muat data contoh lalu dbt build (butuh Postgres di localhost:5432)
	python scripts/load_fixtures.py
	cd dbt && dbt build --profiles-dir .

docs:  ## Generate & buka dokumentasi dbt di http://localhost:8081 (butuh Postgres di localhost:5432)
	python scripts/load_fixtures.py
	cd dbt && dbt build --profiles-dir . && dbt docs generate --profiles-dir . && dbt docs serve --profiles-dir . --port 8081

psql:  ## Buka psql ke database warehouse
	docker compose exec postgres psql -U postgres -d warehouse
