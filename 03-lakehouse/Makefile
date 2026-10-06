MONTHS ?= 2024-01 2024-02
SQL ?= select * from gold_hourly_demand order by avg_trips_per_day desc limit 10
TABLE ?= silver/trips
LH = docker compose run --rm lakehouse

.PHONY: help build sample download run maintain query history clean setup test lint

help:  ## Tampilkan daftar perintah
	@grep -E '^[a-zA-Z_-]+:.*?## ' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "  %-9s %s\n", $$1, $$2}'

build:  ## Build image Docker (Python + Java + Spark + Delta)
	docker compose build

sample:  ## Buat data contoh berskema TLC untuk MONTHS
	$(LH) sample --months $(MONTHS)

download:  ## Unduh data asli NYC TLC untuk MONTHS (~50 MB/bulan)
	$(LH) download --months $(MONTHS)

run:  ## Bangun bronze -> silver -> gold untuk MONTHS
	$(LH) run --months $(MONTHS)

maintain:  ## OPTIMIZE + Z-ORDER + VACUUM tabel silver
	$(LH) maintain

query:  ## Jalankan SQL, mis. make query SQL="select * from gold_payment_summary"
	$(LH) query "$(SQL)"

history:  ## Riwayat versi tabel Delta, mis. make history TABLE=silver/trips
	$(LH) history $(TABLE)

clean:  ## Hapus data mentah dan seluruh lakehouse
	rm -rf data lakehouse

setup:  ## Install dependensi untuk development lokal (butuh Java 17+)
	pip install -e ".[dev]"

test:  ## Unit test (Spark + Delta lokal)
	pytest -q

lint:  ## Cek gaya kode
	ruff check src tests
	ruff format --check src tests
