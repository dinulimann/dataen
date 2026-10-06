.PHONY: help setup up down clean logs dlq psql test lint

help:  ## Tampilkan daftar perintah
	@grep -E '^[a-zA-Z_-]+:.*?## ' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "  %-8s %s\n", $$1, $$2}'

setup:  ## Install dependensi untuk development lokal (butuh Java 17+)
	pip install -e ".[dev]"

up:  ## Jalankan Kafka, Spark job, producer, Postgres, Grafana
	docker compose up -d --build

down:  ## Hentikan stack
	docker compose down

clean:  ## Hentikan stack dan hapus data, checkpoint, dan arsip
	docker compose down -v
	rm -rf data

logs:  ## Lihat progres Spark job (throughput & event terlambat yang dibuang)
	docker compose logs -f spark-job

dlq:  ## Intip isi dead-letter queue
	docker compose exec kafka /opt/kafka/bin/kafka-console-consumer.sh \
		--bootstrap-server localhost:9092 --topic orders.dlq --from-beginning --max-messages 10

psql:  ## Buka psql ke database streaming
	docker compose exec postgres psql -U postgres -d streaming

test:  ## Unit test (Spark lokal)
	pytest -q

lint:  ## Cek gaya kode
	ruff check src tests
	ruff format --check src tests
