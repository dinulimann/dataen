# Menjalankan ketiga proyek sebagai satu platform, dengan Airflow (proyek 01)
# sebagai control plane untuk lakehouse (03) dan pemantauan streaming (02).

.PHONY: help up down clean status lakehouse health

help:  ## Tampilkan daftar perintah
	@grep -E '^[a-zA-Z_-]+:.*?## ' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "  %-10s %s\n", $$1, $$2}'

up:  ## Build image lakehouse, jalankan stack streaming dan Airflow
	$(MAKE) -C 03-lakehouse build
	$(MAKE) -C 02-streaming-pipeline up
	$(MAKE) -C 01-batch-elt-pipeline up
	@echo ""
	@echo "Airflow   http://localhost:8080  (admin/admin)"
	@echo "Grafana   http://localhost:3000"
	@echo "Lanjutkan: make lakehouse  |  make health"

lakehouse:  ## Jalankan lakehouse lewat Airflow, mis. make lakehouse MONTH=2024-01 SOURCE=download
	$(MAKE) -C 01-batch-elt-pipeline trigger-lakehouse $(if $(MONTH),MONTH=$(MONTH)) $(if $(SOURCE),SOURCE=$(SOURCE))

health:  ## Aktifkan DAG pemantauan kesehatan streaming
	$(MAKE) -C 01-batch-elt-pipeline health

status:  ## Tampilkan status container ketiga proyek
	@for d in 01-batch-elt-pipeline 02-streaming-pipeline 03-lakehouse; do \
		echo "== $$d"; (cd $$d && docker compose ps --format 'table {{.Service}}\t{{.Status}}'); done

down:  ## Hentikan semua stack
	$(MAKE) -C 01-batch-elt-pipeline down
	$(MAKE) -C 02-streaming-pipeline down

clean:  ## Hentikan semua stack dan hapus seluruh data
	$(MAKE) -C 01-batch-elt-pipeline clean
	$(MAKE) -C 02-streaming-pipeline clean
	$(MAKE) -C 03-lakehouse clean
