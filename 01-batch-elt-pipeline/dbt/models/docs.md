{% docs __overview__ %}
# Warehouse Cuaca Indonesia

Dokumentasi model dbt untuk **proyek 01 · Batch ELT Pipeline** dari
[portofolio data engineering ini](https://github.com/dinulimann/dataen).

Setiap hari Airflow mengambil data cuaca 10 kota di Indonesia dari
[Open-Meteo](https://open-meteo.com/), menyimpan JSON mentahnya di MinIO,
lalu memuatnya ke `raw.open_meteo_daily`. Dari sana dbt membangun:

| Lapisan | Model | Isi |
|---|---|---|
| staging | `stg_open_meteo__daily` | nama kolom & satuan yang jelas |
| marts | `dim_city` | dimensi kota (dari seed) |
| marts | `fct_weather_daily` | fakta harian per kota, *incremental* |
| marts | `mart_city_monthly_weather` | ringkasan bulanan siap dashboard |

**Cara membaca halaman ini:** klik ikon grafik di kanan bawah untuk melihat
*lineage* (alur data dari source sampai dashboard), atau pilih model di
panel kiri untuk melihat kolom, tes kualitas data, dan SQL-nya.

Semua model dites setiap kali `dbt build` dijalankan (unik, not null,
relasi, rentang nilai, dan `temp_min_c <= temp_max_c`).
{% enddocs %}

{% docs city_id %}
ID kota dalam huruf kecil (mis. `jakarta`). Berasal dari seed `cities` yang
juga dipakai proses ingestion, sehingga selalu konsisten.
{% enddocs %}

{% docs weather_date %}
Tanggal pengamatan dalam zona waktu Asia/Jakarta (WIB).
{% enddocs %}

{% docs temp_max_c %}Suhu maksimum harian pada ketinggian 2 m, dalam °C.{% enddocs %}
{% docs temp_min_c %}Suhu minimum harian pada ketinggian 2 m, dalam °C.{% enddocs %}
{% docs temp_mean_c %}Suhu rata-rata harian pada ketinggian 2 m, dalam °C.{% enddocs %}
{% docs precipitation_mm %}Total curah hujan harian, dalam milimeter.{% enddocs %}
{% docs precipitation_hours %}Jumlah jam dalam sehari yang tercatat ada hujan.{% enddocs %}
{% docs wind_speed_max_kmh %}Kecepatan angin maksimum harian pada 10 m, dalam km/jam.{% enddocs %}

{% docs weather_code %}
Kode cuaca [WMO](https://open-meteo.com/en/docs#weathervariables) yang
dominan hari itu (0 = cerah, 1–3 = berawan, 61–65 = hujan, 95–99 = badai petir, dst).
{% enddocs %}

{% docs ingested_at %}
Waktu baris dimuat atau terakhir diperbarui di tabel raw. Dipakai model
incremental untuk memproses hanya data baru.
{% enddocs %}
