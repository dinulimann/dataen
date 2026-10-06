{{
    config(
        materialized='incremental',
        unique_key=['city_id', 'weather_date'],
        incremental_strategy='delete+insert',
    )
}}

with daily as (
    select * from {{ ref('stg_open_meteo__daily') }}
    {% if is_incremental() %}
    -- Hanya proses baris yang baru dimuat / diperbarui sejak run terakhir.
    where ingested_at > (select coalesce(max(ingested_at), '1900-01-01') from {{ this }})
    {% endif %}
)

select
    city_id,
    weather_date,
    temp_max_c,
    temp_min_c,
    temp_mean_c,
    temp_max_c - temp_min_c                                         as temp_range_c,
    precipitation_mm,
    precipitation_hours,
    precipitation_mm >= {{ var('rainy_day_threshold_mm') }}         as is_rainy_day,
    wind_speed_max_kmh,
    weather_code,
    {{ wmo_weather_category('weather_code') }}                      as weather_category,
    ingested_at
from daily
