with source as (
    select * from {{ source('open_meteo', 'open_meteo_daily') }}
)

select
    city_id,
    obs_date                     as weather_date,
    weather_code,
    temperature_max              as temp_max_c,
    temperature_min              as temp_min_c,
    temperature_mean             as temp_mean_c,
    precipitation_sum            as precipitation_mm,
    precipitation_hours,
    wind_speed_max               as wind_speed_max_kmh,
    source_file,
    ingested_at
from source
