-- Sanity check: suhu minimum harian tidak boleh melebihi suhu maksimum.
select *
from {{ ref('fct_weather_daily') }}
where temp_min_c > temp_max_c
