-- Ringkasan bulanan per kota: siap dipakai untuk dashboard.
select
    c.city_id,
    c.city_name,
    c.province,
    c.island,
    date_trunc('month', f.weather_date)::date                        as month,
    count(*)                                                         as days_observed,
    round(avg(f.temp_mean_c), 2)                                     as avg_temp_c,
    max(f.temp_max_c)                                                as max_temp_c,
    min(f.temp_min_c)                                                as min_temp_c,
    round(sum(f.precipitation_mm), 1)                                as total_precipitation_mm,
    count(*) filter (where f.is_rainy_day)                           as rainy_days,
    round(100.0 * count(*) filter (where f.is_rainy_day) / count(*), 1) as rainy_day_pct,
    count(*) filter (where f.weather_category = 'badai petir')       as thunderstorm_days
from {{ ref('fct_weather_daily') }} as f
join {{ ref('dim_city') }} as c using (city_id)
group by 1, 2, 3, 4, 5
