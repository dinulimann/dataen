select
    city_id,
    city_name,
    province,
    island,
    latitude,
    longitude
from {{ ref('cities') }}
