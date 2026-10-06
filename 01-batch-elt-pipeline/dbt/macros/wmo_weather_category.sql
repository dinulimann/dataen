{# Kelompokkan kode cuaca WMO (dipakai Open-Meteo) ke kategori yang mudah dibaca. #}
{% macro wmo_weather_category(code) -%}
    case
        when {{ code }} = 0 then 'cerah'
        when {{ code }} between 1 and 3 then 'berawan'
        when {{ code }} in (45, 48) then 'berkabut'
        when {{ code }} between 51 and 67 then 'hujan'
        when {{ code }} between 80 and 82 then 'hujan'
        when {{ code }} between 95 and 99 then 'badai petir'
        else 'lainnya'
    end
{%- endmacro %}
