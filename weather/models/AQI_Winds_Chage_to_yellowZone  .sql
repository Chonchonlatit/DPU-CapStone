select
    ts,aqi_cn,wind_s
from {{source('public','iqa')}}
where aqi_cn >= 50