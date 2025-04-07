select
  min(aqi_cn) as min_aqi_cn
from 
  {{ source('public', 'iqa') }}
where 
  ts >= current_date - interval '14 days'