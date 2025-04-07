select 
  avg(aqi_cn) as avg_aqi_this_month
from 
  {{ source('public', 'iqa') }}
where 
  date_trunc('month', ts) = date_trunc('month', current_date)