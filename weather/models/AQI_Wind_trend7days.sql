select 
  date_trunc('day', ts) as date,   
  avg(aqi_cn) as avg_aqi_cn,          
  avg(wind_s) as avg_wind_s           
from 
  {{ source('public', 'iqa') }}  
where 
  ts >= current_date - interval '7 days' 
group by 1 
order by 1 
