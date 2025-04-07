select 
  date_trunc('day', ts) as date,      -- ตัดเวลาของ ts ให้เหลือแค่วันที่
  max(aqi_cn) as max_aqi             -- หาค่าสูงสุดของ aqi_cn ในแต่ละวัน
from 
  {{ source('public', 'iqa') }}  -- ใช้แหล่งข้อมูล 'weather_air_quality' จาก public schema
group by 
  date_trunc('day', ts)               -- กลุ่มข้อมูลตามวันที่
order by 
  date                               -- เรียงข้อมูลตามวันที่


