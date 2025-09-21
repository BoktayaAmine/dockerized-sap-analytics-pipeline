
  create or replace   view SAP_ANALYTICS_DB.ANALYTICS.stg_countries
  
   as (
    select
    "LAND1" as country_code,
    "LANDX" as country_name
from SAP_ANALYTICS_DB.RAW_STAGING.RAW_T005T
  );

