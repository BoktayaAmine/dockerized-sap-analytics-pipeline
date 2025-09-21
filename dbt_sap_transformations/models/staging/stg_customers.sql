select
    "KUNNR" as customer_id,
    "LAND1" as country_code,
    "NAME1" as customer_name,
    "ORT01" as city,
    "PSTLZ" as postal_code
from {{ source('RAW_STAGING', 'RAW_KNA1') }}