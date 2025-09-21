select
    "LAND1" as country_code,
    "LANDX" as country_name
from {{ source('RAW_STAGING', 'RAW_T005T') }}