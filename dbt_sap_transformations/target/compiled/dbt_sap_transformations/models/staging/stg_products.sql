select
    "MATNR" as product_id,
    "MAKTX" as product_description,
    "MTART" as product_type,
    try_to_decimal("BRGEW", 10, 2) as gross_weight
from SAP_ANALYTICS_DB.RAW_STAGING.RAW_MARA