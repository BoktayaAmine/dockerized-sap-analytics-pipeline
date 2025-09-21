select
    "VBELN" as sales_order_id,
    try_to_date("ERDAT") as creation_date,
    "AUART" as document_type,
    "KUNNR" as customer_id
from {{ source('RAW_STAGING', 'RAW_VBAK') }}