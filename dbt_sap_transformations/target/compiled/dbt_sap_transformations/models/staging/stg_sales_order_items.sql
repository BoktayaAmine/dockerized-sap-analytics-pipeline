select
    "VBELN" as sales_order_id,
    try_to_number("POSNR") as item_number,
    "MATNR" as product_id,
    try_to_number("KWMENG") as quantity,
    try_to_decimal("NETWR", 12, 2) as net_value
from SAP_ANALYTICS_DB.RAW_STAGING.RAW_VBAP