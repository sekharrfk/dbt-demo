{{ config(materialized='table') }}

SELECT
    ckey
  , account
  , domain_hash
  , domain_name

  , date

  , referrer
  , device_type
  , app_type
  , country
  , region
  , locale_country
  , locale_language

  , session_id
  , event_name
  , ref_id
  , order_id
  , order_total
  , quantity
  , txn_price
  , event_data
FROM
    RFK_DEV.ETL.user_events_v2
WHERE
    date = TO_DATE('{{ var("CURRENT_DATE") }}', 'YYYYMMDD')
    AND event_name in ('order', 'order_split', 'pdp_view', 'a2c', 'a2w', 'widget_click')
    AND CASE WHEN event_name = 'a2c' THEN event_data:is_a2c_removed::BOOLEAN = false ELSE 1 END


