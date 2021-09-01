SELECT
    ckey
  , account
  , domain_hash
  , domain_name

  , date
  , session_id

  , referrer
  , device_type
  , app_type
  , country
  , region
  , ETL.income_group(median_income)                                  AS  income_group
  , locale_country
  , locale_language

  , num_orders
  , order_amount
  , products_added_to_cart
  , add_to_cart_final_amount
  , products_purchase_units
  , products_purchase_txn_amount
  , store
FROM
    RFK_DEV.PUBLIC.user_sessions_v2
WHERE
    date = TO_DATE('{{ var("CURRENT_DATE") }}', 'YYYYMMDD')
