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
  , income_group
  , locale_country
  , locale_language

  , store:group_id::STRING                                           AS  store_group_id
  , store:id::STRING                                                 AS  store_id

  , session_id

  , num_orders
  , order_amount
  , products_added_to_cart
  , add_to_cart_final_amount
  , products_purchase_units
  , products_purchase_txn_amount
FROM
    {{ ref("tmp_user_sessions") }}
WHERE
    -- Fetches only BOPIS INTENT sessions. For adhoc data, use adhoc_user_sessions table
    store IS NOT NULL