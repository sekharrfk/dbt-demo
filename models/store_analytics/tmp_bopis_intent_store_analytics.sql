SELECT
    ANY_VALUE(ckey)                                                     AS ckey
  , ANY_VALUE(account)                                                  AS account
  , domain_hash
  , ANY_VALUE(domain_name)                                              AS domain_name

  , date

  , referrer
  , device_type
  , app_type
  , country
  , region
  , income_group
  , locale_country
  , locale_language

  , store_group_id
  , store_id

  , COUNT(session_id)                                                   AS  bopis_intent_sessions
  , SUM(num_orders)                                                     AS  bopis_intent_orders
  , SUM(order_amount)                                                   AS  bopis_intent_revenue
  , SUM(products_added_to_cart)                                         AS  bopis_intent_a2c
  , SUM(add_to_cart_final_amount)                                       AS  bopis_intent_a2c_revenue
  , SUM(IFF(num_orders > 0, 1, 0))                                      AS  bopis_intent_cvt_sessions
  , SUM(products_purchase_units)                                        AS  bopis_intent_purchase_units
  , SUM(products_purchase_txn_amount)                                   AS  bopis_intent_purchase_revenue
FROM
    {{ ref("tmp_bopis_intent_sessions") }}
GROUP BY
    domain_hash
  , date

  , referrer
  , device_type
  , app_type
  , country
  , region
  , income_group
  , locale_country
  , locale_language

  , store_group_id
  , store_id