SELECT
    ANY_VALUE(tbas.ckey)                                                     AS ckey
  , ANY_VALUE(tbas.account)                                                  AS account
  , tbas.domain_hash                                                         AS domain_hash
  , ANY_VALUE(tbas.domain_name)                                              AS domain_name

  , tbas.date

  , tbas.referrer
  , tbas.device_type
  , tbas.app_type
  , tbas.country
  , tbas.region
  , tbas.income_group
  , tbas.locale_country
  , tbas.locale_language

  , tbas.store_group_id
  , tbas.store_id

  , COUNT(tbas.session_id)                                                   AS bopis_cvt_sessions
  , COUNT(tbas.num_orders)                                                   AS bopis_orders
  , SUM(tbas.order_amount)                                                   AS bopis_revenue
  , SUM(tbas.products_added_to_cart)                                         AS bopis_a2c
  , SUM(tbas.add_to_cart_final_amount)                                       AS bopis_a2c_revenue
  , SUM(tbose.product_units_purchased)                                       AS bopis_purchase_units
  , SUM(tbose.txn_price)                                                     AS bopis_purchase_revenue
FROM
    {{ ref("tmp_bopis_order_split_event_aggregates") }} tbose
    INNER JOIN {{ ref("tmp_bopis_actual_sessions") }} tbas
    ON  tbose.domain_hash    = tbas.domain_hash
    AND tbose.date           = tbas.date
    AND tbose.session_id     = tbas.session_id
    AND tbose.store_group_id = tbas.store_group_id
    AND tbose.store_id       = tbas.store_id
GROUP BY
    tbas.domain_hash
  , tbas.date

  , tbas.referrer
  , tbas.device_type
  , tbas.app_type
  , tbas.country
  , tbas.region
  , tbas.income_group
  , tbas.locale_country
  , tbas.locale_language

  , tbas.store_group_id
  , tbas.store_id
