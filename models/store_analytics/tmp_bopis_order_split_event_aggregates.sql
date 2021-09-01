SELECT
    domain_hash
  , date

  , event_data:store:group_id::STRING                                   AS  store_group_id
  , event_data:store:id::STRING                                         AS  store_id

  , session_id

  , SUM(quantity)                                                       AS product_units_purchased
  , SUM(txn_price)                                                      AS txn_price
FROM
    {{ ref("tmp_user_events") }}
WHERE
    event_name  = 'order_split'
    AND event_data:store IS NOT NULL AND LOWER(event_data:shipping_method) = 'bopis'
GROUP BY
    domain_hash
  , date
  , store_group_id
  , store_id
  , session_id