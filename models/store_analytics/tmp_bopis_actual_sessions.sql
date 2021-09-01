SELECT DISTINCT
    tus.ckey
  , tus.account
  , tus.domain_hash
  , tus.domain_name

  , tus.date

  , tus.referrer
  , tus.device_type
  , tus.app_type
  , tus.country
  , tus.region
  , tus.income_group
  , tus.locale_country
  , tus.locale_language

  , tue.event_data:store:group_id::STRING                                AS  store_group_id
  , tue.event_data:store:id::STRING                                      AS  store_id

  , tus.session_id

  , tus.num_orders
  , tus.order_amount
  , tus.products_added_to_cart
  , tus.add_to_cart_final_amount
FROM
    {{ ref("tmp_user_sessions") }} tus
    INNER JOIN {{ ref("tmp_user_events") }} tue
    ON  tus.ckey       = tue.ckey
    AND tus.date       = tue.date
    AND tus.session_id = tue.session_id
WHERE
    tue.event_name  = 'order_split'
    AND tue.event_data:store IS NOT NULL AND LOWER(tue.event_data:shipping_method) = 'bopis'
