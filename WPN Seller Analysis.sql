select distinct
wpn.seller_key
,so.product_amount_usd
from ANALYTICS.SOURCES.SOURCE_GOOGLE_SHEETS__WPN_ACTIVATIONS as wpn
left join ANALYTICS.CORE.SELLERS as s
on s.key = wpn.seller_key
left join ANALYTICS.CORE.SELLER_ORDERS as so 
on so.seller_id = s.id
left join ANALYTICS.CORE.ORDER_ITEMS as oi 
on so.id = oi.seller_order_id
where oi.product_line = 'Magic' and wpn.wpn_activation_date = '2025-06-12' and so.seller_order_status = 'Complete'


select * from ANALYTICS.SOURCES.SOURCE_GOOGLE_SHEETS__WPN_ACTIVATIONS as wpn where wpn_activation_date = '2025-06-12'
select * from ANALYTICS.CORE.DAILY_WPN_SALES where product_category = 'MTG' and order_date between'2025-06-02 00:00:00' and '2026-01-01 00:00:00'

select wpn.*, s.is_direct from ANALYTICS.CORE.DAILY_WPN_SALES as wpn 
inner join ANALYTICS.CORE.SELLERS as s
on wpn.seller_key = s.key
where product_category = 'MTG' and order_date between'2025-06-02 00:00:00' and '2026-01-01 00:00:00'

select * from ANALYTICS.SOURCES.SOURCE_GOOGLE_SHEETS__WPN_ACTIVATIONS as wpn
select * from ANALYTICS.CORE.SELLER_ORDERS limit 100


WITH base_orders AS (
    SELECT
        wpn.seller_key,
        wpn.wpn_activation_date::date AS activation_date,
        oi.ordered_at_et::date AS order_date,
        oi.total_usd as product_amount_usd
    FROM ANALYTICS.SOURCES.SOURCE_GOOGLE_SHEETS__WPN_ACTIVATIONS AS wpn
    JOIN ANALYTICS.CORE.SELLERS AS s
        ON s.key = wpn.seller_key
    JOIN ANALYTICS.CORE.SELLER_ORDERS AS so
        ON so.seller_id = s.id
    JOIN ANALYTICS.CORE.ORDER_ITEMS AS oi
        ON so.id = oi.seller_order_id
    WHERE
        wpn.wpn_activation_date = '2025-06-12'
        AND so.seller_order_status = 'Complete'
        AND oi.product_line = 'Magic'
        AND so.ordered_at_et BETWEEN
            DATEADD(month, -3, wpn.wpn_activation_date)
            AND DATEADD(month,  3, wpn.wpn_activation_date)
),

labeled_periods AS (
    SELECT
        seller_key,
        product_amount_usd,
        CASE
            WHEN order_date < activation_date THEN 'pre_3_months'
            WHEN order_date >= activation_date THEN 'post_3_months'
        END AS period
    FROM base_orders
)

SELECT
    period,
    COUNT(DISTINCT seller_key) AS sellers,
    SUM(product_amount_usd) AS total_gmv,
    AVG(product_amount_usd) AS avg_item_gmv
FROM labeled_periods
GROUP BY period
ORDER BY period;


WITH base AS (
  SELECT
      wpn.seller_key,
      wpn.wpn_activation_date::date AS activation_date,
      so.ordered_at_et::date           AS order_date,
      oi.total_usd         AS gmv
  FROM ANALYTICS.SOURCES.SOURCE_GOOGLE_SHEETS__WPN_ACTIVATIONS AS wpn
  JOIN ANALYTICS.CORE.SELLERS AS s
    ON s.key = wpn.seller_key
  JOIN ANALYTICS.CORE.SELLER_ORDERS AS so
    ON so.seller_id = s.id
  JOIN ANALYTICS.CORE.ORDER_ITEMS AS oi
    ON so.id = oi.seller_order_id
  WHERE
      wpn.wpn_activation_date = '2025-06-12'
      AND so.seller_order_status = 'Complete'
      AND oi.product_line = 'Magic'
      AND so.ordered_at_et::date >= DATEADD(month, -3, wpn.wpn_activation_date::date)
      AND so.ordered_at_et::date <  DATEADD(month,  3, wpn.wpn_activation_date::date)
),
seller_pre_post AS (
  SELECT
    seller_key,
    activation_date,
    SUM(CASE WHEN order_date <  activation_date THEN gmv ELSE 0 END) AS gmv_pre_3mo,
    SUM(CASE WHEN order_date >= activation_date THEN gmv ELSE 0 END) AS gmv_post_3mo
  FROM base
  GROUP BY 1,2
)
SELECT
  seller_key,
  activation_date,
  gmv_pre_3mo,
  gmv_post_3mo,
  (gmv_post_3mo - gmv_pre_3mo) AS gmv_change,
  (gmv_post_3mo - gmv_pre_3mo) / NULLIF(gmv_pre_3mo, 0) AS gmv_lift_pct
FROM seller_pre_post
ORDER BY gmv_lift_pct DESC NULLS LAST;

WITH base AS (
  SELECT
      wpn.seller_key,
      wpn.wpn_activation_date::date AS activation_date,
      so.ordered_at_et::date        AS order_date,
      oi.total_usd                  AS gmv
  FROM ANALYTICS.SOURCES.SOURCE_GOOGLE_SHEETS__WPN_ACTIVATIONS AS wpn
  JOIN ANALYTICS.CORE.SELLERS AS s
    ON s.key = wpn.seller_key
  JOIN ANALYTICS.CORE.SELLER_ORDERS AS so
    ON so.seller_id = s.id
  JOIN ANALYTICS.CORE.ORDER_ITEMS AS oi
    ON so.id = oi.seller_order_id
  WHERE
      wpn.wpn_activation_date = '2025-06-12'
      AND so.seller_order_status = 'Complete'
      AND oi.product_line = 'Magic'
      AND so.ordered_at_et::date >= DATEADD(month, -6, wpn.wpn_activation_date::date)
      AND so.ordered_at_et::date <  DATEADD(month,  6, wpn.wpn_activation_date::date)
),
seller_pre_post AS (
  SELECT
    seller_key,
    activation_date,
    SUM(CASE WHEN order_date <  activation_date THEN gmv ELSE 0 END) AS gmv_pre_6mo,
    SUM(CASE WHEN order_date >= activation_date THEN gmv ELSE 0 END) AS gmv_post_6mo
  FROM base
  GROUP BY 1,2
)
SELECT
  seller_key,
  activation_date,
  gmv_pre_6mo,
  gmv_post_6mo,
  (gmv_post_6mo - gmv_pre_6mo) AS gmv_change,
  (gmv_post_6mo - gmv_pre_6mo) / NULLIF(gmv_pre_6mo, 0) AS gmv_lift_pct
FROM seller_pre_post
ORDER BY gmv_lift_pct DESC NULLS LAST;


select * from SEGMENT.MARKETPLACE_PRD.MARKETPLACE_FILTER_SELECTED where seller_filter = 'Wizards Play Network' and received_at between '2025-06-02 00:00:00' and '2026-01-01 00:00:00'

select count (distinct id), count(distinct user_id) from 
SEGMENT.MARKETPLACE_PRD.MARKETPLACE_FILTER_SELECTED 
where seller_filter = 'Wizards Play Network' and received_at between '2025-06-02 00:00:00' and '2026-01-01 00:00:00'

select  count (distinct cart.id),count(distinct sf.user_id) from 
SEGMENT.MARKETPLACE_PRD.MARKETPLACE_SEARCH_ADDED_TO_CART as cart
inner join SEGMENT.MARKETPLACE_PRD.MARKETPLACE_FILTER_SELECTED as sf
on cart.user_id = sf.user_id
where sf.received_at between '2025-06-02 00:00:00' and '2026-01-01 00:00:00'and sf.seller_filter = 'Wizards Play Network' and cart.received_at between '2025-06-02 00:00:00' and '2026-01-01 00:00:00'

select  count (distinct o.id),count(distinct sf.user_id) from 
SEGMENT.MARKETPLACE_PRD.ORDER_SUBMITTED as o
inner join SEGMENT.MARKETPLACE_PRD.MARKETPLACE_FILTER_SELECTED as sf
on o.user_id = sf.user_id
where sf.received_at between '2025-06-02 00:00:00' and '2026-01-01 00:00:00'and sf.seller_filter = 'Wizards Play Network' and o.received_at between '2025-06-02 00:00:00' and '2026-01-01 00:00:00'

select count (distinct id), count(distinct user_id) from 
SEGMENT.MARKETPLACE_PRD.MARKETPLACE_FILTER_SELECTED 
where received_at between '2025-06-02 00:00:00' and '2026-01-01 00:00:00'

select  count (distinct cart.id),count(distinct sf.user_id) from 
SEGMENT.MARKETPLACE_PRD.MARKETPLACE_SEARCH_ADDED_TO_CART as cart
join SEGMENT.MARKETPLACE_PRD.MARKETPLACE_FILTER_SELECTED as sf
on cart.user_id = sf.user_id
where sf.received_at between '2025-06-02 00:00:00' and '2026-01-01 00:00:00'and sf.seller_filter = 'Wizards Play Network' and cart.received_at between '2025-06-02 00:00:00' and '2026-01-01 00:00:00'



select count (distinct id), count(distinct user_id) from 
SEGMENT.MARKETPLACE_PRD.MARKETPLACE_SEARCH_ADDED_TO_CART 
where received_at between '2025-06-02 00:00:00' and '2026-01-01 00:00:00'

select count (distinct id), count(distinct user_id) from 
SEGMENT.MARKETPLACE_PRD.ORDER_SUBMITTED
where received_at between '2025-06-02 00:00:00' and '2026-01-01 00:00:00'

SEGMENT.MARKETPLACE_PRD.PRODUCT_ADDED_TO_CART





select *
from ANALYTICS.SOURCES.SOURCE_GOOGLE_SHEETS__WPN_ACTIVATIONS as wpn
left join ANALYTICS.CORE.SELLERS as s
on s.key = wpn.seller_key



select * from 
ANALYTICS.CORE.SELLER_ORDERS

