select distinct
so.buyer_id
from ANALYTICS.SOURCES.SOURCE_GOOGLE_SHEETS__WPN_ACTIVATIONS as wpn
left join ANALYTICS.CORE.SELLERS as s
on s.key = wpn.seller_key
left join ANALYTICS.CORE.SELLER_ORDERS as so 
on so.seller_id = s.id
left join ANALYTICS.CORE.ORDER_ITEMS as oi 
on so.id = oi.seller_order_id
where oi.product_line = 'Magic' and oi.ordered_at_et  between '2025-06-01 00:00:00' and '2025-12-01 00:00:00'

select * from FIVETRAN.GOOGLE_SHEETS.WPN_ACTIVATIONS limit 100
select seller_key, wpn_activation_date, wpn_deactivation_date from ANALYTICS.SOURCES.SOURCE_GOOGLE_SHEETS__WPN_ACTIVATIONS 
select * from ANALYTICS.CORE.DAILY_WPN_SALES where product_category = 'MTG' and seller_type = 'WPN' limit 100
select * from ANALYTICS.CORE_STAGING.STAGE_WPN_SALES limit 100
select * from ANALYTICS.CORE.ORDER_ITEMS limit 100
select * from ANALYTICS.SOURCES.SOURCE_GOOGLE_SHEETS__WPN_ACTIVATIONS limit 100

select s.entity_id, wpn.seller_key, wpn.wpn_activation_date from ANALYTICS.SOURCES.SOURCE_GOOGLE_SHEETS__WPN_ACTIVATIONS as wpn
left join ANALYTICS.CORE.SELLERS as s
on s.key = wpn.seller_key



select wpn.seller_key
,s.id as seller_id
,wpn_activation_date
,oi.id as order_item_id
,oi.order_id
,oi.seller_order_id
,oi.product_condition_id
,oi.product_id
,oi.ordered_at_et
,oi.unit_price_usd
,oi.total_usd
,oi.quantity
,so.buyer_id
from ANALYTICS.SOURCES.SOURCE_GOOGLE_SHEETS__WPN_ACTIVATIONS as wpn
left join ANALYTICS.CORE.SELLERS as s
on s.key = wpn.seller_key
left join ANALYTICS.CORE.SELLER_ORDERS as so 
on so.seller_id = s.id
left join ANALYTICS.CORE.ORDER_ITEMS as oi 
on so.id = oi.seller_order_id
where oi.product_line = 'Magic' and oi.ordered_at_et  >= '2025-06-01 00:00:00'

select so.seller_id as seller_id
,oi.id as order_item_id
,oi.order_id
,oi.seller_order_id
,oi.product_condition_id
,oi.product_id
,oi.ordered_at_et
,oi.unit_price_usd
,oi.total_usd
,oi.quantity
,so.buyer_id
from ANALYTICS.CORE.SELLER_ORDERS as so 
left join ANALYTICS.CORE.ORDER_ITEMS as oi 
on so.id = oi.seller_order_id
where oi.product_line = 'Magic' and oi.ordered_at_et  between '2025-01-01 00:00:00' and '2025-06-01 00:00:00'


(select distinct so.buyer_id
,oi.id as order_item_id
,oi.order_id
,oi.product_condition_id
,oi.product_id
,oi.ordered_at_et
,oi.unit_price_usd
from ANALYTICS.SOURCES.SOURCE_GOOGLE_SHEETS__WPN_ACTIVATIONS as wpn
left join ANALYTICS.CORE.SELLERS as s
on s.key = wpn.seller_key
left join ANALYTICS.CORE.SELLER_ORDERS as so 
on so.seller_id = s.id
left join ANALYTICS.CORE.ORDER_ITEMS as oi 
on so.id = oi.seller_order_id
where oi.product_line = 'Magic' and oi.ordered_at_et  >= '2025-06-01 00:00:00') as sub1

(select so.buyer_id
,oi.id as order_item_id
,oi.order_id
,oi.product_condition_id
,oi.product_id
,oi.ordered_at_et
,oi.unit_price_usd
,oi.quantity
from ANALYTICS.CORE.SELLER_ORDERS as so 
left join ANALYTICS.CORE.ORDER_ITEMS as oi 
on so.id = oi.seller_order_id
where oi.product_line = 'Magic' and oi.ordered_at_et  between '2025-01-01 00:00:00' and '2025-06-01 00:00:00') as sub2

select 

select * from ANALYTICS.CORE.ORDER_ITEMS limit 100
select * from ANALYTICS.CORE.SELLER_INVENTORY limit 100



with sub1 as (
select distinct
so.buyer_id,
oi.id as order_item_id,
oi.order_id,
oi.product_condition_id,
oi.product_id,
oi.ordered_at_et,
oi.unit_price_usd,
oi.quantity
from ANALYTICS.SOURCES.SOURCE_GOOGLE_SHEETS__WPN_ACTIVATIONS as wpn
left join ANALYTICS.CORE.SELLERS as s
on s.key = wpn.seller_key
left join ANALYTICS.CORE.SELLER_ORDERS as so
on so.seller_id = s.id
left join ANALYTICS.CORE.ORDER_ITEMS as oi
on so.id = oi.seller_order_id
where oi.product_line = 'Magic'
and oi.ordered_at_et >= '2025-06-01 00:00:00'
),
sub2 as (
select
so.buyer_id,
oi.id as order_item_id,
oi.order_id,
oi.product_condition_id,
oi.product_id,
oi.ordered_at_et,
oi.unit_price_usd,
oi.quantity
from ANALYTICS.CORE.SELLER_ORDERS as so
left join ANALYTICS.CORE.ORDER_ITEMS as oi
on so.id = oi.seller_order_id
where oi.product_line = 'Magic'
and oi.ordered_at_et between '2025-01-01 00:00:00' and '2025-06-01 00:00:00'
)
select
s1.buyer_id,
s1.order_item_id as post_order_item_id,
s1.order_id as post_order_id,
s1.product_id,
s1.product_condition_id,
s1.ordered_at_et as post_ordered_at_et,
s1.unit_price_usd as post_unit_price_usd,
s2.order_item_id as pre_order_item_id,
s2.order_id as pre_order_id,
s2.ordered_at_et as pre_ordered_at_et,
s2.unit_price_usd as pre_unit_price_usd,
s2.quantity as pre_quantity
from sub1 s1
inner join sub2 s2
on s1.buyer_id = s2.buyer_id
and s1.product_id = s2.product_id
and s1.product_condition_id = s2.product_condition_id;

select * from ANALYTICS.CORE.AMPLITUDE_EVENTS limit 100
select * from ANALYTICS.CORE.SELLER_INVENTORY limit 100



/*Solution A: Same buyer + same seller + same product&condition (pre vs post)*/
select * 
from(
with wpn_sellers as (
select
s.id as seller_id,
wpn_activation_date as activation_ts
from ANALYTICS.SOURCES.SOURCE_GOOGLE_SHEETS__WPN_ACTIVATIONS wpn
join ANALYTICS.CORE.SELLERS s
on s.key = wpn.seller_key
where wpn.wpn_activation_date is not null
),
order_items as (
select
so.buyer_id,
so.seller_id,
oi.id as order_item_id,
oi.order_id,
oi.product_id,
oi.product_condition_id,
oi.ordered_at_et as ordered_at,
oi.unit_price_usd,
oi.quantity
from ANALYTICS.CORE.SELLER_ORDERS so
join ANALYTICS.CORE.ORDER_ITEMS oi
on so.id = oi.seller_order_id
where oi.product_line = 'Magic'
),
post as (
select
oi.*,
ws.activation_ts
from order_items oi
join wpn_sellers ws
on oi.seller_id = ws.seller_id
where oi.ordered_at >= ws.activation_ts
and oi.ordered_at <  ws.activation_ts + interval '90 days'
)
select
post.buyer_id,
post.seller_id,
post.product_id,
post.product_condition_id,

pre.order_item_id as pre_order_item_id,
pre.ordered_at as pre_ordered_at,
pre.unit_price_usd as pre_unit_price_usd,

post.order_item_id as post_order_item_id,
post.ordered_at as post_ordered_at,
post.unit_price_usd as post_unit_price_usd,

(post.unit_price_usd - pre.unit_price_usd) as delta_unit_price_usd,
post.activation_ts
from post
left join order_items pre
on pre.seller_id = post.seller_id
and pre.buyer_id = post.buyer_id
and pre.product_id = post.product_id
and pre.product_condition_id = post.product_condition_id
and pre.ordered_at >= post.activation_ts - interval '90 days'
and pre.ordered_at <  post.activation_ts
qualify row_number() over (
partition by post.order_item_id
order by pre.ordered_at desc
) = 1
)
where delta_unit_price_usd is not null;



/*Solution B: Same buyer + same product&condition, pre from any seller vs post from the WPN-activated seller*/
select *
from (
with wpn_sellers as (
select s.id as seller_id,
to_timestamp(wpn.wpn_activation_date) as activation_ts
from ANALYTICS.SOURCES.SOURCE_GOOGLE_SHEETS__WPN_ACTIVATIONS wpn
join ANALYTICS.CORE.SELLERS s
on s.key = wpn.seller_key
where wpn.wpn_activation_date is not null
),
order_items as (
select so.buyer_id, so.seller_id,
oi.id as order_item_id, oi.order_id,
oi.product_id, oi.product_condition_id,
oi.ordered_at_et as ordered_at,
oi.unit_price_usd, oi.quantity
from ANALYTICS.CORE.SELLER_ORDERS so
join ANALYTICS.CORE.ORDER_ITEMS oi
on so.id = oi.seller_order_id
where oi.product_line = 'Magic'
),
post as (
select oi.*, ws.activation_ts
from order_items oi
join wpn_sellers ws
on oi.seller_id = ws.seller_id
where oi.ordered_at >= ws.activation_ts
and oi.ordered_at <  ws.activation_ts + interval '90 days'
)
select
post.buyer_id,
post.seller_id as wpn_seller_id,
post.product_id,
post.product_condition_id,
pre.seller_id as pre_any_seller_id,
pre.order_item_id  as pre_order_item_id,
pre.ordered_at     as pre_ordered_at,
pre.unit_price_usd as pre_unit_price_usd,
post.order_item_id as post_order_item_id,
post.ordered_at    as post_ordered_at,
post.unit_price_usd as post_unit_price_usd,
(post.unit_price_usd - pre.unit_price_usd) as delta_unit_price_usd,
post.activation_ts
from post
left join order_items pre
on pre.buyer_id = post.buyer_id
and pre.product_id = post.product_id
and pre.product_condition_id = post.product_condition_id
and pre.ordered_at >= post.activation_ts - interval '90 days'
and pre.ordered_at <  post.activation_ts
qualify row_number() over (
partition by post.order_item_id
order by pre.ordered_at desc
) = 1
)
where delta_unit_price_usd is not null;


/*Solution C: Same day + same product & condition, WPN seller unit price vs inventory lowest price (***percentail price)*/

with wpn_orders as (
select
s.id as seller_id,
wpn.wpn_activation_date,
oi.id as order_item_id,
oi.seller_order_id,
oi.product_condition_id  as sku_id,
oi.product_id,
oi.ordered_at_et,
cast(oi.ordered_at_et as date) as order_date_et,
oi.unit_price_usd,
oi.total_usd,
oi.quantity,
so.buyer_id
from ANALYTICS.SOURCES.SOURCE_GOOGLE_SHEETS__WPN_ACTIVATIONS wpn
join ANALYTICS.CORE.SELLERS s
on s.key = wpn.seller_key
join ANALYTICS.CORE.SELLER_ORDERS so
on so.seller_id = s.id
join ANALYTICS.CORE.ORDER_ITEMS oi
on so.id = oi.seller_order_id
where oi.product_line = 'Magic'
and oi.ordered_at_et >= '2025-06-01 00:00:00'
),
bounds as (
select min(order_date_et) as min_dt, max(order_date_et) as max_dt from wpn_orders
),
need_skus as (
select distinct sku_id from wpn_orders
),
inv_min as (
select
inv.sku_id,
cast(inv.date_et as date) as date_et,
min(nullif(inv.price_usd, 0))  as min_price_usd  -- ignore zeros
from ANALYTICS.CORE.DAILY_SELLER_INVENTORY inv
join need_skus k
on k.sku_id = inv.sku_id
cross join bounds b
where inv.product_line = 'Magic'
and inv.quantity >= 1
and inv.date_et >= b.min_dt
and inv.date_et <  dateadd(day, 1, b.max_dt)
group by 1,2
)
select
o.*,
m.min_price_usd  as same_day_min_inventory_price_usd,
(o.unit_price_usd - m.min_price_usd)  as price_premium_usd,
-- avoid divide-by-zero; returns NULL when min_price_usd is 0 or NULL
(o.unit_price_usd / nullif(m.min_price_usd, 0)) - 1  as price_premium_pct,
case
when m.min_price_usd is null then null
when o.unit_price_usd <  m.min_price_usd then 'LOWER'
when o.unit_price_usd =  m.min_price_usd then 'EQUAL'
else 'HIGHER'
end as wpn_vs_same_day_min
from wpn_orders o
left join inv_min m
on m.sku_id  = o.sku_id
and m.date_et = o.order_date_et;

/* final solution with quantiles */
with wpn_orders as (
select
s.id                            as seller_id,
wpn.wpn_activation_date,
oi.id                           as order_item_id,
oi.seller_order_id,
oi.product_condition_id         as sku_id,
oi.product_id,
oi.ordered_at_et,
cast(oi.ordered_at_et as date)  as order_date_et,
oi.unit_price_usd,
oi.total_usd,
oi.quantity,
so.buyer_id
from ANALYTICS.SOURCES.SOURCE_GOOGLE_SHEETS__WPN_ACTIVATIONS wpn
join ANALYTICS.CORE.SELLERS s
on s.key = wpn.seller_key
join ANALYTICS.CORE.SELLER_ORDERS so
on so.seller_id = s.id
join ANALYTICS.CORE.ORDER_ITEMS oi
on so.id = oi.seller_order_id
where oi.product_line = 'Magic'
and oi.ordered_at_et between '2025-06-01 00:00:00' and '2025-11-01 00:00:00'
),
need_sku_dates as (
select distinct sku_id, order_date_et from wpn_orders
),
inv_filtered as (
select
inv.sku_id,
nsd.order_date_et as date_et,
inv.price_usd
from ANALYTICS.CORE.DAILY_SELLER_INVENTORY inv
join need_sku_dates nsd
on inv.sku_id = nsd.sku_id
and inv.date_et >= nsd.order_date_et
and inv.date_et <  dateadd(day, 1, nsd.order_date_et)  -- same calendar day, no casts
where inv.product_line = 'Magic' and inv.date_et between '2025-06-01 00:00:00' and '2025-11-01 00:00:00'
and inv.quantity >= 1
and inv.price_usd > 0  -- drop zeros to avoid divide-by-zero and bogus mins
),
inv_stats as (
select
sku_id,
date_et,
min(price_usd) as min_price_usd,
max(price_usd) as max_price_usd,
approx_percentile(price_usd, 0.25) as q1_price_usd,
approx_percentile(price_usd, 0.50) as median_price_usd,
approx_percentile(price_usd, 0.75) as q3_price_usd
from inv_filtered
group by 1,2
)
select
o.*,
s.min_price_usd    as same_day_min_inventory_price_usd,
s.max_price_usd    as same_day_max_inventory_price_usd,
s.q1_price_usd,
s.median_price_usd,
s.q3_price_usd,
(o.unit_price_usd - s.min_price_usd)                      as price_premium_usd,
(o.unit_price_usd / nullif(s.min_price_usd, 0)) - 1       as price_premium_pct,
case
when s.min_price_usd is null then null
when o.unit_price_usd <  s.min_price_usd     then 'below-min'
when o.unit_price_usd <  s.q1_price_usd      then 'min–q1'
when o.unit_price_usd <  s.median_price_usd  then 'q1–median'
when o.unit_price_usd <  s.q3_price_usd      then 'median–q3'
when o.unit_price_usd <= s.max_price_usd     then 'q3–max'
else 'above-max'
end as wpn_price_bucket
from wpn_orders o
inner join inv_stats s
on s.sku_id  = o.sku_id
and s.date_et = o.order_date_et;


select count(distinct buyer_id) from ANALYTICS.CORE.SELLER_ORDERS as so 
left join ANALYTICS.CORE.ORDER_ITEMS as oi 
on so.id = oi.seller_order_id
where oi.product_line = 'Magic' and oi.ordered_at_et  between '2025-06-01 00:00:00' and '2025-11-01 00:00:00'



select distinct
so.buyer_id
from ANALYTICS.SOURCES.SOURCE_GOOGLE_SHEETS__WPN_ACTIVATIONS as wpn
join ANALYTICS.CORE.SELLERS as s
on s.key = wpn.seller_key
join ANALYTICS.CORE.SELLER_ORDERS as so 
on so.seller_id = s.id
join ANALYTICS.CORE.ORDER_ITEMS as oi 
on oi.seller_order_id = so.id
where oi.product_line = 'Magic' and oi.ordered_at_et  between '2025-06-01 00:00:00' and '2025-12-01 00:00:00'

---------------------------------------------------------------------------
select * from ANALYTICS.CORE.ORDERS as o limit 100
-- Get buyer cohorts: WPN vs Non-WPN
WITH buyer_orders AS (
    SELECT
        o.buyer_id,
        o.id as order_id,
        o.ordered_at_et as order_date,
        o.total_usd,
        s.is_wpn
    FROM ANALYTICS.CORE.ORDERS o
    join ANALYTICS.CORE.SELLER_ORDERS as so 
    on so.order_id = o.id
    JOIN ANALYTICS.CORE.SELLERS s
      ON so.seller_id = s.seller_id
),

-- Identify each buyer’s first purchase date
first_orders AS (
    SELECT
        buyer_id,
        MIN(order_date) AS first_order_date
    FROM buyer_orders
    GROUP BY buyer_id
),

-- Add first order date and flag whether buyer ever bought from WPN
buyer_flags AS (
    SELECT
        b.buyer_id,
        MAX(CASE WHEN is_wpn THEN 1 ELSE 0 END) AS is_wpn_buyer,
        MIN(b.order_date) AS first_order_date
    FROM buyer_orders b
    GROUP BY b.buyer_id
),

-- Aggregate purchase activity by buyer
buyer_metrics AS (
    SELECT
        bo.buyer_id,
        bf.is_wpn_buyer,
        COUNT(DISTINCT bo.order_id) AS total_orders,
        SUM(bo.total_usd) AS total_spend,
        DATEDIFF('day',
                 MIN(bo.order_date),
                 MAX(bo.order_date)) AS days_between_first_last
    FROM buyer_orders bo
    JOIN buyer_flags bf ON bo.buyer_id = bf.buyer_id
    GROUP BY 1, 2
),

-- Identify repeat buyers (made another purchase within 90 days)
repeat_behavior AS (
    SELECT
        f.buyer_id,
        MIN(o.order_date) AS first_order_date,
        CASE
            WHEN COUNT(DISTINCT CASE
                                WHEN o.order_date > f.first_order_date
                                     AND DATEDIFF('day', f.first_order_date, o.order_date) <= 90
                                THEN o.order_id END) > 0
            THEN 1 ELSE 0 END AS repeated_within_90d
    FROM first_orders f
    JOIN orders o ON f.buyer_id = o.buyer_id
    GROUP BY 1,2
)

-- Final summary by cohort
SELECT
    m.is_wpn_buyer,
    COUNT(DISTINCT m.buyer_id) AS buyers,
    ROUND(AVG(r.repeated_within_90d), 3) AS repeat_purchase_rate_90d,
    ROUND(AVG(m.total_orders), 2) AS avg_orders,
    ROUND(AVG(m.total_spend), 2) AS avg_spend,
    ROUND(AVG(m.days_between_first_last), 1) AS avg_days_between_orders
FROM buyer_metrics m
JOIN repeat_behavior r USING (buyer_id)
GROUP BY 1
ORDER BY is_wpn_buyer DESC;

