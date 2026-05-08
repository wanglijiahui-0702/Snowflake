----------------------------------------------------------------------------------------------------------------------
-- STEP 1 – ORDER FEATURES
----------------------------------------------------------------------------------------------------------------------
with order_items_base as (
    select
        id as order_item_id,
        order_id,
        seller_order_id,
        lower(trim(coalesce(product_line, ''))) || '|' || lower(trim(coalesce(product_name, ''))) as product_key,
        product_name,
        product_line,
        product_type,
        publisher_name,
        set_name,
        super_condition,
        is_direct_order,
        is_foil,
        is_sealed,
        ordered_at_et,
        updated_at_et,
        quantity,
        unit_price_usd,
        total_usd,
        total_weight_oz,
        unit_weight_oz
    from analytics.core.order_items
    -- 3 full calendar years = 12 quarters
    where ordered_at_et >= '2025-01-01'
      and ordered_at_et <  '2026-01-01'
      and product_name  is not null
      and product_line  is not null
),

manual_kickbacks as (
    -- Point-in-time promotions — flagged at order level, then max()-rolled up per quarter
    -- Add historical kickbacks here as needed
    select
        'it''s mayhem! get 10% store credit on all products'      as kickback_name,
        1  as is_all_products_flag,
        0  as is_other_flag,
        to_timestamp_ntz('2025-05-16 13:00:00')                   as start_ts,
        to_timestamp_ntz('2025-05-17 03:00:00')                   as end_ts,
        10.00 as promo_value_pct,
        50.00 as promo_threshold_usd
    union all
    select
        'cyber weekend is here! get 10% store credit on all products',
        1, 0,
        to_timestamp_ntz('2025-11-28 14:00:00'),
        to_timestamp_ntz('2025-12-02 04:00:00'),
        10.00, 50.00
    -- Add 2023 / 2024 kickbacks here:
    -- union all select '...', 1, 0, to_timestamp_ntz('2024-...'), to_timestamp_ntz('2024-...'), X, Y
),

order_items_kickback_labeled as (
    select
        o.order_item_id,
        o.order_id,
        o.seller_order_id,
        o.product_key,
        o.product_name,
        o.product_line,
        o.product_type,
        o.publisher_name,
        o.set_name,
        o.super_condition,
        case when o.is_direct_order then 1 else 0 end as is_direct_order_flag,
        case when o.is_foil         then 1 else 0 end as is_foil_flag,
        case when o.is_sealed       then 1 else 0 end as is_sealed_flag,

        -- ── quarterly grain ──────────────────────────────────────────────────
        date_trunc('quarter', o.ordered_at_et)       as quarter_start_date,
        cast(o.ordered_at_et as date)                as order_date,
        o.ordered_at_et,
        o.quantity        as units_sold,
        o.total_usd       as revenue_usd,
        o.unit_price_usd,
        o.total_weight_oz,
        o.unit_weight_oz,

        case when k.kickback_name is not null then 1 else 0 end as is_kickback_promo,
        k.kickback_name,
        k.is_all_products_flag,
        k.is_other_flag,
        k.start_ts        as kickback_start_ts,
        k.end_ts          as kickback_end_ts,
        k.promo_value_pct,
        k.promo_threshold_usd,
        datediff('day', cast(k.start_ts as date), cast(o.ordered_at_et as date)) as days_from_kickback_start,
        datediff('day', cast(o.ordered_at_et as date), cast(k.end_ts as date))   as days_to_kickback_end,
        case
            when k.kickback_name is not null
             and datediff('day', cast(k.start_ts as date), cast(o.ordered_at_et as date)) < 0
            then 1 else 0
        end as is_pre_kickback,
        case
            when k.kickback_name is not null
             and datediff('day', cast(o.ordered_at_et as date), cast(k.end_ts as date)) < 0
            then 1 else 0
        end as is_post_kickback
    from order_items_base o
    left join manual_kickbacks k
        on o.ordered_at_et >= k.start_ts
       and o.ordered_at_et <  k.end_ts
),

quarterly_order_features as (
    select
        product_key,
        max(product_name)    as product_name,
        max(product_line)    as product_line,
        max(product_type)    as product_type,
        max(publisher_name)  as publisher_name,
        max(set_name)        as set_name,
        max(super_condition) as super_condition,
        quarter_start_date,

        -- ── calendar helpers ─────────────────────────────────────────────────
        -- Useful standalone features for seasonality modelling
        year(quarter_start_date)                                        as calendar_year,
        quarter(quarter_start_date)                                     as quarter_number,   -- 1/2/3/4
        year(quarter_start_date)::varchar || '-Q'
            || quarter(quarter_start_date)::varchar                     as year_quarter,     -- e.g. '2024-Q3'

        -- ── demand metrics ───────────────────────────────────────────────────
        count(*)                                                        as order_item_count,
        count(distinct order_id)                                        as order_count,
        sum(units_sold)                                                 as quarterly_units_sold,
        sum(revenue_usd)                                                as quarterly_revenue_usd,
        avg(unit_price_usd)                                             as avg_unit_price_usd,
        avg(total_weight_oz)                                            as avg_total_weight_oz,

        sum(is_direct_order_flag)                                       as direct_order_item_count,
        sum(is_foil_flag)                                               as foil_item_count,
        sum(is_sealed_flag)                                             as sealed_item_count,

        count(distinct case when units_sold > 0 then order_id end)     as active_orders,

        -- ── kickback / promotion metrics ─────────────────────────────────────
        max(is_kickback_promo)                                          as is_kickback_promo,
        count(distinct kickback_name)                                   as num_active_kickbacks,
        sum(case when is_kickback_promo = 1 then units_sold  else 0 end) as promo_units_sold,
        sum(case when is_kickback_promo = 1 then revenue_usd else 0 end) as promo_revenue_usd,
        count(distinct case when is_kickback_promo = 1 then order_id end) as promo_order_count,

        avg(case when is_kickback_promo = 1 then promo_value_pct     end) as avg_promo_value_pct,
        max(case when is_kickback_promo = 1 then promo_value_pct     end) as max_promo_value_pct,
        avg(case when is_kickback_promo = 1 then promo_threshold_usd end) as avg_promo_threshold_usd,

        avg(case when is_kickback_promo = 1 then days_from_kickback_start end) as avg_days_from_kickback_start,
        avg(case when is_kickback_promo = 1 then days_to_kickback_end     end) as avg_days_to_kickback_end,

        sum(is_pre_kickback)  as pre_kickback_order_items,
        sum(is_post_kickback) as post_kickback_order_items
    from order_items_kickback_labeled
    group by
        product_key,
        quarter_start_date
),

order_rolling_features as (
    select
        *,
        -- ── quarterly demand lags ─────────────────────────────────────────────
        -- lag_1q = last quarter (most recent prior demand signal)
        lag(quarterly_units_sold, 1) over (partition by product_key order by quarter_start_date) as units_lag_1q,
        -- lag_2q = two quarters ago
        lag(quarterly_units_sold, 2) over (partition by product_key order by quarter_start_date) as units_lag_2q,
        -- lag_3q = three quarters ago
        lag(quarterly_units_sold, 3) over (partition by product_key order by quarter_start_date) as units_lag_3q,
        -- lag_4q = same quarter last year (year-over-year seasonal baseline)
        lag(quarterly_units_sold, 4) over (partition by product_key order by quarter_start_date) as units_lag_4q,

        -- ── rolling demand averages (1-quarter lag prevents leakage) ──────────
        -- 2-quarter (half-year) rolling avg — short-term trend
        avg(quarterly_units_sold) over (
            partition by product_key order by quarter_start_date
            rows between 2 preceding and 1 preceding
        ) as units_roll_mean_2q,

        -- 4-quarter (full-year) rolling avg — captures annual seasonality
        avg(quarterly_units_sold) over (
            partition by product_key order by quarter_start_date
            rows between 4 preceding and 1 preceding
        ) as units_roll_mean_4q,

        -- ── demand volatility ─────────────────────────────────────────────────
        -- 2-quarter rolling stddev — how erratic is this product quarter-to-quarter
        stddev_samp(quarterly_units_sold) over (
            partition by product_key order by quarter_start_date
            rows between 2 preceding and 1 preceding
        ) as units_roll_std_2q,

        -- ── year-over-year growth rate ────────────────────────────────────────
        -- Percentage change vs. same quarter last year
        -- Requires lag_4q > 0 to avoid division by zero
        case
            when lag(quarterly_units_sold, 4) over (partition by product_key order by quarter_start_date) > 0
            then (
                quarterly_units_sold
                - lag(quarterly_units_sold, 4) over (partition by product_key order by quarter_start_date)
            ) / lag(quarterly_units_sold, 4) over (partition by product_key order by quarter_start_date)
            else null
        end as yoy_units_growth_pct

    from quarterly_order_features
),

----------------------------------------------------------------------------------------------------------------------
-- STEP 2 – PRICE DYNAMICS
--
-- Snowflake note: MEDIAN() is not supported as a sliding ROWS BETWEEN window function.
-- Rolling 8-quarter (2-year) median is computed via a self-join + PERCENTILE_CONT
-- GROUP BY aggregate — the only reliable rolling-percentile pattern in Snowflake.
----------------------------------------------------------------------------------------------------------------------

-- 2a: quarter-over-quarter price change + 2-quarter rolling volatility
price_lag_vol as (
    select
        product_key,
        quarter_start_date,
        avg_unit_price_usd,

        -- quarter-over-quarter % change in avg transaction price
        -- Sustained rise → supply tightening or demand spike ahead
        -- Sustained fall → oversupply or buyer demand cooling
        case
            when lag(avg_unit_price_usd, 1) over (partition by product_key order by quarter_start_date) > 0
            then (avg_unit_price_usd
                  - lag(avg_unit_price_usd, 1) over (partition by product_key order by quarter_start_date))
                 / lag(avg_unit_price_usd, 1) over (partition by product_key order by quarter_start_date)
            else null
        end as price_qoq_pct,

        -- year-over-year % change in avg transaction price (same-quarter comparison)
        case
            when lag(avg_unit_price_usd, 4) over (partition by product_key order by quarter_start_date) > 0
            then (avg_unit_price_usd
                  - lag(avg_unit_price_usd, 4) over (partition by product_key order by quarter_start_date))
                 / lag(avg_unit_price_usd, 4) over (partition by product_key order by quarter_start_date)
            else null
        end as price_yoy_pct,

        -- 2-quarter rolling price volatility (stddev over prior 2 quarters)
        stddev_samp(avg_unit_price_usd) over (
            partition by product_key order by quarter_start_date
            rows between 2 preceding and 1 preceding
        ) as price_roll_vol_2q

    from quarterly_order_features
),

-- 2b: rolling 8-quarter (2-year) median — Snowflake-safe self-join pattern
--     We join each quarter to its prior 8 quarters and apply PERCENTILE_CONT
--     as a GROUP BY aggregate (not a window function, which Snowflake blocks).
price_rolling_median as (
    select
        cur.product_key,
        cur.quarter_start_date,
        percentile_cont(0.5) within group (order by hist.avg_unit_price_usd)
            as rolling_8q_median_price
    from quarterly_order_features cur
    join quarterly_order_features hist
        on  hist.product_key         = cur.product_key
        -- strictly prior quarters only (1-quarter lag prevents leakage)
        and hist.quarter_start_date  <  cur.quarter_start_date
        -- look back 8 quarters = 2 full years
        and hist.quarter_start_date  >= dateadd('month', -24, cur.quarter_start_date)
    group by
        cur.product_key,
        cur.quarter_start_date
),

-- 2c: combined price dynamics CTE
price_dynamics as (
    select
        lv.product_key,
        lv.quarter_start_date,
        lv.avg_unit_price_usd,
        lv.price_qoq_pct,          -- quarter-over-quarter % change
        lv.price_yoy_pct,          -- year-over-year % change (same quarter last year)
        lv.price_roll_vol_2q,      -- 2-quarter rolling price volatility

        -- current price vs. rolling 8-quarter (2-year) median
        -- > 1.0 → price above 2-year trend (potentially stretched)
        -- < 1.0 → price below 2-year trend (potential buying opportunity)
        case
            when rm.rolling_8q_median_price > 0
            then lv.avg_unit_price_usd / rm.rolling_8q_median_price
            else null
        end as price_vs_8q_median

    from price_lag_vol lv
    left join price_rolling_median rm
        on  lv.product_key         = rm.product_key
        and lv.quarter_start_date  = rm.quarter_start_date
),

----------------------------------------------------------------------------------------------------------------------
-- STEP 3 – SUPPLY / LISTING DEPTH  (analytics.core.daily_seller_inventory)
--
-- Quarterly change: we take the last calendar day of each quarter as the
-- supply snapshot — the most recent inventory reading before the quarter closes.
-- This avoids averaging intra-quarter fluctuations and gives a clean EOQ position.
----------------------------------------------------------------------------------------------------------------------

-- 3a: pull raw daily inventory; assign quarter bucket
daily_inventory_base as (
    select
        lower(trim(coalesce(product_line, ''))) || '|' || lower(trim(coalesce(product_name, ''))) as product_key,
        date_et,
        -- quarter bucket = first day of the calendar quarter
        date_trunc('quarter', date_et)  as quarter_start_date,
        seller_id,
        quantity,
        reserve_quantity,
        price_usd,
        is_seller_live,
        is_foil,
        is_sealed,
        super_condition
    from analytics.core.daily_seller_inventory
    where date_et >= '2025-01-01'
      and date_et <  '2026-01-01'
      and product_name  is not null
      and product_line  is not null
      and is_seller_live = true
      and quantity > 0           -- only genuinely available stock
),

-- 3b: keep only the last snapshot day per seller per quarter (end-of-quarter position)
inventory_quarterly_snapshot as (
    select
        product_key,
        quarter_start_date,
        date_et,
        seller_id,
        quantity,
        reserve_quantity,
        price_usd,
        super_condition,
        row_number() over (
            partition by product_key, seller_id, quarter_start_date
            order by date_et desc    -- most recent day of the quarter wins
        ) as rn
    from daily_inventory_base
),

inventory_deduped as (
    select *
    from inventory_quarterly_snapshot
    where rn = 1
),

-- 3c: aggregate to product × quarter level
quarterly_supply_features as (
    select
        product_key,
        quarter_start_date,

        -- total units available (end-of-quarter snapshot across all live sellers)
        sum(quantity)                                                as total_units_available,

        -- total reserve (hidden) stock held by sellers
        -- When sellers release reserves, prices often drop — leading indicator
        sum(reserve_quantity)                                        as reserve_units_total,

        -- number of distinct active sellers with stock at quarter end
        count(distinct seller_id)                                    as active_seller_count,

        -- average stock per active seller
        case
            when count(distinct seller_id) > 0
            then sum(quantity)::float / count(distinct seller_id)
            else null
        end                                                          as avg_qty_per_seller,

        -- floor price = cheapest listing at end of quarter
        min(price_usd)                                               as floor_price_usd,

        -- listing-level price percentiles (end-of-quarter snapshot)
        -- PERCENTILE_CONT(0.5) used — MEDIAN() is a GROUP BY aggregate in Snowflake
        -- but cannot be referenced in a CASE WHEN in the same SELECT, so we repeat
        -- the expression in price_dispersion_pct below.
        percentile_cont(0.50) within group (order by price_usd)      as median_listing_price,
        percentile_cont(0.10) within group (order by price_usd)      as p10_listing_price,
        percentile_cont(0.90) within group (order by price_usd)      as p90_listing_price,

        -- price dispersion = (p90 - p10) / p50
        -- High → wide spread between cheap and expensive listings (fragmented market)
        -- Low  → commoditized product; buyers will concentrate on the cheapest seller
        case
            when percentile_cont(0.50) within group (order by price_usd) > 0
            then (
                    percentile_cont(0.90) within group (order by price_usd)
                  - percentile_cont(0.10) within group (order by price_usd)
                 )
                 / percentile_cont(0.50) within group (order by price_usd)
            else null
        end                                                          as price_dispersion_pct,

        -- supply concentration: top seller's share of total available units
        -- → 1.0 = single-seller monopoly (fragile supply)
        -- → 0.0 = many sellers evenly distributed (stable supply)
        max(quantity)::float / nullif(sum(quantity), 0)              as supply_concentration

    from inventory_deduped
    group by
        product_key,
        quarter_start_date
),

----------------------------------------------------------------------------------------------------------------------
-- STEP 4 – PDP / PRODUCT DETAIL PAGE VIEW FEATURES
----------------------------------------------------------------------------------------------------------------------
pdp_base as (
    select
        id as event_id,
        lower(trim(coalesce(product_line, ''))) || '|' || lower(trim(coalesce(product_name, ''))) as product_key,
        product_name,
        product_line,
        set_name,
        merch_category,
        merch_type,
        is_lightly_played_available,
        is_moderately_played_available,
        is_near_mint_available,
        is_sealed,
        is_singles,
        new_pd_group,
        timestamp,
        seller_key,
        seller_name,
        seller_spotlight_quantity,
        total_listings,
        total_sellers,
        user_agent,
        user_id,
        anonymous_id,
        application,
        pdp_listing_sort,
        uuid_ts,
        coalesce(nullif(user_id, ''), anonymous_id)   as viewer_key,
        -- ── quarterly grain ──────────────────────────────────────────────────
        date_trunc('quarter', timestamp)              as quarter_start_date
    from segment.marketplace_prd.product_details_viewed
    where timestamp >= '2025-01-01'
      and timestamp <  '2026-01-01'
      and product_name is not null
      and product_line is not null
),

pdp_enriched as (
    select
        *,
        case
            when lower(coalesce(user_agent, '')) like '%iphone%'
              or lower(coalesce(user_agent, '')) like '%android%'
              or lower(coalesce(user_agent, '')) like '%mobile%'  then 'mobile'
            when lower(coalesce(user_agent, '')) like '%ipad%'
              or lower(coalesce(user_agent, '')) like '%tablet%'  then 'tablet'
            when coalesce(user_agent, '') <> ''                   then 'desktop'
            else 'unknown'
        end as device_type
    from pdp_base
),

quarterly_pdp_features as (
    select
        product_key,
        max(product_name)   as product_name,
        max(product_line)   as product_line,
        max(set_name)       as set_name,
        max(merch_category) as merch_category,
        max(merch_type)     as merch_type,
        quarter_start_date,

        count(*)                                                    as pdp_views,
        count(distinct viewer_key)                                  as unique_viewers,
        count(distinct user_id)                                     as unique_user_ids,
        count(distinct anonymous_id)                                as unique_anonymous_ids,
        count(distinct uuid_ts)                                     as event_count,
        count(distinct seller_key)                                  as unique_sellers_viewed,
        count(distinct application)                                 as unique_applications,
        count(distinct pdp_listing_sort)                            as distinct_listing_sorts,

        sum(case when new_pd_group               then 1 else 0 end) as new_pd_group_views,
        avg(case when new_pd_group               then 1 else 0 end) as new_pd_group_rate,

        sum(case when is_lightly_played_available   then 1 else 0 end) as lightly_played_available_views,
        sum(case when is_moderately_played_available then 1 else 0 end) as moderately_played_available_views,
        sum(case when is_near_mint_available         then 1 else 0 end) as near_mint_available_views,
        sum(case when is_sealed                      then 1 else 0 end) as sealed_views,
        sum(case when is_singles                     then 1 else 0 end) as singles_views,

        avg(total_listings)                                         as avg_total_listings,
        max(total_listings)                                         as max_total_listings,
        avg(total_sellers)                                          as avg_total_sellers,
        max(total_sellers)                                          as max_total_sellers,
        avg(seller_spotlight_quantity)                              as avg_seller_spotlight_quantity,
        max(seller_spotlight_quantity)                              as max_seller_spotlight_quantity,

        sum(case when device_type = 'mobile'  then 1 else 0 end)   as mobile_views,
        sum(case when device_type = 'desktop' then 1 else 0 end)   as desktop_views,
        sum(case when device_type = 'tablet'  then 1 else 0 end)   as tablet_views,
        sum(case when device_type = 'unknown' then 1 else 0 end)   as unknown_device_views,

        count(distinct case when user_id      is not null then user_id      end) as identified_users,
        count(distinct case when anonymous_id is not null then anonymous_id end) as anonymous_users
    from pdp_enriched
    group by product_key, quarter_start_date
),

pdp_features as (
    select
        *,
        case when unique_viewers > 0 then pdp_views / unique_viewers       else 0 end as views_per_viewer,
        case when pdp_views > 0 then new_pd_group_views / pdp_views        else 0 end as new_pd_group_view_share,
        case when pdp_views > 0 then sealed_views  / pdp_views             else 0 end as sealed_view_share,
        case when pdp_views > 0 then singles_views / pdp_views             else 0 end as singles_view_share,
        case when pdp_views > 0 then mobile_views  / pdp_views             else 0 end as mobile_view_share,
        case when pdp_views > 0 then desktop_views / pdp_views             else 0 end as desktop_view_share,
        case when pdp_views > 0 then tablet_views  / pdp_views             else 0 end as tablet_view_share
    from quarterly_pdp_features
),

----------------------------------------------------------------------------------------------------------------------
-- STEP 5 – ADD-TO-CART INTENT FEATURES
----------------------------------------------------------------------------------------------------------------------
unified_add_to_cart_events as (
    select
        lower(trim(coalesce(product_line, ''))) || '|' || lower(trim(coalesce(product_name, ''))) as product_key,
        timestamp as event_timestamp,
        coalesce(nullif(user_id, ''), anonymous_id) as user_key,
        coalesce(items_added, 1) as items_added,
        'saved_for_later' as source
    from segment.marketplace_prd.saved_for_later_added_to_cart
    where timestamp >= '2025-01-01' and timestamp < '2026-01-01'
      and product_name is not null and product_line is not null

    union all

    select
        lower(trim(coalesce(product_line, ''))) || '|' || lower(trim(coalesce(product_name, ''))) as product_key,
        timestamp as event_timestamp,
        coalesce(nullif(user_id, ''), anonymous_id) as user_key,
        coalesce(items_added, 1) as items_added,
        'marketplace_search' as source
    from segment.marketplace_prd.marketplace_search_added_to_cart
    where timestamp >= '2025-01-01' and timestamp < '2026-01-01'
      and product_name is not null and product_line is not null

    union all

    select
        lower(trim(coalesce(product_line, ''))) || '|' || lower(trim(coalesce(product_name, ''))) as product_key,
        timestamp as event_timestamp,
        coalesce(nullif(user_id, ''), anonymous_id) as user_key,
        1 as items_added,
        'storefront_listing' as source
    from segment.marketplace_prd.storefront_listing_added_to_cart
    where timestamp >= '2025-01-01' and timestamp < '2026-01-01'
      and product_name is not null and product_line is not null
),

filtered_atc_events as (
    select * from unified_add_to_cart_events
    where product_key is not null and product_key <> '|'
),

quarterly_atc_features as (
    select
        product_key,
        -- ── quarterly grain ──────────────────────────────────────────────────
        date_trunc('quarter', event_timestamp)                            as quarter_start_date,
        count(*)                                                          as atc_events,
        sum(items_added)                                                  as items_added_total,
        count(distinct user_key)                                          as atc_users,
        sum(case when source = 'saved_for_later'    then 1 else 0 end)   as saved_for_later_events,
        sum(case when source = 'marketplace_search' then 1 else 0 end)   as marketplace_search_atc_events,
        sum(case when source = 'mass_entry'         then 1 else 0 end)   as mass_entry_atc_events,
        sum(case when source = 'product'            then 1 else 0 end)   as product_atc_events,
        sum(case when source = 'storefront_listing' then 1 else 0 end)   as storefront_listing_atc_events
    from filtered_atc_events
    group by 1, 2
),

atc_features as (
    select
        *,
        case when atc_events > 0 then items_added_total / atc_events                 else 0 end as items_per_event,
        case when atc_users  > 0 then atc_events        / atc_users                  else 0 end as events_per_user,
        case when atc_events > 0 then saved_for_later_events / atc_events            else 0 end as saved_for_later_share,
        case when atc_events > 0 then marketplace_search_atc_events / atc_events     else 0 end as marketplace_search_share,
        case when atc_events > 0 then mass_entry_atc_events         / atc_events     else 0 end as mass_entry_share,
        case when atc_events > 0 then product_atc_events            / atc_events     else 0 end as product_share,
        case when atc_events > 0 then storefront_listing_atc_events / atc_events     else 0 end as storefront_listing_share
    from quarterly_atc_features
),

----------------------------------------------------------------------------------------------------------------------
-- STEP 6 – FINAL JOIN  (product × quarter grain)
----------------------------------------------------------------------------------------------------------------------
final_model_table as (
    select
        -- ── identifiers ──────────────────────────────────────────────────────
        coalesce(o.product_key,          p.product_key,          a.product_key)          as product_key,
        coalesce(o.quarter_start_date,   p.quarter_start_date,   a.quarter_start_date)   as quarter_start_date,

        o.product_name       as order_product_name,
        o.product_line       as order_product_line,
        o.product_type,
        o.publisher_name,
        o.set_name,
        o.super_condition,

        -- ── calendar helpers ─────────────────────────────────────────────────
        o.calendar_year,
        o.quarter_number,     -- 1 / 2 / 3 / 4
        o.year_quarter,       -- e.g. '2024-Q3'

        -- ── demand / order features ──────────────────────────────────────────
        o.order_item_count,
        o.order_count,
        o.quarterly_units_sold,
        o.quarterly_revenue_usd,
        o.avg_unit_price_usd,
        o.avg_total_weight_oz,
        o.direct_order_item_count,
        o.foil_item_count,
        o.sealed_item_count,
        o.active_orders,

        -- ── promotion / kickback features ───────────────────────────────────
        o.is_kickback_promo,
        o.num_active_kickbacks,
        o.promo_units_sold,
        o.promo_revenue_usd,
        o.promo_order_count,
        o.avg_promo_value_pct,
        o.max_promo_value_pct,
        o.avg_promo_threshold_usd,
        o.avg_days_from_kickback_start,
        o.avg_days_to_kickback_end,
        o.pre_kickback_order_items,
        o.post_kickback_order_items,

        -- ── demand lags & rolling features ──────────────────────────────────
        o.units_lag_1q,             -- last quarter
        o.units_lag_2q,             -- two quarters ago
        o.units_lag_3q,             -- three quarters ago
        o.units_lag_4q,             -- same quarter last year (YoY baseline)
        o.units_roll_mean_2q,       -- 2-quarter (half-year) rolling avg
        o.units_roll_mean_4q,       -- 4-quarter (full-year) rolling avg
        o.units_roll_std_2q,        -- 2-quarter demand volatility
        o.yoy_units_growth_pct,     -- % growth vs. same quarter last year

        -- ── price dynamics (transaction prices) ─────────────────────────────
        pd.price_qoq_pct,           -- quarter-over-quarter % Δ in avg transaction price
        pd.price_yoy_pct,           -- year-over-year % Δ in avg transaction price
        pd.price_roll_vol_2q,       -- 2-quarter rolling price volatility (stddev)
        pd.price_vs_8q_median,      -- current price / 2-year rolling median

        -- ── listing price features (from daily_seller_inventory) ─────────────
        s.floor_price_usd,          -- cheapest active listing at quarter end
        s.median_listing_price,     -- middle of the market (end-of-quarter snapshot)
        s.p10_listing_price,        -- cheapest decile of listings
        s.p90_listing_price,        -- most expensive decile of listings
        s.price_dispersion_pct,     -- (p90 - p10) / p50 — market fragmentation

        -- floor price vs. avg transaction price gap
        -- Negative → buyers paying above cheapest listing (demand urgency)
        -- Positive → buyers paying below floor (stale listings / settlement lag)
        case
            when s.floor_price_usd > 0
            then (o.avg_unit_price_usd - s.floor_price_usd) / s.floor_price_usd
            else null
        end                                                       as price_vs_floor_pct,

        -- ── supply / inventory depth features ───────────────────────────────
        s.total_units_available,    -- total stock across all live sellers (EOQ)
        s.reserve_units_total,      -- hidden reserve stock at quarter end
        s.active_seller_count,      -- live sellers with inventory > 0
        s.avg_qty_per_seller,       -- average units per seller
        s.supply_concentration,     -- top seller's share of total units

        -- days_of_supply = total_units_available / avg daily run-rate
        -- avg daily run-rate = units_roll_mean_2q / 91 days (approximate quarter length)
        -- < 30  → less than 1 month of stock remaining → scarcity
        -- > 365 → over a year of supply → significant oversupply / price pressure
        case
            when o.units_roll_mean_2q > 0
            then s.total_units_available::float / (o.units_roll_mean_2q / 91.0)
            else null
        end                                                       as days_of_supply,

        -- supply cover in quarters (how many quarters of demand the current stock covers)
        case
            when o.units_roll_mean_2q > 0
            then s.total_units_available::float / o.units_roll_mean_2q
            else null
        end                                                       as supply_cover_quarters,

        -- ── PDP / product view features ──────────────────────────────────────
        p.product_name               as view_product_name,
        p.product_line               as view_product_line,
        p.set_name                   as view_set_name,
        p.merch_category,
        p.merch_type,
        p.pdp_views,
        p.unique_viewers,
        p.unique_user_ids,
        p.unique_anonymous_ids,
        p.event_count                as pdp_event_count,
        p.unique_sellers_viewed,
        p.unique_applications,
        p.distinct_listing_sorts,
        p.new_pd_group_views,
        p.new_pd_group_rate,
        p.lightly_played_available_views,
        p.moderately_played_available_views,
        p.near_mint_available_views,
        p.sealed_views               as pdp_sealed_views,
        p.singles_views              as pdp_singles_views,
        p.avg_total_listings,
        p.max_total_listings,
        p.avg_total_sellers,
        p.max_total_sellers,
        p.avg_seller_spotlight_quantity,
        p.max_seller_spotlight_quantity,
        p.mobile_views,
        p.desktop_views,
        p.tablet_views,
        p.unknown_device_views,
        p.identified_users,
        p.anonymous_users,
        p.views_per_viewer,
        p.new_pd_group_view_share,
        p.sealed_view_share          as pdp_sealed_view_share,
        p.singles_view_share         as pdp_singles_view_share,
        p.mobile_view_share,
        p.desktop_view_share,
        p.tablet_view_share,

        -- ── add-to-cart intent features ───────────────────────────────────────
        a.atc_events,
        a.items_added_total,
        a.atc_users,
        a.saved_for_later_events,
        a.marketplace_search_atc_events,
        a.mass_entry_atc_events,
        a.product_atc_events,
        a.storefront_listing_atc_events,
        a.items_per_event            as atc_items_per_event,
        a.events_per_user            as atc_events_per_user,
        a.saved_for_later_share,
        a.marketplace_search_share,
        a.mass_entry_share,
        a.product_share,
        a.storefront_listing_share

    from order_rolling_features o

    left join price_dynamics pd
        on  o.product_key          = pd.product_key
        and o.quarter_start_date   = pd.quarter_start_date

    left join quarterly_supply_features s
        on  o.product_key          = s.product_key
        and o.quarter_start_date   = s.quarter_start_date

    full outer join pdp_features p
        on  o.product_key          = p.product_key
        and o.quarter_start_date   = p.quarter_start_date

    full outer join atc_features a
        on  coalesce(o.product_key,        p.product_key)        = a.product_key
        and coalesce(o.quarter_start_date, p.quarter_start_date) = a.quarter_start_date
)

select *
from final_model_table
order by product_key, quarter_start_date;