select * from ANALYTICS.CORE.ORDER_ITEMS
select * from SEGMENT.MARKETPLACE_PRD.PRODUCT_DETAILS_VIEWED

select * from SEGMENT.MARKETPLACE_PRD.SAVED_FOR_LATER_ADDED_TO_CART
select * from SEGMENT.MARKETPLACE_PRD.MARKETPLACE_SEARCH_ADDED_TO_CART
select * from SEGMENT.MARKETPLACE_PRD.MASS_ENTRY_ADDED_TO_CART
select * from SEGMENT.MARKETPLACE_PRD.PRODUCT_ADDED_TO_CART
select * from SEGMENT.MARKETPLACE_PRD.STOREFRONT_LISTING_ADDED_TO_CART

select * from SEGMENT.MARKETPLACE_PRD.PRODUCT_ADDED_TO_WISHLIST
select * from SEGMENT.MARKETPLACE_PRD.STOREFRONT_LISTING_ADDED_TO_CARTSEGMENT.MARKETPLACE_PRD.USERS

select count (distinct id) from ANALYTICS.CORE.SELLERS where is_direct = true

with order_items_base as (
    select
        id as order_item_id,
        order_id,
        seller_order_id,
        product_id,
        product_name,
        product_number,
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
    where ordered_at_et >= '2025-01-01'
      and ordered_at_et < '2026-01-01'
),

order_items_enriched as (
    select
        order_item_id,
        order_id,
        seller_order_id,
        product_id,
        product_name,
        product_number,
        product_line,
        product_type,
        publisher_name,
        set_name,
        super_condition,
        case when is_direct_order then 1 else 0 end as is_direct_order_flag,
        case when is_foil then 1 else 0 end as is_foil_flag,
        case when is_sealed then 1 else 0 end as is_sealed_flag,
        date_trunc('week', ordered_at_et) as week_start_date,
        cast(ordered_at_et as date) as order_date,
        quantity as units_sold,
        total_usd as revenue_usd,
        unit_price_usd as unit_price_usd,
        total_weight_oz as total_weight_oz,
        unit_weight_oz as unit_weight_oz
    from order_items_base
),

weekly_product_features as (
    select
        product_id,
        product_name,
        product_number,
        product_line,
        product_type,
        publisher_name,
        set_name,
        super_condition,
        week_start_date,

        count(*) as order_item_count,
        count(distinct order_id) as order_count,
        sum(units_sold) as weekly_units_sold,
        sum(revenue_usd) as weekly_revenue_usd,
        avg(unit_price_usd) as avg_unit_price_usd,
        avg(total_weight_oz) as avg_total_weight_oz,

        sum(is_direct_order_flag) as direct_order_item_count,
        sum(is_foil_flag) as foil_item_count,
        sum(is_sealed_flag) as sealed_item_count,

        count(distinct case when units_sold > 0 then order_id end) as active_orders
    from order_items_enriched
    group by
        product_id,
        product_name,
        product_number,
        product_line,
        product_type,
        publisher_name,
        set_name,
        super_condition,
        week_start_date
),

rolling_features as (
    select
        *,
        lag(weekly_units_sold, 1) over (partition by product_id order by week_start_date) as units_lag_1w,
        lag(weekly_units_sold, 2) over (partition by product_id order by week_start_date) as units_lag_2w,
        lag(weekly_units_sold, 4) over (partition by product_id order by week_start_date) as units_lag_4w,
        avg(weekly_units_sold) over (
            partition by product_id
            order by week_start_date
            rows between 4 preceding and 1 preceding
        ) as units_roll_mean_4w,
        avg(weekly_units_sold) over (
            partition by product_id
            order by week_start_date
            rows between 8 preceding and 1 preceding
        ) as units_roll_mean_8w,
        stddev_samp(weekly_units_sold) over (
            partition by product_id
            order by week_start_date
            rows between 4 preceding and 1 preceding
        ) as units_roll_std_4w
    from weekly_product_features
)

select *
from rolling_features;

--- orders
with order_items_base as (
    select
        id as order_item_id,
        order_id,
        seller_order_id,
        product_id,
        product_name,
        product_number,
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
    where ordered_at_et >= '2025-01-01'
      and ordered_at_et < '2026-01-01'
),

manual_kickbacks as (
    select
        'it''s mayhem! get 10% store credit on all products' as kickback_name,
        1 as is_all_products_flag,
        0 as is_other_flag,
        to_timestamp_ntz('2025-05-16 13:00:00') as start_ts,
        to_timestamp_ntz('2025-05-17 03:00:00') as end_ts,
        10.00 as promo_value_pct,
        50.00 as promo_threshold_usd
    union all
    select
        'cyber weekend is here! get 10% store credit on all products' as kickback_name,
        1 as is_all_products_flag,
        0 as is_other_flag,
        to_timestamp_ntz('2025-11-28 14:00:00') as start_ts,
        to_timestamp_ntz('2025-12-02 04:00:00') as end_ts,
        10.00 as promo_value_pct,
        50.00 as promo_threshold_usd
),

order_items_kickback_labeled as (
    select
        o.order_item_id,
        o.order_id,
        o.seller_order_id,
        o.product_id,
        o.product_name,
        o.product_number,
        o.product_line,
        o.product_type,
        o.publisher_name,
        o.set_name,
        o.super_condition,
        case when o.is_direct_order then 1 else 0 end as is_direct_order_flag,
        case when o.is_foil then 1 else 0 end as is_foil_flag,
        case when o.is_sealed then 1 else 0 end as is_sealed_flag,
        date_trunc('week', o.ordered_at_et) as week_start_date,
        cast(o.ordered_at_et as date) as order_date,
        o.ordered_at_et,
        o.quantity as units_sold,
        o.total_usd as revenue_usd,
        o.unit_price_usd,
        o.total_weight_oz,
        o.unit_weight_oz,

        case when k.kickback_name is not null then 1 else 0 end as is_kickback_promo,
        k.kickback_name,
        k.is_all_products_flag,
        k.is_other_flag,
        k.start_ts as kickback_start_ts,
        k.end_ts as kickback_end_ts,
        k.promo_value_pct,
        k.promo_threshold_usd,
        datediff('day', cast(k.start_ts as date), cast(o.ordered_at_et as date)) as days_from_kickback_start,
        datediff('day', cast(o.ordered_at_et as date), cast(k.end_ts as date)) as days_to_kickback_end,
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
       and o.ordered_at_et < k.end_ts
),

weekly_product_features as (
    select
        product_id,
        product_name,
        product_number,
        product_line,
        product_type,
        publisher_name,
        set_name,
        super_condition,
        week_start_date,

        count(*) as order_item_count,
        count(distinct order_id) as order_count,
        sum(units_sold) as weekly_units_sold,
        sum(revenue_usd) as weekly_revenue_usd,
        avg(unit_price_usd) as avg_unit_price_usd,
        avg(total_weight_oz) as avg_total_weight_oz,

        sum(is_direct_order_flag) as direct_order_item_count,
        sum(is_foil_flag) as foil_item_count,
        sum(is_sealed_flag) as sealed_item_count,

        count(distinct case when units_sold > 0 then order_id end) as active_orders,

        max(is_kickback_promo) as is_kickback_promo,
        count(distinct kickback_name) as num_active_kickbacks,
        sum(case when is_kickback_promo = 1 then units_sold else 0 end) as promo_units_sold,
        sum(case when is_kickback_promo = 1 then revenue_usd else 0 end) as promo_revenue_usd,
        count(distinct case when is_kickback_promo = 1 then order_id end) as promo_order_count,

        avg(case when is_kickback_promo = 1 then promo_value_pct end) as avg_promo_value_pct,
        max(case when is_kickback_promo = 1 then promo_value_pct end) as max_promo_value_pct,
        avg(case when is_kickback_promo = 1 then promo_threshold_usd end) as avg_promo_threshold_usd,

        avg(case when is_kickback_promo = 1 then days_from_kickback_start end) as avg_days_from_kickback_start,
        avg(case when is_kickback_promo = 1 then days_to_kickback_end end) as avg_days_to_kickback_end,

        sum(is_pre_kickback) as pre_kickback_order_items,
        sum(is_post_kickback) as post_kickback_order_items
    from order_items_kickback_labeled
    group by
        product_id,
        product_name,
        product_number,
        product_line,
        product_type,
        publisher_name,
        set_name,
        super_condition,
        week_start_date
),

rolling_features as (
    select
        *,
        lag(weekly_units_sold, 1) over (partition by product_id order by week_start_date) as units_lag_1w,
        lag(weekly_units_sold, 2) over (partition by product_id order by week_start_date) as units_lag_2w,
        lag(weekly_units_sold, 4) over (partition by product_id order by week_start_date) as units_lag_4w,
        avg(weekly_units_sold) over (
            partition by product_id
            order by week_start_date
            rows between 4 preceding and 1 preceding
        ) as units_roll_mean_4w,
        avg(weekly_units_sold) over (
            partition by product_id
            order by week_start_date
            rows between 8 preceding and 1 preceding
        ) as units_roll_mean_8w,
        stddev_samp(weekly_units_sold) over (
            partition by product_id
            order by week_start_date
            rows between 4 preceding and 1 preceding
        ) as units_roll_std_4w
    from weekly_product_features
)

select *
from rolling_features;

---product views
with pdp_base as (
    select
        id as event_id,
        product_id,
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
        original_timestamp,
        received_at,
        sent_at,
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
        coalesce(nullif(user_id, ''), anonymous_id) as viewer_key,
        date_trunc('week', timestamp) as week_start_date
    from segment.marketplace_prd.product_details_viewed
    where timestamp >= '2025-01-01'
      and timestamp < '2026-01-01'
),

pdp_enriched as (
    select
        event_id,
        product_id,
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
        original_timestamp,
        received_at,
        sent_at,
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
        viewer_key,
        week_start_date,
        case
            when lower(coalesce(user_agent, '')) like '%iphone%'
              or lower(coalesce(user_agent, '')) like '%android%'
              or lower(coalesce(user_agent, '')) like '%mobile%'
            then 'mobile'
            when lower(coalesce(user_agent, '')) like '%ipad%'
              or lower(coalesce(user_agent, '')) like '%tablet%'
            then 'tablet'
            when coalesce(user_agent, '') <> '' then 'desktop'
            else 'unknown'
        end as device_type
    from pdp_base
),

weekly_product_features as (
    select
        product_id,
        max(product_name) as product_name,
        max(product_line) as product_line,
        max(set_name) as set_name,
        max(merch_category) as merch_category,
        max(merch_type) as merch_type,
        week_start_date,

        count(*) as pdp_views,
        count(distinct viewer_key) as unique_viewers,
        count(distinct user_id) as unique_user_ids,
        count(distinct anonymous_id) as unique_anonymous_ids,

        count(distinct uuid_ts) as event_count,
        count(distinct seller_key) as unique_sellers_viewed,
        count(distinct application) as unique_applications,
        count(distinct pdp_listing_sort) as distinct_listing_sorts,

        sum(case when new_pd_group then 1 else 0 end) as new_pd_group_views,
        avg(case when new_pd_group then 1 else 0 end) as new_pd_group_rate,

        sum(case when is_lightly_played_available then 1 else 0 end) as lightly_played_available_views,
        sum(case when is_moderately_played_available then 1 else 0 end) as moderately_played_available_views,
        sum(case when is_near_mint_available then 1 else 0 end) as near_mint_available_views,
        sum(case when is_sealed then 1 else 0 end) as sealed_views,
        sum(case when is_singles then 1 else 0 end) as singles_views,

        avg(total_listings) as avg_total_listings,
        max(total_listings) as max_total_listings,
        avg(total_sellers) as avg_total_sellers,
        max(total_sellers) as max_total_sellers,
        avg(seller_spotlight_quantity) as avg_seller_spotlight_quantity,
        max(seller_spotlight_quantity) as max_seller_spotlight_quantity,

        sum(case when device_type = 'mobile' then 1 else 0 end) as mobile_views,
        sum(case when device_type = 'desktop' then 1 else 0 end) as desktop_views,
        sum(case when device_type = 'tablet' then 1 else 0 end) as tablet_views,
        sum(case when device_type = 'unknown' then 1 else 0 end) as unknown_device_views,

        count(distinct case when user_id is not null then user_id end) as identified_users,
        count(distinct case when anonymous_id is not null then anonymous_id end) as anonymous_users
    from pdp_enriched
    group by
        product_id,
        week_start_date
),

product_view_features as (
    select
        *,
        case when unique_viewers > 0 then pdp_views / unique_viewers else 0 end as views_per_viewer,
        case when pdp_views > 0 then new_pd_group_views / pdp_views else 0 end as new_pd_group_view_share,
        case when pdp_views > 0 then sealed_views / pdp_views else 0 end as sealed_view_share,
        case when pdp_views > 0 then singles_views / pdp_views else 0 end as singles_view_share,
        case when pdp_views > 0 then mobile_views / pdp_views else 0 end as mobile_view_share,
        case when pdp_views > 0 then desktop_views / pdp_views else 0 end as desktop_view_share,
        case when pdp_views > 0 then tablet_views / pdp_views else 0 end as tablet_view_share
    from weekly_product_features
)

select *
from product_view_features;



---add to cart intent
with unified_add_to_cart_events as (

    select
        lower(trim(product_line)) || '|' || lower(trim(product_name)) as product_key,
        timestamp as event_timestamp,
        coalesce(nullif(user_id, ''), anonymous_id) as user_key,
        items_added,
        'saved_for_later' as source
    from segment.marketplace_prd.saved_for_later_added_to_cart
    where timestamp >= '2025-01-01'
      and timestamp < '2026-01-01'

    union all

    select
        lower(trim(product_line)) || '|' || lower(trim(product_name)) as product_key,
        timestamp as event_timestamp,
        coalesce(nullif(user_id, ''), anonymous_id) as user_key,
        items_added,
        'marketplace_search' as source
    from segment.marketplace_prd.marketplace_search_added_to_cart
    where timestamp >= '2025-01-01'
      and timestamp < '2026-01-01'


    union all

    select
        lower(trim(product_line)) || '|' || lower(trim(product_name)) as product_key,
        timestamp as event_timestamp,
        coalesce(nullif(user_id, ''), anonymous_id) as user_key,
        1 as items_added,
        'storefront_listing' as source
    from segment.marketplace_prd.storefront_listing_added_to_cart
    where timestamp >= '2025-01-01'
      and timestamp < '2026-01-01'
),

filtered_events as (
    select *
    from unified_add_to_cart_events
    where product_key is not null
),

weekly_atc_features as (
    select
        product_key,
        date_trunc('week', event_timestamp) as week_start_date,
        count(*) as atc_events,
        sum(items_added) as items_added_total,
        count(distinct user_key) as atc_users,

        sum(case when source = 'saved_for_later' then 1 else 0 end) as saved_for_later_events,
        sum(case when source = 'marketplace_search' then 1 else 0 end) as marketplace_search_atc_events,
        sum(case when source = 'mass_entry' then 1 else 0 end) as mass_entry_atc_events,
        sum(case when source = 'product' then 1 else 0 end) as product_atc_events,
        sum(case when source = 'storefront_listing' then 1 else 0 end) as storefront_listing_atc_events

    from filtered_events
    group by 1, 2
)

select *
from weekly_atc_features;




------------------------------------------------------Combine All--------------------------------------------------

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
    where ordered_at_et >= '2025-01-01'
      and ordered_at_et < '2026-01-01'
      and product_name is not null
      and product_line is not null
),

manual_kickbacks as (
    select
        'it''s mayhem! get 10% store credit on all products' as kickback_name,
        1 as is_all_products_flag,
        0 as is_other_flag,
        to_timestamp_ntz('2025-05-16 13:00:00') as start_ts,
        to_timestamp_ntz('2025-05-17 03:00:00') as end_ts,
        10.00 as promo_value_pct,
        50.00 as promo_threshold_usd
    union all
    select
        'cyber weekend is here! get 10% store credit on all products' as kickback_name,
        1 as is_all_products_flag,
        0 as is_other_flag,
        to_timestamp_ntz('2025-11-28 14:00:00') as start_ts,
        to_timestamp_ntz('2025-12-02 04:00:00') as end_ts,
        10.00 as promo_value_pct,
        50.00 as promo_threshold_usd
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
        case when o.is_foil then 1 else 0 end as is_foil_flag,
        case when o.is_sealed then 1 else 0 end as is_sealed_flag,
        date_trunc('week', o.ordered_at_et) as week_start_date,
        cast(o.ordered_at_et as date) as order_date,
        o.ordered_at_et,
        o.quantity as units_sold,
        o.total_usd as revenue_usd,
        o.unit_price_usd,
        o.total_weight_oz,
        o.unit_weight_oz,

        case when k.kickback_name is not null then 1 else 0 end as is_kickback_promo,
        k.kickback_name,
        k.is_all_products_flag,
        k.is_other_flag,
        k.start_ts as kickback_start_ts,
        k.end_ts as kickback_end_ts,
        k.promo_value_pct,
        k.promo_threshold_usd,
        datediff('day', cast(k.start_ts as date), cast(o.ordered_at_et as date)) as days_from_kickback_start,
        datediff('day', cast(o.ordered_at_et as date), cast(k.end_ts as date)) as days_to_kickback_end,
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
       and o.ordered_at_et < k.end_ts
),

weekly_order_features as (
    select
        product_key,
        max(product_name) as product_name,
        max(product_line) as product_line,
        max(product_type) as product_type,
        max(publisher_name) as publisher_name,
        max(set_name) as set_name,
        max(super_condition) as super_condition,
        week_start_date,

        count(*) as order_item_count,
        count(distinct order_id) as order_count,
        sum(units_sold) as weekly_units_sold,
        sum(revenue_usd) as weekly_revenue_usd,
        avg(unit_price_usd) as avg_unit_price_usd,
        avg(total_weight_oz) as avg_total_weight_oz,

        sum(is_direct_order_flag) as direct_order_item_count,
        sum(is_foil_flag) as foil_item_count,
        sum(is_sealed_flag) as sealed_item_count,

        count(distinct case when units_sold > 0 then order_id end) as active_orders,

        max(is_kickback_promo) as is_kickback_promo,
        count(distinct kickback_name) as num_active_kickbacks,
        sum(case when is_kickback_promo = 1 then units_sold else 0 end) as promo_units_sold,
        sum(case when is_kickback_promo = 1 then revenue_usd else 0 end) as promo_revenue_usd,
        count(distinct case when is_kickback_promo = 1 then order_id end) as promo_order_count,

        avg(case when is_kickback_promo = 1 then promo_value_pct end) as avg_promo_value_pct,
        max(case when is_kickback_promo = 1 then promo_value_pct end) as max_promo_value_pct,
        avg(case when is_kickback_promo = 1 then promo_threshold_usd end) as avg_promo_threshold_usd,

        avg(case when is_kickback_promo = 1 then days_from_kickback_start end) as avg_days_from_kickback_start,
        avg(case when is_kickback_promo = 1 then days_to_kickback_end end) as avg_days_to_kickback_end,

        sum(is_pre_kickback) as pre_kickback_order_items,
        sum(is_post_kickback) as post_kickback_order_items
    from order_items_kickback_labeled
    group by
        product_key,
        week_start_date
),

order_rolling_features as (
    select
        *,
        lag(weekly_units_sold, 1) over (partition by product_key order by week_start_date) as units_lag_1w,
        lag(weekly_units_sold, 2) over (partition by product_key order by week_start_date) as units_lag_2w,
        lag(weekly_units_sold, 4) over (partition by product_key order by week_start_date) as units_lag_4w,
        avg(weekly_units_sold) over (
            partition by product_key
            order by week_start_date
            rows between 4 preceding and 1 preceding
        ) as units_roll_mean_4w,
        avg(weekly_units_sold) over (
            partition by product_key
            order by week_start_date
            rows between 8 preceding and 1 preceding
        ) as units_roll_mean_8w,
        stddev_samp(weekly_units_sold) over (
            partition by product_key
            order by week_start_date
            rows between 4 preceding and 1 preceding
        ) as units_roll_std_4w
    from weekly_order_features
),

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
        original_timestamp,
        received_at,
        sent_at,
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
        coalesce(nullif(user_id, ''), anonymous_id) as viewer_key,
        date_trunc('week', timestamp) as week_start_date
    from segment.marketplace_prd.product_details_viewed
    where timestamp >= '2025-01-01'
      and timestamp < '2026-01-01'
      and product_name is not null
      and product_line is not null
),

pdp_enriched as (
    select
        event_id,
        product_key,
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
        original_timestamp,
        received_at,
        sent_at,
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
        viewer_key,
        week_start_date,
        case
            when lower(coalesce(user_agent, '')) like '%iphone%'
              or lower(coalesce(user_agent, '')) like '%android%'
              or lower(coalesce(user_agent, '')) like '%mobile%'
            then 'mobile'
            when lower(coalesce(user_agent, '')) like '%ipad%'
              or lower(coalesce(user_agent, '')) like '%tablet%'
            then 'tablet'
            when coalesce(user_agent, '') <> '' then 'desktop'
            else 'unknown'
        end as device_type
    from pdp_base
),

weekly_pdp_features as (
    select
        product_key,
        max(product_name) as product_name,
        max(product_line) as product_line,
        max(set_name) as set_name,
        max(merch_category) as merch_category,
        max(merch_type) as merch_type,
        week_start_date,

        count(*) as pdp_views,
        count(distinct viewer_key) as unique_viewers,
        count(distinct user_id) as unique_user_ids,
        count(distinct anonymous_id) as unique_anonymous_ids,

        count(distinct uuid_ts) as event_count,
        count(distinct seller_key) as unique_sellers_viewed,
        count(distinct application) as unique_applications,
        count(distinct pdp_listing_sort) as distinct_listing_sorts,

        sum(case when new_pd_group then 1 else 0 end) as new_pd_group_views,
        avg(case when new_pd_group then 1 else 0 end) as new_pd_group_rate,

        sum(case when is_lightly_played_available then 1 else 0 end) as lightly_played_available_views,
        sum(case when is_moderately_played_available then 1 else 0 end) as moderately_played_available_views,
        sum(case when is_near_mint_available then 1 else 0 end) as near_mint_available_views,
        sum(case when is_sealed then 1 else 0 end) as sealed_views,
        sum(case when is_singles then 1 else 0 end) as singles_views,

        avg(total_listings) as avg_total_listings,
        max(total_listings) as max_total_listings,
        avg(total_sellers) as avg_total_sellers,
        max(total_sellers) as max_total_sellers,
        avg(seller_spotlight_quantity) as avg_seller_spotlight_quantity,
        max(seller_spotlight_quantity) as max_seller_spotlight_quantity,

        sum(case when device_type = 'mobile' then 1 else 0 end) as mobile_views,
        sum(case when device_type = 'desktop' then 1 else 0 end) as desktop_views,
        sum(case when device_type = 'tablet' then 1 else 0 end) as tablet_views,
        sum(case when device_type = 'unknown' then 1 else 0 end) as unknown_device_views,

        count(distinct case when user_id is not null then user_id end) as identified_users,
        count(distinct case when anonymous_id is not null then anonymous_id end) as anonymous_users
    from pdp_enriched
    group by
        product_key,
        week_start_date
),

pdp_features as (
    select
        *,
        case when unique_viewers > 0 then pdp_views / unique_viewers else 0 end as views_per_viewer,
        case when pdp_views > 0 then new_pd_group_views / pdp_views else 0 end as new_pd_group_view_share,
        case when pdp_views > 0 then sealed_views / pdp_views else 0 end as sealed_view_share,
        case when pdp_views > 0 then singles_views / pdp_views else 0 end as singles_view_share,
        case when pdp_views > 0 then mobile_views / pdp_views else 0 end as mobile_view_share,
        case when pdp_views > 0 then desktop_views / pdp_views else 0 end as desktop_view_share,
        case when pdp_views > 0 then tablet_views / pdp_views else 0 end as tablet_view_share
    from weekly_pdp_features
),

unified_add_to_cart_events as (
    select
        lower(trim(coalesce(product_line, ''))) || '|' || lower(trim(coalesce(product_name, ''))) as product_key,
        timestamp as event_timestamp,
        coalesce(nullif(user_id, ''), anonymous_id) as user_key,
        coalesce(items_added, 1) as items_added,
        'saved_for_later' as source
    from segment.marketplace_prd.saved_for_later_added_to_cart
    where timestamp >= '2025-01-01'
      and timestamp < '2026-01-01'
      and product_name is not null
      and product_line is not null

    union all

    select
        lower(trim(coalesce(product_line, ''))) || '|' || lower(trim(coalesce(product_name, ''))) as product_key,
        timestamp as event_timestamp,
        coalesce(nullif(user_id, ''), anonymous_id) as user_key,
        coalesce(items_added, 1) as items_added,
        'marketplace_search' as source
    from segment.marketplace_prd.marketplace_search_added_to_cart
    where timestamp >= '2025-01-01'
      and timestamp < '2026-01-01'
      and product_name is not null
      and product_line is not null


    union all

    select
        lower(trim(coalesce(product_line, ''))) || '|' || lower(trim(coalesce(product_name, ''))) as product_key,
        timestamp as event_timestamp,
        coalesce(nullif(user_id, ''), anonymous_id) as user_key,
        1 as items_added,
        'storefront_listing' as source
    from segment.marketplace_prd.storefront_listing_added_to_cart
    where timestamp >= '2025-01-01'
      and timestamp < '2026-01-01'
      and product_name is not null
      and product_line is not null
),

filtered_atc_events as (
    select *
    from unified_add_to_cart_events
    where product_key is not null
      and product_key <> '|'
),

weekly_atc_features as (
    select
        product_key,
        date_trunc('week', event_timestamp) as week_start_date,
        count(*) as atc_events,
        sum(items_added) as items_added_total,
        count(distinct user_key) as atc_users,

        sum(case when source = 'saved_for_later' then 1 else 0 end) as saved_for_later_events,
        sum(case when source = 'marketplace_search' then 1 else 0 end) as marketplace_search_atc_events,
        sum(case when source = 'mass_entry' then 1 else 0 end) as mass_entry_atc_events,
        sum(case when source = 'product' then 1 else 0 end) as product_atc_events,
        sum(case when source = 'storefront_listing' then 1 else 0 end) as storefront_listing_atc_events
    from filtered_atc_events
    group by 1, 2
),

atc_features as (
    select
        *,
        case when atc_events > 0 then items_added_total / atc_events else 0 end as items_per_event,
        case when atc_users > 0 then atc_events / atc_users else 0 end as events_per_user,
        case when atc_events > 0 then saved_for_later_events / atc_events else 0 end as saved_for_later_share,
        case when atc_events > 0 then marketplace_search_atc_events / atc_events else 0 end as marketplace_search_share,
        case when atc_events > 0 then mass_entry_atc_events / atc_events else 0 end as mass_entry_share,
        case when atc_events > 0 then product_atc_events / atc_events else 0 end as product_share,
        case when atc_events > 0 then storefront_listing_atc_events / atc_events else 0 end as storefront_listing_share
    from weekly_atc_features
),

final_model_table as (
    select
        coalesce(o.product_key, p.product_key, a.product_key) as product_key,
        coalesce(o.week_start_date, p.week_start_date, a.week_start_date) as week_start_date,

        o.product_name as order_product_name,
        o.product_line as order_product_line,
        o.product_type,
        o.publisher_name,
        o.set_name,
        o.super_condition,

        o.order_item_count,
        o.order_count,
        o.weekly_units_sold,
        o.weekly_revenue_usd,
        o.avg_unit_price_usd,
        o.avg_total_weight_oz,
        o.direct_order_item_count,
        o.foil_item_count,
        o.sealed_item_count,
        o.active_orders,
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
        o.units_lag_1w,
        o.units_lag_2w,
        o.units_lag_4w,
        o.units_roll_mean_4w,
        o.units_roll_mean_8w,
        o.units_roll_std_4w,

        p.product_name as view_product_name,
        p.product_line as view_product_line,
        p.set_name as view_set_name,
        p.merch_category,
        p.merch_type,
        p.pdp_views,
        p.unique_viewers,
        p.unique_user_ids,
        p.unique_anonymous_ids,
        p.event_count as pdp_event_count,
        p.unique_sellers_viewed,
        p.unique_applications,
        p.distinct_listing_sorts,
        p.new_pd_group_views,
        p.new_pd_group_rate,
        p.lightly_played_available_views,
        p.moderately_played_available_views,
        p.near_mint_available_views,
        p.sealed_views as pdp_sealed_views,
        p.singles_views as pdp_singles_views,
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
        p.sealed_view_share as pdp_sealed_view_share,
        p.singles_view_share as pdp_singles_view_share,
        p.mobile_view_share,
        p.desktop_view_share,
        p.tablet_view_share,

        a.atc_events,
        a.items_added_total,
        a.atc_users,
        a.saved_for_later_events,
        a.marketplace_search_atc_events,
        a.mass_entry_atc_events,
        a.product_atc_events,
        a.storefront_listing_atc_events,
        a.items_per_event as atc_items_per_event,
        a.events_per_user as atc_events_per_user,
        a.saved_for_later_share,
        a.marketplace_search_share,
        a.mass_entry_share,
        a.product_share,
        a.storefront_listing_share

    from order_rolling_features o
    full outer join pdp_features p
        on o.product_key = p.product_key
       and o.week_start_date = p.week_start_date
    full outer join atc_features a
        on coalesce(o.product_key, p.product_key) = a.product_key
       and coalesce(o.week_start_date, p.week_start_date) = a.week_start_date
)

select *
from final_model_table;