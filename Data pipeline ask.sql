--ask:
--list (Apr 1, 2025 – Mar 31, 2026)
--1. TCGplayer seller name
--2. Business address
--3. Total GMV
--4. Sealed product GMV
--5. Singles GMV
--6. Direct GMV
--7. Total number of sold items (fixed typo)
--8. Number of sealed product items sold
--9. Number of cards sold
--10. Date joined TCGplayer
--11. WPN badge seller Y/N
--12. Direct seller Y/N
--13. Pro Seller Y/N
--14. TCGplayer seller type (Enterprise, Professional, Pre-Professional, etc.)
--15. Number of listings live on TCGplayer
--Critical additions
--16. Game/category mix (% GMV by MTG, Pokémon, Yu-Gi-Oh, Lorcana, etc.) — WPN is MTG-specific; without this you can't triage WPN relevance.
--17. TCGplayer Live participation Y/N + Live GMV — direct parallel to eBay's GMV_LIVE; identifies Live recruits.
--18. Last sale date / active in last 90 days Y/N — 789K sellers needs a dormancy filter before Live/Partnerships outreach.
--19. Address parsed (street, city, state, zip) — cleaner address match for Ankit's next pass; enables regional cuts now.
--20. Seller status (active/suspended/closed) — avoid wasting outreach on non-transacting accounts.
--Nice to have
--21. Buylist participation Y/N — operational sophistication signal.
--22. Seller feedback score/rating — quality bar for outreach.
--23. Shipping performance (on-time %, cancel rate, defect rate) — protects Live/WPN brand from bad-ops recruits.
--24. YoY GMV growth (Apr'24–Mar'25 vs Apr'25–Mar'26) — separates rising from declining sellers.
--25. TCGplayer storefront URL / public seller name — extra join key for matching.
--26. Unique SKU count — distinguishes deep inventory from duplicate listings.

----------------------------------------------------------------------------------------------------------------------
-- STEP 1 – ORDER ITEMS: current year (Apr 2025 – Mar 2026)
----------------------------------------------------------------------------------------------------------------------
with oi_current as (
    select
        oi.seller_order_id,
        oi.order_id,
        oi.product_line,                        -- MTG, Pokemon, Yu-Gi-Oh, Lorcana, etc.
        oi.product_type,                        -- 'Sealed' vs 'Singles'
        oi.is_sealed,
        oi.is_direct_order,
        oi.quantity,
        oi.total_usd,
        oi.unit_price_usd,
        oi.ordered_at_et,
        oi.updated_at_et
    from analytics.core.order_items oi
    where oi.ordered_at_et >= '2025-04-01'
      and oi.ordered_at_et <  '2026-04-01'
),

----------------------------------------------------------------------------------------------------------------------
-- STEP 2 – SELLER ORDERS: current year — join to get seller_id & order status
----------------------------------------------------------------------------------------------------------------------
so_current as (
    select
        so.id              as seller_order_id,
        so.seller_id,
        so.order_id,
        so.ordered_at_et,
        so.seller_order_status,
        so.is_complete,
        so.net_seller_revenue_marketplace_usd,
        so.order_amount_usd,
        so.product_amount_usd,
        so.shipping_usd,
        -- Live order flag: TCGplayer Live orders appear as a specific channel/flag
        -- Using IS_MARKETPLACE_ORDER as a proxy; refine if a dedicated live flag exists
        so.is_marketplace_order,
        so.is_direct_order,
        so.is_presale,
        so.has_refund,
        so.refunded_order_amount_no_tax_usd
    from analytics.core.seller_orders so
    where so.ordered_at_et >= '2025-04-01'
      and so.ordered_at_et <  '2026-04-01'
      -- exclude internal / test orders
      and so.is_internal_seller = false
),

----------------------------------------------------------------------------------------------------------------------
-- STEP 3 – JOIN ORDER ITEMS → SELLER ORDERS (current year)
-- Build one row per order_item enriched with seller_id
----------------------------------------------------------------------------------------------------------------------
items_with_seller_current as (
    select
        so.seller_id,
        oi.seller_order_id,
        oi.product_line,
        oi.product_type,
        oi.is_sealed,
        oi.is_direct_order,
        oi.quantity,
        -- Net GMV = total_usd minus any refunds (allocated proportionally via order-level refund flag)
        -- Simple approach: use total_usd; refund-adjusted GMV computed at seller_order grain below
        oi.total_usd,
        oi.unit_price_usd,
        oi.ordered_at_et,
        so.seller_order_status,
        so.has_refund,
        so.refunded_order_amount_no_tax_usd,
        so.is_marketplace_order
    from oi_current oi
    inner join so_current so
        on oi.seller_order_id = so.seller_order_id
),

----------------------------------------------------------------------------------------------------------------------
-- STEP 4 – SELLER LEVEL AGGREGATIONS: current year GMV & items
----------------------------------------------------------------------------------------------------------------------
seller_current_agg as (
    select
        seller_id,

        -- ── GMV (gross, before refunds) ──────────────────────────────────────
        sum(total_usd)                                                       as total_gmv,
        sum(case when is_sealed = true  then total_usd else 0 end)           as sealed_gmv,
        sum(case when is_sealed = false then total_usd else 0 end)           as singles_gmv,
        sum(case when is_direct_order = true then total_usd else 0 end)      as direct_gmv,

        -- Live GMV proxy (marketplace orders that are not direct)
        -- Refine this if TCGplayer Live has a dedicated channel flag in SELLER_ORDERS
        sum(case when is_marketplace_order = true
                  and is_direct_order = false
             then total_usd else 0 end)                                      as live_gmv_proxy,

        -- ── Item / unit counts ───────────────────────────────────────────────
        sum(quantity)                                                         as total_items_sold,
        sum(case when is_sealed = true  then quantity else 0 end)             as sealed_items_sold,
        sum(case when is_sealed = false then quantity else 0 end)             as cards_sold,

        -- ── Unique SKUs (depth of inventory signal) ──────────────────────────
        count(distinct unit_price_usd)                                        as unique_sku_count,
        -- Note: true SKU count needs product_condition_id from order_items;
        -- unit_price_usd is a proxy. Replace with count(distinct product_condition_id)
        -- if that column is available on your order_items pull.

        -- ── Activity / recency ───────────────────────────────────────────────
        max(ordered_at_et)                                                    as last_sale_date,
        min(ordered_at_et)                                                    as first_sale_date_in_period,
        count(distinct date_trunc('day', ordered_at_et))                      as active_selling_days,

        -- ── Game / category GMV mix ───────────────────────────────────────────
        -- % of GMV by major TCG product lines
        sum(case when lower(product_line) like '%magic%'
                  or lower(product_line) like '%mtg%'
             then total_usd else 0 end)                                       as mtg_gmv,

        sum(case when lower(product_line) like '%pokemon%'
             then total_usd else 0 end)                                       as pokemon_gmv,

        sum(case when lower(product_line) like '%yu-gi-oh%'
                  or lower(product_line) like '%yugioh%'
                  or lower(product_line) like '%ygo%'
             then total_usd else 0 end)                                       as yugioh_gmv,

        sum(case when lower(product_line) like '%lorcana%'
             then total_usd else 0 end)                                       as lorcana_gmv,

        sum(case when lower(product_line) like '%one piece%'
             then total_usd else 0 end)                                       as one_piece_gmv,

        sum(case when lower(product_line) like '%flesh%'   -- Flesh and Blood
             then total_usd else 0 end)                                       as fab_gmv,

        sum(case when lower(product_line) not like '%magic%'
                  and lower(product_line) not like '%mtg%'
                  and lower(product_line) not like '%pokemon%'
                  and lower(product_line) not like '%yu-gi-oh%'
                  and lower(product_line) not like '%yugioh%'
                  and lower(product_line) not like '%ygo%'
                  and lower(product_line) not like '%lorcana%'
                  and lower(product_line) not like '%one piece%'
                  and lower(product_line) not like '%flesh%'
             then total_usd else 0 end)                                       as other_gmv

    from items_with_seller_current
    group by seller_id
),

----------------------------------------------------------------------------------------------------------------------
-- STEP 5 – PRIOR YEAR GMV (Apr 2024 – Mar 2025) for YoY growth
----------------------------------------------------------------------------------------------------------------------
oi_prior as (
    select
        oi.seller_order_id,
        oi.total_usd
    from analytics.core.order_items oi
    where oi.ordered_at_et >= '2024-04-01'
      and oi.ordered_at_et <  '2025-04-01'
),

so_prior as (
    select
        so.id  as seller_order_id,
        so.seller_id
    from analytics.core.seller_orders so
    where so.ordered_at_et >= '2024-04-01'
      and so.ordered_at_et <  '2025-04-01'
      and so.is_internal_seller = false
),

seller_prior_gmv as (
    select
        so.seller_id,
        sum(oi.total_usd) as prior_year_gmv
    from oi_prior oi
    inner join so_prior so on oi.seller_order_id = so.seller_order_id
    group by so.seller_id
),

----------------------------------------------------------------------------------------------------------------------
-- STEP 6 – WPN ACTIVATIONS (Google Sheets → Fivetran)
-- seller_key is an 8-digit hex matching ANALYTICS.CORE.SELLERS.KEY
----------------------------------------------------------------------------------------------------------------------
wpn as (
    select
        seller_key,
        -- treat as WPN-badged if activation date exists and no deactivation (or deactivation in future)
        case
            when wpn_activation_date is not null
             and (wpn_deactivation_date is null or try_to_date(wpn_deactivation_date) >= '2025-04-01')
            then 'Y'
            else 'N'
        end                                          as is_wpn_seller,
        try_to_date(wpn_activation_date)              as wpn_activation_date,
        try_to_date(wpn_deactivation_date)               as wpn_deactivation_date,
        test_group
    from analytics.sources.source_google_sheets__wpn_activations
),

----------------------------------------------------------------------------------------------------------------------
-- STEP 7 – SELLER MASTER: pull all relevant attributes from ANALYTICS.CORE.SELLERS
----------------------------------------------------------------------------------------------------------------------
seller_master as (
    select
        -- ── Identity ─────────────────────────────────────────────────────────
        s.id                                         as seller_id,
        s.name                                       as seller_name,
        s.key                                        as seller_key,             -- 8-digit hex (join to WPN)
        s.admin_url                                  as tcgplayer_storefront_url,

        -- ── Status & segment ─────────────────────────────────────────────────
        s.status,                                    -- Active / Suspended / Closed / Hold
        s.seller_segment,                            -- e.g. Enterprise, Professional, Pre-Professional
        s.level,                                     -- Level 1 / Level 2 / etc.
        s.maturity_tier,
        s.pro_services_level,
        s.rate_card,
        s.type                                       as seller_type,            -- Marketplace / Store / etc.

        -- ── Seller flags ─────────────────────────────────────────────────────
        case when s.is_seller_live           then 'Y' else 'N' end as is_currently_live,
        case when s.is_direct                then 'Y' else 'N' end as is_direct_seller,
        case when s.is_tcgplayer_pro         then 'Y' else 'N' end as is_pro_seller,
        case when s.is_gold_star             then 'Y' else 'N' end as is_gold_star_seller,
        case when s.is_buylist_enabled       then 'Y' else 'N' end as is_buylist_seller,
        case when s.is_certified_hobby_shop  then 'Y' else 'N' end as is_certified_hobby_shop,
        case when s.is_consignment_seller    then 'Y' else 'N' end as is_consignment_seller,
        case when s.is_vip                   then 'Y' else 'N' end as is_vip_seller,
        case when s.is_employee              then 'Y' else 'N' end as is_employee_account,
        case when s.is_test_seller           then 'Y' else 'N' end as is_test_account,
        case when s.has_seller_enabled_presales then 'Y' else 'N' end as has_presales_enabled,
        case when s.is_store_your_products   then 'Y' else 'N' end as is_store_your_products,

        -- ── Dates ────────────────────────────────────────────────────────────
        cast(s.created_at_et as date)                as date_joined_tcgplayer,

        -- ── Address (full + parsed) ───────────────────────────────────────────
        -- Full address for display
        trim(
            coalesce(s.shipping_street_address_1, '') ||
            case when s.shipping_street_address_2 is not null
                 then ', ' || s.shipping_street_address_2 else '' end ||
            ', ' || coalesce(s.shipping_city, '') ||
            ', ' || coalesce(s.shipping_state, '') ||
            ' '  || coalesce(s.shipping_zipcode, '') ||
            ', ' || coalesce(s.shipping_country, '')
        )                                            as full_business_address,
        -- Parsed address fields (for regional cuts & match)
        s.shipping_street_address_1                  as address_street_1,
        s.shipping_street_address_2                  as address_street_2,
        s.shipping_city                              as address_city,
        s.shipping_state                             as address_state,
        s.shipping_zipcode                           as address_zip,
        s.shipping_country                           as address_country,
        s.location                                   as seller_location_state,  -- 2-letter state code

        -- ── Financial / operational ──────────────────────────────────────────
        s.daily_refund_limit,
        s.daily_refund_count,
        s.tax_rate,
        s.payment_type,
        s.payment_processor,

        -- ── eCommerce integrations ───────────────────────────────────────────
        s.ecommerce_provider_name,
        s.binder_id,                                -- Binder POS integration

        -- ── Live / number of listings ────────────────────────────────────────
        -- SELLERS table doesn't carry live listing count directly.
        -- This would need a join to a listings/inventory table if available.
        -- Placeholder kept as null; replace with actual listings source.
        null::number                                 as live_listing_count

    from analytics.core.sellers s
    -- Exclude internal employees and test accounts from outreach lists
    where s.is_employee   = false
      and s.is_test_seller = false
),

----------------------------------------------------------------------------------------------------------------------
-- STEP 8 – FINAL ASSEMBLY
----------------------------------------------------------------------------------------------------------------------
final as (
    select
        -- ════════════════════════════════════════════════════════════════════
        -- SECTION A: IDENTITY & CONTACT
        -- ════════════════════════════════════════════════════════════════════
        sm.seller_id,
        sm.seller_name,                             -- #1  TCGplayer seller name
        sm.tcgplayer_storefront_url,                -- #25 Storefront URL / public name

        -- ════════════════════════════════════════════════════════════════════
        -- SECTION B: ADDRESS (parsed) — #19
        -- ════════════════════════════════════════════════════════════════════
        sm.full_business_address,                   -- #2  Full business address
        sm.address_street_1,                        -- #19 Street
        sm.address_street_2,
        sm.address_city,                            -- #19 City
        sm.address_state,                           -- #19 State
        sm.address_zip,                             -- #19 ZIP
        sm.address_country,
        sm.seller_location_state,

        -- ════════════════════════════════════════════════════════════════════
        -- SECTION C: GMV — #3 #4 #5 #6
        -- ════════════════════════════════════════════════════════════════════
        coalesce(cy.total_gmv,   0)                 as total_gmv,              -- #3
        coalesce(cy.sealed_gmv,  0)                 as sealed_product_gmv,     -- #4
        coalesce(cy.singles_gmv, 0)                 as singles_gmv,            -- #5
        coalesce(cy.direct_gmv,  0)                 as direct_gmv,             -- #6
        coalesce(cy.live_gmv_proxy, 0)              as live_gmv,               -- #17 Live GMV

        -- ════════════════════════════════════════════════════════════════════
        -- SECTION D: UNIT COUNTS — #7 #8 #9
        -- ════════════════════════════════════════════════════════════════════
        coalesce(cy.total_items_sold,  0)           as total_items_sold,       -- #7
        coalesce(cy.sealed_items_sold, 0)           as sealed_items_sold,      -- #8
        coalesce(cy.cards_sold,        0)           as cards_sold,             -- #9

        -- ════════════════════════════════════════════════════════════════════
        -- SECTION E: DATES & STATUS — #10 #18 #20
        -- ════════════════════════════════════════════════════════════════════
        sm.date_joined_tcgplayer,                   -- #10 Date joined TCGplayer

        cy.last_sale_date,                          -- #18 Last sale date
        case
            when cy.last_sale_date >= dateadd('day', -90, current_date())
            then 'Y' else 'N'
        end                                         as active_last_90_days,    -- #18 dormancy filter

        sm.status                                   as seller_status,          -- #20 Active/Suspended/Closed
        sm.is_currently_live                        as seller_is_live_status,

        -- ════════════════════════════════════════════════════════════════════
        -- SECTION F: SELLER FLAGS — #11 #12 #13 #17 #21
        -- ════════════════════════════════════════════════════════════════════
        coalesce(w.is_wpn_seller, 'N')              as is_wpn_seller,          -- #11 WPN badge Y/N
        sm.is_direct_seller,                        -- #12 Direct seller Y/N
        sm.is_pro_seller,                           -- #13 Pro seller Y/N

        -- #17 TCGplayer Live participation Y/N
        -- Live GMV > 0 as proxy for participation; refine with dedicated Live flag if available
        case
            when coalesce(cy.live_gmv_proxy, 0) > 0 then 'Y' else 'N'
        end                                         as participates_in_live,

        sm.is_buylist_seller,                       -- #21 Buylist participation Y/N
        sm.is_gold_star_seller,
        sm.is_certified_hobby_shop,
        sm.is_consignment_seller,
        sm.is_vip_seller,
        sm.has_presales_enabled,
        sm.is_store_your_products,

        -- ════════════════════════════════════════════════════════════════════
        -- SECTION G: SELLER TIER & TYPE — #14
        -- ════════════════════════════════════════════════════════════════════
        sm.seller_segment                           as seller_tier,            -- #14 Enterprise / Professional / etc.
        sm.seller_type,
        sm.level                                    as seller_level,
        sm.maturity_tier,
        sm.pro_services_level,
        sm.rate_card,
        sm.ecommerce_provider_name,


        -- ════════════════════════════════════════════════════════════════════
        -- SECTION I: GAME / CATEGORY MIX — #16
        -- ════════════════════════════════════════════════════════════════════
        coalesce(cy.mtg_gmv,       0)               as mtg_gmv,
        coalesce(cy.pokemon_gmv,   0)               as pokemon_gmv,
        coalesce(cy.yugioh_gmv,    0)               as yugioh_gmv,
        coalesce(cy.lorcana_gmv,   0)               as lorcana_gmv,
        coalesce(cy.one_piece_gmv, 0)               as one_piece_gmv,
        coalesce(cy.fab_gmv,       0)               as flesh_and_blood_gmv,
        coalesce(cy.other_gmv,     0)               as other_game_gmv,

        -- % of GMV by game (WPN relevance = MTG% is the key signal)
        case when coalesce(cy.total_gmv, 0) > 0
             then round(cy.mtg_gmv       / cy.total_gmv * 100, 1) else 0 end  as mtg_gmv_pct,
        case when coalesce(cy.total_gmv, 0) > 0
             then round(cy.pokemon_gmv   / cy.total_gmv * 100, 1) else 0 end  as pokemon_gmv_pct,
        case when coalesce(cy.total_gmv, 0) > 0
             then round(cy.yugioh_gmv    / cy.total_gmv * 100, 1) else 0 end  as yugioh_gmv_pct,
        case when coalesce(cy.total_gmv, 0) > 0
             then round(cy.lorcana_gmv   / cy.total_gmv * 100, 1) else 0 end  as lorcana_gmv_pct,
        case when coalesce(cy.total_gmv, 0) > 0
             then round(cy.one_piece_gmv / cy.total_gmv * 100, 1) else 0 end  as one_piece_gmv_pct,
        case when coalesce(cy.total_gmv, 0) > 0
             then round(cy.fab_gmv       / cy.total_gmv * 100, 1) else 0 end  as fab_gmv_pct,
        case when coalesce(cy.total_gmv, 0) > 0
             then round(cy.other_gmv     / cy.total_gmv * 100, 1) else 0 end  as other_game_gmv_pct,

        -- ════════════════════════════════════════════════════════════════════
        -- SECTION J: YoY GMV GROWTH — #24
        -- ════════════════════════════════════════════════════════════════════
        coalesce(py.prior_year_gmv, 0)              as prior_year_gmv,
        coalesce(cy.total_gmv, 0)
            - coalesce(py.prior_year_gmv, 0)        as yoy_gmv_change,
        case
            when coalesce(py.prior_year_gmv, 0) > 0
            then round(
                (coalesce(cy.total_gmv, 0) - py.prior_year_gmv)
                / py.prior_year_gmv * 100, 1)
            else null
        end                                         as yoy_gmv_growth_pct,    -- #24


        -- ════════════════════════════════════════════════════════════════════
        -- SECTION L: WPN DETAIL
        -- ════════════════════════════════════════════════════════════════════
        w.wpn_activation_date,
        w.wpn_deactivation_date,
        w.test_group                                as wpn_test_group,

        -- ════════════════════════════════════════════════════════════════════
        -- SECTION M: MISC OPERATIONAL
        -- ════════════════════════════════════════════════════════════════════
        sm.payment_type,
        sm.payment_processor,
        sm.binder_id,


    from seller_master sm

    -- Current year activity
    left join seller_current_agg cy
        on sm.seller_id = cy.seller_id

    -- Prior year GMV for YoY
    left join seller_prior_gmv py
        on sm.seller_id = py.seller_id

    -- WPN badge (join on hex seller key)
    left join wpn w
        on sm.seller_key = w.seller_key
)

select *
from final;