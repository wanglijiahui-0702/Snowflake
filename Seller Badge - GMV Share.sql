WITH params AS (
    SELECT
        TO_DATE('2024-01-01') AS start_date
),

wpn_sellers AS (
    SELECT
        LOWER(LPAD(TRIM(SELLER_KEY), 8, '0')) AS seller_key,
        MIN(WPN_ACTIVATION_DATE) AS wpn_activation_date,
        MAX(WPN_DEACTIVATION_DATE) AS wpn_deactivation_date
    FROM ANALYTICS.SOURCES.SOURCE_GOOGLE_SHEETS__WPN_ACTIVATIONS
    WHERE SELLER_KEY IS NOT NULL
    GROUP BY 1
),

order_item_base AS (
    SELECT
        so.ID AS seller_order_id,
        so.ORDER_ID,
        so.ORDER_NUMBER,
        so.SELLER_ID AS seller_id,
        so.BUYER_ID AS buyer_id,
        so.ORDERED_AT_ET,
        CAST(so.ORDERED_AT_ET AS DATE) AS order_date,

        CASE
            WHEN oi.PRODUCT_LINE = 'Magic'
                THEN 'MTG'

            WHEN oi.PRODUCT_LINE = 'Pokemon'
                THEN 'Pokemon'

            WHEN oi.PRODUCT_LINE = 'YuGiOh'
                THEN 'YGO'

            ELSE 'Other'
        END AS game_category,

        SUM(
            COALESCE(
                oi.TOTAL_USD,
                oi.UNIT_PRICE_USD * oi.QUANTITY,
                0
            )
        ) AS gmv_usd,

        SUM(COALESCE(oi.QUANTITY, 0)) AS units,
        COUNT(DISTINCT oi.ID) AS order_item_count

    FROM ANALYTICS.CORE.SELLER_ORDERS so

    INNER JOIN ANALYTICS.CORE.ORDER_ITEMS oi
        ON oi.SELLER_ORDER_ID = so.ID

    INNER JOIN params p
        ON CAST(so.ORDERED_AT_ET AS DATE) >= p.start_date

    WHERE COALESCE(so.IS_COMPLETE, FALSE) = TRUE
      AND COALESCE(so.IS_MARKETPLACE_ORDER, FALSE) = TRUE
      AND COALESCE(so.IS_INTERNAL_SELLER, FALSE) = FALSE
      AND COALESCE(so.HAS_BUYER_PAID, TRUE) = TRUE

    GROUP BY
        so.ID,
        so.ORDER_ID,
        so.ORDER_NUMBER,
        so.SELLER_ID,
        so.BUYER_ID,
        so.ORDERED_AT_ET,
        CAST(so.ORDERED_AT_ET AS DATE),
        CASE
            WHEN oi.PRODUCT_LINE ILIKE '%magic%'
              OR oi.PRODUCT_LINE ILIKE '%mtg%'
              OR oi.PUBLISHER_NAME ILIKE '%wizards%'
                THEN 'MTG'

            WHEN oi.PRODUCT_LINE ILIKE '%pok%'
              OR oi.PUBLISHER_NAME ILIKE '%pok%'
                THEN 'Pokemon'

            WHEN oi.PRODUCT_LINE ILIKE '%yu-gi-oh%'
              OR oi.PRODUCT_LINE ILIKE '%yugioh%'
              OR oi.PRODUCT_LINE ILIKE '%ygo%'
              OR oi.PUBLISHER_NAME ILIKE '%konami%'
                THEN 'YGO'

            ELSE 'Other'
        END
),

badged_order_items AS (
    SELECT
        oib.*,

        COALESCE(NULLIF(TRIM(s.SELLER_SEGMENT), ''), 'Unknown') AS seller_segment,

        IFF(COALESCE(s.IS_GOLD_STAR, FALSE), 1, 0) AS gold_star,

        IFF(COALESCE(s.IS_CERTIFIED_HOBBY_SHOP, FALSE), 1, 0) AS chs,

        IFF(
            (
                s.JOINED_DIRECT_AT_ET IS NOT NULL
                AND s.JOINED_DIRECT_AT_ET <= oib.ORDERED_AT_ET
                AND (
                    s.OPTED_OUT_DIRECT_AT_ET IS NULL
                    OR s.OPTED_OUT_DIRECT_AT_ET > oib.ORDERED_AT_ET
                )
            )
            OR (
                s.JOINED_DIRECT_AT_ET IS NULL
                AND COALESCE(s.IS_DIRECT, FALSE)
            ),
            1,
            0
        ) AS direct,

        IFF(
            w.seller_key IS NOT NULL
            AND w.wpn_activation_date <= oib.order_date
            AND (
                w.wpn_deactivation_date IS NULL
                OR w.wpn_deactivation_date > oib.order_date
            ),
            1,
            0
        ) AS wpn

    FROM order_item_base oib

    INNER JOIN ANALYTICS.CORE.SELLERS s
        ON s.ID = oib.seller_id

    LEFT JOIN wpn_sellers w
        ON LOWER(LPAD(TRIM(s.KEY), 8, '0')) = w.seller_key

    WHERE oib.game_category IN ('MTG', 'Pokemon', 'YGO')
),

periodized AS (
    SELECT
        'month' AS period_grain,
        DATE_TRUNC('month', ORDERED_AT_ET)::DATE AS period_start,
        game_category,
        seller_order_id,
        order_id,
        order_number,
        seller_id,
        buyer_id,
        gmv_usd,
        units,
        order_item_count,
        seller_segment,
        gold_star,
        chs,
        direct,
        wpn
    FROM badged_order_items

    UNION ALL

    SELECT
        'quarter' AS period_grain,
        DATE_TRUNC('quarter', ORDERED_AT_ET)::DATE AS period_start,
        game_category,
        seller_order_id,
        order_id,
        order_number,
        seller_id,
        buyer_id,
        gmv_usd,
        units,
        order_item_count,
        seller_segment,
        gold_star,
        chs,
        direct,
        wpn
    FROM badged_order_items
),

marketplace_totals AS (
    SELECT
        period_grain,
        period_start,

        CASE
            WHEN period_grain = 'month'
                THEN LAST_DAY(period_start, 'month')
            WHEN period_grain = 'quarter'
                THEN LAST_DAY(period_start, 'quarter')
        END AS period_end,

        game_category,

        SUM(gmv_usd) AS total_marketplace_gmv_usd,

        COUNT(DISTINCT seller_order_id) AS total_seller_order_count,
        COUNT(DISTINCT order_id) AS total_order_count,
        COUNT(DISTINCT seller_id) AS total_seller_count,
        COUNT(DISTINCT buyer_id) AS total_buyer_count,

        SUM(units) AS total_units,
        SUM(order_item_count) AS total_order_item_count

    FROM periodized

    GROUP BY
        period_grain,
        period_start,
        game_category
),

badge_fact AS (
    SELECT
        period_grain,
        period_start,
        game_category,
        'Gold Star' AS badge_type,
        seller_order_id,
        order_id,
        seller_id,
        buyer_id,
        gmv_usd,
        units,
        order_item_count
    FROM periodized
    WHERE gold_star = 1

    UNION ALL

    SELECT
        period_grain,
        period_start,
        game_category,
        'CHS' AS badge_type,
        seller_order_id,
        order_id,
        seller_id,
        buyer_id,
        gmv_usd,
        units,
        order_item_count
    FROM periodized
    WHERE chs = 1

    UNION ALL

    SELECT
        period_grain,
        period_start,
        game_category,
        'Direct' AS badge_type,
        seller_order_id,
        order_id,
        seller_id,
        buyer_id,
        gmv_usd,
        units,
        order_item_count
    FROM periodized
    WHERE direct = 1
      AND game_category IN ('MTG', 'Pokemon', 'YGO')

    UNION ALL

    SELECT
        period_grain,
        period_start,
        game_category,
        'WPN' AS badge_type,
        seller_order_id,
        order_id,
        seller_id,
        buyer_id,
        gmv_usd,
        units,
        order_item_count
    FROM periodized
    WHERE wpn = 1
      AND game_category = 'MTG'

    UNION ALL

    SELECT
        period_grain,
        period_start,
        game_category,
        'Any Badge' AS badge_type,
        seller_order_id,
        order_id,
        seller_id,
        buyer_id,
        gmv_usd,
        units,
        order_item_count
    FROM periodized
    WHERE gold_star + chs + direct + wpn > 0

    UNION ALL

    SELECT
        period_grain,
        period_start,
        game_category,
        'No Badge' AS badge_type,
        seller_order_id,
        order_id,
        seller_id,
        buyer_id,
        gmv_usd,
        units,
        order_item_count
    FROM periodized
    WHERE gold_star + chs + direct + wpn = 0
),

badge_gmv AS (
    SELECT
        period_grain,
        period_start,
        game_category,
        badge_type,

        SUM(gmv_usd) AS badge_gmv_usd,

        COUNT(DISTINCT seller_order_id) AS badge_seller_order_count,
        COUNT(DISTINCT order_id) AS badge_order_count,
        COUNT(DISTINCT seller_id) AS badge_seller_count,
        COUNT(DISTINCT buyer_id) AS badge_buyer_count,

        SUM(units) AS badge_units,
        SUM(order_item_count) AS badge_order_item_count

    FROM badge_fact

    GROUP BY
        period_grain,
        period_start,
        game_category,
        badge_type
),

badge_types AS (
    SELECT 'Gold Star' AS badge_type
    UNION ALL SELECT 'CHS'
    UNION ALL SELECT 'Direct'
    UNION ALL SELECT 'WPN'
    UNION ALL SELECT 'Any Badge'
    UNION ALL SELECT 'No Badge'
),

final_grid AS (
    SELECT
        mt.period_grain,
        mt.period_start,
        mt.period_end,
        mt.game_category,
        bt.badge_type,

        CASE
            WHEN bt.badge_type = 'WPN'
             AND mt.game_category <> 'MTG'
                THEN 0
            ELSE 1
        END AS is_badge_relevant_to_category,

        mt.total_marketplace_gmv_usd,
        mt.total_seller_order_count,
        mt.total_order_count,
        mt.total_seller_count,
        mt.total_buyer_count,
        mt.total_units,
        mt.total_order_item_count

    FROM marketplace_totals mt

    CROSS JOIN badge_types bt
)

SELECT
    fg.period_grain,
    fg.period_start,
    fg.period_end,
    fg.game_category,
    fg.badge_type,

    COALESCE(bg.badge_gmv_usd, 0) AS badge_gmv_usd,
    fg.total_marketplace_gmv_usd,

    ROUND(
        COALESCE(bg.badge_gmv_usd, 0)
        / NULLIF(fg.total_marketplace_gmv_usd, 0),
        6
    ) AS badge_gmv_share,

    ROUND(
        COALESCE(bg.badge_gmv_usd, 0)
        / NULLIF(fg.total_marketplace_gmv_usd, 0)
        * 100,
        2
    ) AS badge_gmv_share_pct,

    COALESCE(bg.badge_seller_order_count, 0) AS badge_seller_order_count,
    fg.total_seller_order_count,

    COALESCE(bg.badge_order_count, 0) AS badge_order_count,
    fg.total_order_count,

    COALESCE(bg.badge_seller_count, 0) AS badge_seller_count,
    fg.total_seller_count,

    COALESCE(bg.badge_buyer_count, 0) AS badge_buyer_count,
    fg.total_buyer_count,

    COALESCE(bg.badge_units, 0) AS badge_units,
    fg.total_units,

    COALESCE(bg.badge_order_item_count, 0) AS badge_order_item_count,
    fg.total_order_item_count

FROM final_grid fg

LEFT JOIN badge_gmv bg
    ON fg.period_grain = bg.period_grain
   AND fg.period_start = bg.period_start
   AND fg.game_category = bg.game_category
   AND fg.badge_type = bg.badge_type

WHERE fg.is_badge_relevant_to_category = 1

ORDER BY
    fg.period_grain,
    fg.period_start,
    fg.game_category,
    fg.badge_type;