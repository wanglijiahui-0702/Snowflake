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

seller_order_base AS (
    SELECT
        DATE_TRUNC('month', so.ORDERED_AT_ET)::DATE AS order_month,

        so.ID AS seller_order_id,
        so.ORDER_ID,
        so.SELLER_ID AS seller_id,
        so.BUYER_ID AS buyer_id,
        so.ORDERED_AT_ET,

        COALESCE(NULLIF(TRIM(s.SELLER_SEGMENT), ''), 'Unknown') AS seller_segment,

        COALESCE(
            so.NET_ORDER_SALES_USD,
            so.ORDER_AMOUNT_USD,
            0
        ) AS gmv_usd,

        IFF(COALESCE(s.IS_GOLD_STAR, FALSE), 1, 0) AS gold_star,

        IFF(COALESCE(s.IS_CERTIFIED_HOBBY_SHOP, FALSE), 1, 0) AS chs,

        IFF(
            s.JOINED_DIRECT_AT_ET IS NOT NULL
            AND s.JOINED_DIRECT_AT_ET <= so.ORDERED_AT_ET
            AND (
                s.OPTED_OUT_DIRECT_AT_ET IS NULL
                OR s.OPTED_OUT_DIRECT_AT_ET > so.ORDERED_AT_ET
            ),
            1,
            0
        ) AS direct,

        IFF(
            w.seller_key IS NOT NULL
            AND w.wpn_activation_date <= CAST(so.ORDERED_AT_ET AS DATE)
            AND (
                w.wpn_deactivation_date IS NULL
                OR w.wpn_deactivation_date > CAST(so.ORDERED_AT_ET AS DATE)
            ),
            1,
            0
        ) AS wpn

    FROM ANALYTICS.CORE.SELLER_ORDERS so

    INNER JOIN ANALYTICS.CORE.SELLERS s
        ON s.ID = so.SELLER_ID

    LEFT JOIN wpn_sellers w
        ON LOWER(LPAD(TRIM(s.KEY), 8, '0')) = w.seller_key

    INNER JOIN params p
        ON CAST(so.ORDERED_AT_ET AS DATE) >= p.start_date

    WHERE COALESCE(so.IS_COMPLETE, FALSE) = TRUE
      AND COALESCE(so.IS_MARKETPLACE_ORDER, FALSE) = TRUE
      AND COALESCE(so.IS_INTERNAL_SELLER, FALSE) = FALSE
      AND COALESCE(so.HAS_BUYER_PAID, TRUE) = TRUE

      AND COALESCE(s.IS_TEST_SELLER, FALSE) = FALSE
      AND COALESCE(s.IS_INTERNAL_SELLER, FALSE) = FALSE
      AND COALESCE(s.IS_EMPLOYEE, FALSE) = FALSE
),

seller_month_gmv AS (
    SELECT
        order_month,
        seller_segment,
        seller_id,

        SUM(gmv_usd) AS monthly_gmv_usd,

        COUNT(DISTINCT seller_order_id) AS monthly_seller_order_count,
        COUNT(DISTINCT order_id) AS monthly_order_count,
        COUNT(DISTINCT buyer_id) AS monthly_buyer_count,

        MAX(gold_star) AS gold_star,
        MAX(chs) AS chs,
        MAX(direct) AS direct,
        MAX(wpn) AS wpn

    FROM seller_order_base

    GROUP BY
        order_month,
        seller_segment,
        seller_id
),

badge_seller_month AS (
    SELECT
        order_month,
        seller_segment,
        seller_id,
        monthly_gmv_usd,
        monthly_seller_order_count,
        monthly_order_count,
        monthly_buyer_count,
        'Gold Star' AS badge_type,
        gold_star AS has_badge
    FROM seller_month_gmv

    UNION ALL

    SELECT
        order_month,
        seller_segment,
        seller_id,
        monthly_gmv_usd,
        monthly_seller_order_count,
        monthly_order_count,
        monthly_buyer_count,
        'CHS' AS badge_type,
        chs AS has_badge
    FROM seller_month_gmv

    UNION ALL

    SELECT
        order_month,
        seller_segment,
        seller_id,
        monthly_gmv_usd,
        monthly_seller_order_count,
        monthly_order_count,
        monthly_buyer_count,
        'Direct' AS badge_type,
        direct AS has_badge
    FROM seller_month_gmv

    UNION ALL

    SELECT
        order_month,
        seller_segment,
        seller_id,
        monthly_gmv_usd,
        monthly_seller_order_count,
        monthly_order_count,
        monthly_buyer_count,
        'WPN' AS badge_type,
        wpn AS has_badge
    FROM seller_month_gmv
),

monthly_segment_badge_compare AS (
    SELECT
        order_month,
        seller_segment,
        badge_type,

        COUNT(DISTINCT seller_id) AS total_active_sellers_in_segment_month,

        COUNT(DISTINCT IFF(has_badge = 1, seller_id, NULL)) AS with_badge_seller_count,
        COUNT(DISTINCT IFF(has_badge = 0, seller_id, NULL)) AS without_badge_seller_count,

        SUM(IFF(has_badge = 1, monthly_gmv_usd, 0)) AS with_badge_gmv_usd,
        SUM(IFF(has_badge = 0, monthly_gmv_usd, 0)) AS without_badge_gmv_usd,

        SUM(monthly_gmv_usd) AS total_segment_month_gmv_usd,

        AVG(IFF(has_badge = 1, monthly_gmv_usd, NULL)) AS with_badge_avg_gmv_per_seller,
        AVG(IFF(has_badge = 0, monthly_gmv_usd, NULL)) AS without_badge_avg_gmv_per_seller,

        SUM(IFF(has_badge = 1, monthly_seller_order_count, 0)) AS with_badge_seller_order_count,
        SUM(IFF(has_badge = 0, monthly_seller_order_count, 0)) AS without_badge_seller_order_count,

        AVG(IFF(has_badge = 1, monthly_seller_order_count, NULL)) AS with_badge_avg_seller_orders_per_seller,
        AVG(IFF(has_badge = 0, monthly_seller_order_count, NULL)) AS without_badge_avg_seller_orders_per_seller,

        SUM(IFF(has_badge = 1, monthly_buyer_count, 0)) AS with_badge_buyer_count,
        SUM(IFF(has_badge = 0, monthly_buyer_count, 0)) AS without_badge_buyer_count

    FROM badge_seller_month

    GROUP BY
        order_month,
        seller_segment,
        badge_type
)

SELECT
    order_month,
    seller_segment,
    badge_type,

    total_active_sellers_in_segment_month,

    with_badge_seller_count,
    without_badge_seller_count,

    ROUND(with_badge_gmv_usd, 2) AS with_badge_gmv_usd,
    ROUND(without_badge_gmv_usd, 2) AS without_badge_gmv_usd,
    ROUND(total_segment_month_gmv_usd, 2) AS total_segment_month_gmv_usd,

    ROUND(
        with_badge_gmv_usd / NULLIF(total_segment_month_gmv_usd, 0),
        6
    ) AS with_badge_gmv_share,

    ROUND(
        with_badge_gmv_usd / NULLIF(total_segment_month_gmv_usd, 0) * 100,
        2
    ) AS with_badge_gmv_share_pct,

    ROUND(with_badge_avg_gmv_per_seller, 2) AS with_badge_avg_gmv_per_seller,
    ROUND(without_badge_avg_gmv_per_seller, 2) AS without_badge_avg_gmv_per_seller,

    ROUND(
        with_badge_avg_gmv_per_seller
        - without_badge_avg_gmv_per_seller,
        2
    ) AS avg_gmv_per_seller_difference,

    ROUND(
        (
            with_badge_avg_gmv_per_seller
            / NULLIF(without_badge_avg_gmv_per_seller, 0)
            - 1
        ) * 100,
        2
    ) AS avg_gmv_per_seller_lift_pct,

    with_badge_seller_order_count,
    without_badge_seller_order_count,

    ROUND(with_badge_avg_seller_orders_per_seller, 2) AS with_badge_avg_orders_per_seller,
    ROUND(without_badge_avg_seller_orders_per_seller, 2) AS without_badge_avg_orders_per_seller,

    with_badge_buyer_count,
    without_badge_buyer_count

FROM monthly_segment_badge_compare

ORDER BY
    order_month,
    seller_segment,
    badge_type;