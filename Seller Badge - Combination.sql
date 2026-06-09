
WITH wpn_sellers AS (
    SELECT
        LOWER(TRIM(SELLER_KEY)) AS seller_key,

        MIN(WPN_ACTIVATION_DATE) AS wpn_activation_date,

        MAX(WPN_DEACTIVATION_DATE) AS wpn_deactivation_date

    FROM ANALYTICS.SOURCES.SOURCE_GOOGLE_SHEETS__WPN_ACTIVATIONS

    WHERE WPN_ACTIVATION_DATE IS NOT NULL

    GROUP BY 1
),

seller_badges AS (
    SELECT
        s.ID AS seller_id,
        s.KEY AS seller_key,
        CURRENT_DATE() AS snapshot_date,

        COALESCE(NULLIF(TRIM(s.SELLER_SEGMENT), ''), 'Unknown') AS seller_segment,

        IFF(COALESCE(s.IS_GOLD_STAR, FALSE), 1, 0) AS gold_star,

        IFF(COALESCE(s.IS_CERTIFIED_HOBBY_SHOP, FALSE), 1, 0) AS chs,

        IFF(COALESCE(s.IS_DIRECT, FALSE), 1, 0) AS direct,

        IFF(
            w.seller_key IS NOT NULL
            AND w.wpn_activation_date <= CURRENT_DATE()
            AND (
                w.wpn_deactivation_date IS NULL
                OR w.wpn_deactivation_date > CURRENT_DATE()
            ),
            1,
            0
        ) AS wpn,

        w.wpn_activation_date,
        w.wpn_deactivation_date

    FROM ANALYTICS.CORE.SELLERS s

    LEFT JOIN wpn_sellers w
        ON LOWER(TRIM(s.KEY)) = w.seller_key

    WHERE COALESCE(s.IS_TEST_SELLER, FALSE) = FALSE
      AND COALESCE(s.IS_INTERNAL_SELLER, FALSE) = FALSE
      AND COALESCE(s.IS_EMPLOYEE, FALSE) = FALSE
),

badge_combo_base AS (
    SELECT
        seller_id,
        seller_key,
        snapshot_date,
        seller_segment,

        gold_star,
        chs,
        direct,
        wpn,

        ARRAY_CONSTRUCT_COMPACT(
            IFF(gold_star = 1, 'gold_star', NULL),
            IFF(chs = 1, 'chs', NULL),
            IFF(direct = 1, 'direct', NULL),
            IFF(wpn = 1, 'wpn', NULL)
        ) AS badge_array,

        wpn_activation_date,
        wpn_deactivation_date

    FROM seller_badges
),

seller_badge_combo AS (
    SELECT
        seller_id,
        seller_key,
        snapshot_date,
        seller_segment,

        gold_star,
        chs,
        direct,
        wpn,

        gold_star + chs + direct + wpn AS badge_count,

        IFF(
            ARRAY_SIZE(badge_array) = 0,
            'none',
            ARRAY_TO_STRING(badge_array, ' + ')
        ) AS badge_combo,

        wpn_activation_date,
        wpn_deactivation_date

    FROM badge_combo_base
),

combo_summary AS (
    SELECT
        badge_combo,

        COUNT(DISTINCT seller_id) AS badge_combo_seller_count,

        ROUND(
            COUNT(DISTINCT seller_id)
            / SUM(COUNT(DISTINCT seller_id)) OVER (),
            4
        ) AS badge_combo_pct_of_sellers

    FROM seller_badge_combo

    GROUP BY 1
),

segment_combo_summary AS (
    SELECT
        seller_segment,
        badge_combo,

        COUNT(DISTINCT seller_id) AS segment_badge_combo_seller_count,

        ROUND(
            COUNT(DISTINCT seller_id)
            / SUM(COUNT(DISTINCT seller_id)) OVER (
                PARTITION BY seller_segment
            ),
            4
        ) AS segment_badge_combo_pct_of_sellers

    FROM seller_badge_combo

    GROUP BY 1, 2
),

overall_summary AS (
    SELECT
        COUNT(DISTINCT seller_id) AS total_sellers,

        COUNT(DISTINCT IFF(gold_star = 1, seller_id, NULL)) AS gold_star_seller_count,
        COUNT(DISTINCT IFF(chs = 1, seller_id, NULL)) AS chs_seller_count,
        COUNT(DISTINCT IFF(direct = 1, seller_id, NULL)) AS direct_seller_count,
        COUNT(DISTINCT IFF(wpn = 1, seller_id, NULL)) AS wpn_seller_count,

        ROUND(
            COUNT(DISTINCT IFF(gold_star = 1, seller_id, NULL))
            / COUNT(DISTINCT seller_id),
            4
        ) AS gold_star_pct_of_sellers,

        ROUND(
            COUNT(DISTINCT IFF(chs = 1, seller_id, NULL))
            / COUNT(DISTINCT seller_id),
            4
        ) AS chs_pct_of_sellers,

        ROUND(
            COUNT(DISTINCT IFF(direct = 1, seller_id, NULL))
            / COUNT(DISTINCT seller_id),
            4
        ) AS direct_pct_of_sellers,

        ROUND(
            COUNT(DISTINCT IFF(wpn = 1, seller_id, NULL))
            / COUNT(DISTINCT seller_id),
            4
        ) AS wpn_pct_of_sellers

    FROM seller_badge_combo
)

SELECT
    b.seller_id,
    b.seller_key,
    b.seller_segment,

    b.gold_star,
    b.chs,
    b.direct,
    b.wpn,

    b.badge_count,
    b.badge_combo,

    c.badge_combo_seller_count,
    c.badge_combo_pct_of_sellers,

    sc.segment_badge_combo_seller_count,
    sc.segment_badge_combo_pct_of_sellers,

    o.total_sellers,

    o.gold_star_seller_count,
    o.chs_seller_count,
    o.direct_seller_count,
    o.wpn_seller_count,

    o.gold_star_pct_of_sellers,
    o.chs_pct_of_sellers,
    o.direct_pct_of_sellers,
    o.wpn_pct_of_sellers

FROM seller_badge_combo b

LEFT JOIN combo_summary c
    ON b.badge_combo = c.badge_combo

LEFT JOIN segment_combo_summary sc
    ON b.seller_segment = sc.seller_segment
   AND b.badge_combo = sc.badge_combo

CROSS JOIN overall_summary o

ORDER BY
    b.seller_segment,
    b.badge_combo,
    b.seller_id;