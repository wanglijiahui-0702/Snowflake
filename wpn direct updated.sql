WITH wpn_sellers AS (
    SELECT DISTINCT
        s.id AS seller_id,
        wpn.wpn_activation_date::date AS activated_date
    FROM ANALYTICS.SOURCES.SOURCE_GOOGLE_SHEETS__WPN_ACTIVATIONS AS wpn
    LEFT JOIN ANALYTICS.CORE.SELLERS AS s
        ON s.key = wpn.seller_key
),

/* 1) Orders aggregated first (smaller) */
orders_agg AS (
    SELECT
        DATE_TRUNC('day', oi.ordered_at_et) AS order_date,
        oi.product_condition_id AS sku_id,
        so.seller_id,
        SUM(oi.total_usd) AS seller_gmv,
        SUM(oi.quantity)  AS seller_units
    FROM ANALYTICS.CORE.ORDER_ITEMS oi
    LEFT JOIN ANALYTICS.CORE.SELLER_ORDERS so
        ON oi.seller_order_id = so.id
    WHERE oi.ordered_at_et BETWEEN '2025-06-02 00:00:00' AND '2026-01-01 00:00:00'
      AND oi.product_line = 'Magic'
    GROUP BY 1,2,3
),

sku_day_totals AS (
    SELECT
        order_date,
        sku_id,
        SUM(seller_gmv)   AS total_sku_day_gmv,
        SUM(seller_units) AS total_sku_day_units
    FROM orders_agg
    GROUP BY 1,2
),

/* 2) Keep only SKU-days with demand */
sku_days_with_demand AS (
    SELECT order_date, sku_id, total_sku_day_gmv, total_sku_day_units
    FROM sku_day_totals
    WHERE total_sku_day_gmv > 0
),

/* 3) Inventory filtered to demand SKU-days + dedup to 1 row per SKU-day-seller */
inv_dedup AS (
    SELECT
        DATE_TRUNC('day', inv.date_et) AS order_date,
        inv.sku_id,
        inv.seller_id,
        inv.quantity AS inventory_qty,
        inv.price_usd AS price,
        s.is_direct
    FROM ANALYTICS.CORE.DAILY_SELLER_INVENTORY inv
    LEFT JOIN ANALYTICS.CORE.SELLERS AS s
        ON s.id = inv.seller_id
    /* join early to reduce inventory scanned */
    JOIN sku_days_with_demand d
      ON DATE_TRUNC('day', inv.date_et) = d.order_date
     AND inv.sku_id = d.sku_id
    WHERE inv.date_et BETWEEN '2025-06-02 00:00:00' AND '2026-01-01 00:00:00'
      AND COALESCE(inv.quantity, 0) > 0
      AND inv.price_usd IS NOT NULL
      AND inv.channel = 'Marketplace'
      AND inv.product_line = 'Magic'
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY DATE_TRUNC('day', inv.date_et), inv.sku_id, inv.seller_id
        ORDER BY inv.quantity DESC, inv.price_usd ASC
    ) = 1
),

/* 4) Add WPN flag */
inv_wpn AS (
    SELECT
        b.*,
        CASE
            WHEN w.seller_id IS NOT NULL
                 AND (w.activated_date IS NULL OR b.order_date >= w.activated_date)
            THEN 1 ELSE 0
        END AS is_wpn
    FROM inv_dedup b
    LEFT JOIN wpn_sellers w
        ON b.seller_id = w.seller_id
),

/* 5) Rank only within reduced competitive set */
inv_ranked AS (
    SELECT
        order_date,
        sku_id,
        seller_id,
        is_wpn,
        is_direct,
        inventory_qty,
        price,

        ROW_NUMBER() OVER (
            PARTITION BY order_date, sku_id
            ORDER BY price ASC
        ) AS price_rank,

        MIN(price) OVER (PARTITION BY order_date, sku_id) AS lowest_price,
        COUNT(*) OVER (PARTITION BY order_date, sku_id) AS num_competitors

    FROM inv_wpn
    /* Optional: keep only top N cheapest competitors to cut size further */
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY order_date, sku_id
        ORDER BY price ASC
    ) <= 50
),

/* 6) Shares */
sku_day_share AS (
    SELECT
        o.order_date,
        o.sku_id,
        o.seller_id,
        o.seller_gmv,
        o.seller_units,
        o.seller_gmv / NULLIF(d.total_sku_day_gmv, 0)     AS gmv_share,
        o.seller_units / NULLIF(d.total_sku_day_units, 0) AS unit_share
    FROM orders_agg o
    JOIN sku_days_with_demand d
      ON o.order_date = d.order_date
     AND o.sku_id = d.sku_id
)

SELECT
    i.order_date,
    i.sku_id,
    i.seller_id,

    i.is_wpn,
    i.is_direct,
    (i.is_wpn * IFF(i.is_direct, 1, 0)) AS wpn_x_direct,

    i.inventory_qty,
    i.price,
    i.price_rank,
    i.price - i.lowest_price AS price_gap_to_lowest,
    (i.price - i.lowest_price) / NULLIF(i.lowest_price, 0) AS pct_gap_to_lowest,
    CASE WHEN i.price_rank = 1 THEN 1 ELSE 0 END AS lowest_price_flag,
    i.num_competitors,

    COALESCE(s.seller_gmv, 0)   AS seller_gmv,
    COALESCE(s.seller_units, 0) AS seller_units,
    COALESCE(s.gmv_share, 0)    AS gmv_share,
    COALESCE(s.unit_share, 0)   AS unit_share,

    d.total_sku_day_gmv,
    d.total_sku_day_units

FROM inv_ranked i
LEFT JOIN sku_day_share s
  ON i.order_date = s.order_date
 AND i.sku_id     = s.sku_id
 AND i.seller_id  = s.seller_id
JOIN sku_days_with_demand d
  ON i.order_date = d.order_date
 AND i.sku_id     = d.sku_id

WHERE i.num_competitors >= 2;



WITH wpn_sellers AS (
    SELECT DISTINCT
        s.id AS seller_id,
        wpn.wpn_activation_date::date AS activated_date
    FROM ANALYTICS.SOURCES.SOURCE_GOOGLE_SHEETS__WPN_ACTIVATIONS AS wpn
    LEFT JOIN ANALYTICS.CORE.SELLERS AS s
        ON s.key = wpn.seller_key
),

orders_agg AS (
    SELECT
        DATE_TRUNC('day', oi.ordered_at_et) AS order_date,
        oi.product_condition_id AS sku_id,
        so.seller_id,
        SUM(oi.total_usd) AS seller_gmv,
        SUM(oi.quantity)  AS seller_units
    FROM ANALYTICS.CORE.ORDER_ITEMS oi
    LEFT JOIN ANALYTICS.CORE.SELLER_ORDERS so
        ON oi.seller_order_id = so.id
    WHERE oi.ordered_at_et BETWEEN '2025-06-02 00:00:00' AND '2026-01-01 00:00:00'
      AND oi.product_line = 'Magic'
    GROUP BY 1,2,3
),

sku_day_totals AS (
    SELECT
        order_date,
        sku_id,
        SUM(seller_gmv)   AS total_sku_day_gmv,
        SUM(seller_units) AS total_sku_day_units
    FROM orders_agg
    GROUP BY 1,2
),

/* 1) demand filter: set threshold here */
sku_days_with_demand AS (
    SELECT *
    FROM sku_day_totals
    WHERE total_sku_day_gmv >= 50   -- <<< tune: 25/50/100
),

/* 2) inventory filtered to demand sku-days + dedup to 1 row per SKU-day-seller */
inv_dedup AS (
    SELECT
        DATE_TRUNC('day', inv.date_et) AS order_date,
        inv.sku_id,
        inv.seller_id,
        inv.quantity AS inventory_qty,
        inv.price_usd AS price,
        s.is_direct
    FROM ANALYTICS.CORE.DAILY_SELLER_INVENTORY inv
    LEFT JOIN ANALYTICS.CORE.SELLERS AS s
        ON s.id = inv.seller_id
    JOIN sku_days_with_demand d
      ON DATE_TRUNC('day', inv.date_et) = d.order_date
     AND inv.sku_id = d.sku_id
    WHERE inv.date_et BETWEEN '2025-06-02 00:00:00' AND '2026-01-01 00:00:00'
      AND COALESCE(inv.quantity, 0) > 0
      AND inv.price_usd IS NOT NULL
      AND inv.channel = 'Marketplace'
      AND inv.product_line = 'Magic'
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY DATE_TRUNC('day', inv.date_et), inv.sku_id, inv.seller_id
        ORDER BY inv.quantity DESC, inv.price_usd ASC
    ) = 1
),

inv_wpn AS (
    SELECT
        b.*,
        CASE
            WHEN w.seller_id IS NOT NULL
                 AND (w.activated_date IS NULL OR b.order_date >= w.activated_date)
            THEN 1 ELSE 0
        END AS is_wpn
    FROM inv_dedup b
    LEFT JOIN wpn_sellers w
        ON b.seller_id = w.seller_id
),

/* 3) cap competitors: top N cheapest per SKU-day */
inv_ranked AS (
    SELECT
        order_date,
        sku_id,
        seller_id,
        is_wpn,
        is_direct,
        inventory_qty,
        price,

        ROW_NUMBER() OVER (
            PARTITION BY order_date, sku_id
            ORDER BY price ASC
        ) AS price_rank,

        MIN(price) OVER (PARTITION BY order_date, sku_id) AS lowest_price,
        COUNT(*) OVER (PARTITION BY order_date, sku_id)   AS num_competitors

    FROM inv_wpn
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY order_date, sku_id
        ORDER BY price ASC
    ) <= 50   -- <<< tune: 20 for quick iteration, 50 for final
),

sku_day_share AS (
    SELECT
        o.order_date,
        o.sku_id,
        o.seller_id,
        o.seller_gmv,
        o.seller_units,
        o.seller_gmv / NULLIF(d.total_sku_day_gmv, 0)     AS gmv_share,
        o.seller_units / NULLIF(d.total_sku_day_units, 0) AS unit_share
    FROM orders_agg o
    JOIN sku_days_with_demand d
      ON o.order_date = d.order_date
     AND o.sku_id     = d.sku_id
)

SELECT
    i.order_date,
    i.sku_id,
    i.seller_id,
    i.is_wpn,
    i.is_direct,
    (i.is_wpn * IFF(i.is_direct, 1, 0)) AS wpn_x_direct,

    i.inventory_qty,
    i.price,
    i.price_rank,
    i.price - i.lowest_price AS price_gap_to_lowest,
    (i.price - i.lowest_price) / NULLIF(i.lowest_price, 0) AS pct_gap_to_lowest,
    CASE WHEN i.price_rank = 1 THEN 1 ELSE 0 END AS lowest_price_flag,
    i.num_competitors,

    COALESCE(s.seller_gmv, 0) AS seller_gmv,
    COALESCE(s.seller_units, 0) AS seller_units,
    COALESCE(s.gmv_share, 0) AS gmv_share,
    COALESCE(s.unit_share, 0) AS unit_share,

    d.total_sku_day_gmv,
    d.total_sku_day_units

FROM inv_ranked i
LEFT JOIN sku_day_share s
  ON i.order_date = s.order_date
 AND i.sku_id     = s.sku_id
 AND i.seller_id  = s.seller_id
JOIN sku_days_with_demand d
  ON i.order_date = d.order_date
 AND i.sku_id     = d.sku_id
WHERE i.num_competitors >= 2;