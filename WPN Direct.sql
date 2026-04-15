
/*Inventory-Based SKU–Day–Seller Table*/
WITH wpn_sellers AS (
    SELECT DISTINCT
        s.id as seller_id,
        wpn.wpn_activation_date::date AS activated_date
    FROM ANALYTICS.SOURCES.SOURCE_GOOGLE_SHEETS__WPN_ACTIVATIONS as wpn
    left join ANALYTICS.CORE.SELLERS as s
on s.key = wpn.seller_key
),

inv_base AS (
    SELECT
        DATE_TRUNC('day', inv.date_et) AS order_date,
        inv.sku_id,
        inv.seller_id,
        inv.quantity AS inventory_qty,
        inv.price_usd AS price,
        s.is_direct
    FROM ANALYTICS.CORE.DAILY_SELLER_INVENTORY inv
    left join ANALYTICS.CORE.SELLERS as s
    on s.id = inv.seller_id
    WHERE inv.date_et BETWEEN '2025-06-02 00:00:00'
                              AND '2026-01-01 00:00:00'
      AND COALESCE(inv.quantity, 0) > 0
      AND inv.price_usd IS NOT NULL
      and channel = 'Marketplace'
      and product_line = 'Magic'
),

inv_wpn AS (
    SELECT
        b.*,
        CASE
            WHEN w.seller_id IS NOT NULL
                 AND (w.activated_date IS NULL OR b.order_date >= w.activated_date)
            THEN 1 ELSE 0
        END AS is_wpn
    FROM inv_base b
    LEFT JOIN wpn_sellers w
        ON b.seller_id = w.seller_id
),

ranked AS (
    SELECT
        order_date,
        sku_id,
        seller_id,
        is_wpn,
        is_direct,
        inventory_qty,
        price,

        /* Competitive positioning */
        RANK() OVER (
            PARTITION BY order_date, sku_id
            ORDER BY price ASC
        ) AS price_rank,

        MIN(price) OVER (
            PARTITION BY order_date, sku_id
        ) AS lowest_price,

        COUNT(*) OVER (
            PARTITION BY order_date, sku_id
        ) AS num_competitors

    FROM inv_wpn
)

SELECT
    order_date,
    sku_id,
    seller_id,

    is_wpn,
    is_direct,
    (is_wpn * IFF(is_direct, 1, 0)) AS wpn_x_direct,

    inventory_qty,
    price,
    price_rank,
    price - lowest_price AS price_gap_to_lowest,
    (price - lowest_price) / NULLIF(lowest_price,0) AS pct_gap_to_lowest,

    CASE WHEN price_rank = 1 THEN 1 ELSE 0 END AS lowest_price_flag,

    num_competitors

FROM ranked
WHERE num_competitors >= 2;

/*seller gmv share and orders within SKU–Day table*/
WITH orders_agg AS (
    SELECT
        DATE_TRUNC('day', oi.ordered_at_et) AS order_date,
        oi.product_condition_id as sku_id,
        so.seller_id,
        SUM(oi.total_usd) AS seller_gmv,
        SUM(oi.quantity) AS seller_units
    FROM ANALYTICS.CORE.ORDER_ITEMS oi
    left join ANALYTICS.CORE.SELLER_ORDERS as so 
    on oi.seller_order_id = so.id
    WHERE oi.ordered_at_et BETWEEN '2025-06-02 00:00:00'
                          AND '2026-01-01 00:00:00'
        and oi.product_line = 'Magic'
    GROUP BY 1,2,3
)

, sku_day_totals AS (
    SELECT
        order_date,
        sku_id,
        SUM(seller_gmv) AS total_sku_day_gmv,
        SUM(seller_units) AS total_sku_day_units
    FROM orders_agg
    GROUP BY 1,2
)

, sku_day_share AS (
    SELECT
        o.order_date,
        o.sku_id,
        o.seller_id,
        o.seller_gmv,
        o.seller_units,
        o.seller_gmv / NULLIF(t.total_sku_day_gmv,0) AS gmv_share,
        o.seller_units / NULLIF(t.total_sku_day_units,0) AS unit_share
    FROM orders_agg o
    JOIN sku_day_totals t
      ON o.order_date = t.order_date
     AND o.sku_id = t.sku_id
)

SELECT
    inv.*,
    COALESCE(s.gmv_share, 0) AS gmv_share,
    COALESCE(s.unit_share, 0) AS unit_share
FROM ANALYTICS.CORE.DAILY_SELLER_INVENTORY inv
LEFT JOIN sku_day_share s
  ON inv.order_date = s.order_date
 AND inv.sku_id = s.sku_id
 AND inv.seller_id = s.seller_id;

 /* -----------------------------------------------------------------------
COMBINED OUTPUT: Inventory-based SKU–day–seller competitive set
+ seller GMV / units + GMV share / unit share within SKU–day

Notes:
- Inventory competitive set is restricted to in-stock listings (quantity > 0)
- Orders are aggregated from ORDER_ITEMS; left-joined so non-selling listings get 0 GMV/units
- Uses item price only (no shipping in inventory table)
- Filters: channel='Marketplace', product_line='Magic'
----------------------------------------------------------------------- */

WITH wpn_sellers AS (
    SELECT DISTINCT
        s.id AS seller_id,
        wpn.wpn_activation_date::date AS activated_date
    FROM ANALYTICS.SOURCES.SOURCE_GOOGLE_SHEETS__WPN_ACTIVATIONS AS wpn
    LEFT JOIN ANALYTICS.CORE.SELLERS AS s
        ON s.key = wpn.seller_key
),

inv_base AS (
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
    WHERE inv.date_et BETWEEN '2025-06-02 00:00:00' AND '2026-01-01 00:00:00'
      AND COALESCE(inv.quantity, 0) > 0
      AND inv.price_usd IS NOT NULL
      AND inv.channel = 'Marketplace'
      AND inv.product_line = 'Magic'
),

inv_wpn AS (
    SELECT
        b.*,
        CASE
            WHEN w.seller_id IS NOT NULL
                 AND (w.activated_date IS NULL OR b.order_date >= w.activated_date)
            THEN 1 ELSE 0
        END AS is_wpn
    FROM inv_base b
    LEFT JOIN wpn_sellers w
        ON b.seller_id = w.seller_id
),

inv_ranked AS (
    SELECT
        order_date,
        sku_id,
        seller_id,
        is_wpn,
        is_direct,
        inventory_qty,
        price,

        RANK() OVER (
            PARTITION BY order_date, sku_id
            ORDER BY price ASC
        ) AS price_rank,

        MIN(price) OVER (
            PARTITION BY order_date, sku_id
        ) AS lowest_price,

        COUNT(*) OVER (
            PARTITION BY order_date, sku_id
        ) AS num_competitors
    FROM inv_wpn
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

sku_day_share AS (
    SELECT
        o.order_date,
        o.sku_id,
        o.seller_id,
        o.seller_gmv,
        o.seller_units,
        o.seller_gmv / NULLIF(t.total_sku_day_gmv, 0)     AS gmv_share,
        o.seller_units / NULLIF(t.total_sku_day_units, 0) AS unit_share
    FROM orders_agg o
    JOIN sku_day_totals t
      ON o.order_date = t.order_date
     AND o.sku_id = t.sku_id
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

    /* Outcomes from orders (0 if no sales for this SKU-day-seller) */
    COALESCE(s.seller_gmv, 0)   AS seller_gmv,
    COALESCE(s.seller_units, 0) AS seller_units,
    COALESCE(s.gmv_share, 0)    AS gmv_share,
    COALESCE(s.unit_share, 0)   AS unit_share

FROM inv_ranked i
LEFT JOIN sku_day_share s
  ON i.order_date = s.order_date
 AND i.sku_id     = s.sku_id
 AND i.seller_id  = s.seller_id

WHERE i.num_competitors >= 2;


/* Only WPN Sellers */

WITH wpn_sellers AS (
    SELECT DISTINCT
        s.id AS seller_id,
        wpn.wpn_activation_date::date AS activated_date
    FROM ANALYTICS.SOURCES.SOURCE_GOOGLE_SHEETS__WPN_ACTIVATIONS AS wpn
    LEFT JOIN ANALYTICS.CORE.SELLERS AS s
        ON s.key = wpn.seller_key
),

inv_base AS (
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
    WHERE inv.date_et BETWEEN '2025-06-02 00:00:00' AND '2026-01-01 00:00:00'
      AND COALESCE(inv.quantity, 0) > 0
      AND inv.price_usd IS NOT NULL
      AND inv.channel = 'Marketplace'
      AND inv.product_line = 'Magic'
),

/* Since final output keeps only is_wpn=1, filter to WPN here via INNER JOIN */
inv_wpn_only AS (
    SELECT
        b.*,
        1 AS is_wpn
    FROM inv_base b
    JOIN wpn_sellers w
      ON b.seller_id = w.seller_id
     AND (w.activated_date IS NULL OR b.order_date >= w.activated_date)
),

inv_ranked AS (
    SELECT
        order_date,
        sku_id,
        seller_id,
        is_wpn,
        is_direct,
        inventory_qty,
        price,
        RANK() OVER (PARTITION BY order_date, sku_id ORDER BY price ASC) AS price_rank,
        MIN(price)  OVER (PARTITION BY order_date, sku_id) AS lowest_price,
        COUNT(*)    OVER (PARTITION BY order_date, sku_id) AS num_competitors
    FROM inv_wpn_only
    QUALIFY COUNT(*) OVER (PARTITION BY order_date, sku_id) >= 2
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

sku_day_share AS (
    SELECT
        o.order_date,
        o.sku_id,
        o.seller_id,
        o.seller_gmv,
        o.seller_units,
        o.seller_gmv / NULLIF(t.total_sku_day_gmv, 0)     AS gmv_share,
        o.seller_units / NULLIF(t.total_sku_day_units, 0) AS unit_share
    FROM orders_agg o
    JOIN sku_day_totals t
      ON o.order_date = t.order_date
     AND o.sku_id = t.sku_id
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
    COALESCE(s.unit_share, 0)   AS unit_share
FROM inv_ranked i
LEFT JOIN sku_day_share s
  ON i.order_date = s.order_date
 AND i.sku_id     = s.sku_id
 AND i.seller_id  = s.seller_id;