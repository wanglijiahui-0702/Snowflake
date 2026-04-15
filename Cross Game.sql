select * from ANALYTICS.CORE.ORDER_ITEMS limit 100
select * from ANALYTICS.CORE.ORDERS limit 100

select product_type, count (id) from ANALYTICS.CORE.ORDER_ITEMS
group by 1
order by 2 desc

select product_type, product_line, count (id) from ANALYTICS.CORE.ORDER_ITEMS
group by 1, 2
order by 3 desc
--------------------------------------------------
WITH base_txns AS (
    SELECT
        o.buyer_id,
        oi.quantity as order_id,
        oi.product_line,
        oi.product_type,
        oi.ordered_at_et as order_date,
        oi.total_usd as gmv
    FROM ANALYTICS.CORE.ORDER_ITEMS as oi 
    inner join ANALYTICS.CORE.ORDERS as o
    on o.id = oi.order_id
    WHERE o.is_complete = 'True' 

),
-- buyer × product line metrics
buyer_product_metrics AS (
    SELECT
        buyer_id,
        product_line,
        SUM(gmv) AS line_gmv,
        COUNT(DISTINCT order_id) AS line_orders
    FROM base_txns
    GROUP BY buyer_id, product_line
),

-- totals per buyer
buyer_totals AS (
    SELECT
        buyer_id,
        SUM(line_gmv) AS total_gmv,
        SUM(line_orders) AS total_orders
    FROM buyer_product_metrics
    GROUP BY buyer_id
),

-- rank product lines per buyer (dominant/secondary/third by GMV)
ranked_lines AS (
    SELECT
        m.*,
        ROW_NUMBER() OVER (
            PARTITION BY buyer_id
            ORDER BY line_gmv DESC, line_orders DESC, product_line
        ) AS line_rank
    FROM buyer_product_metrics m
)

SELECT
    t.buyer_id,

    -- overall totals
    t.total_gmv,
    t.total_orders,

    /* -------------------------
       Rank 1 (Dominant)
    --------------------------*/
    MAX(IFF(r.line_rank = 1, r.product_line, NULL)) AS dominant_product_line,
    MAX(IFF(r.line_rank = 1, r.line_gmv, NULL))      AS dominant_gmv,
    MAX(IFF(r.line_rank = 1, r.line_orders, NULL))   AS dominant_orders,
    ROUND(MAX(IFF(r.line_rank = 1, r.line_gmv, NULL))    / NULLIF(t.total_gmv, 0), 4) AS dominant_gmv_pct,
    ROUND(MAX(IFF(r.line_rank = 1, r.line_orders, NULL)) / NULLIF(t.total_orders, 0), 4) AS dominant_orders_pct,

    /* -------------------------
       Rank 2 (Secondary)
    --------------------------*/
    MAX(IFF(r.line_rank = 2, r.product_line, NULL)) AS secondary_product_line,
    MAX(IFF(r.line_rank = 2, r.line_gmv, NULL))      AS secondary_gmv,
    MAX(IFF(r.line_rank = 2, r.line_orders, NULL))   AS secondary_orders,
    ROUND(MAX(IFF(r.line_rank = 2, r.line_gmv, NULL))    / NULLIF(t.total_gmv, 0), 4) AS secondary_gmv_pct,
    ROUND(MAX(IFF(r.line_rank = 2, r.line_orders, NULL)) / NULLIF(t.total_orders, 0), 4) AS secondary_orders_pct,

    /* -------------------------
       Rank 3 (Third)
    --------------------------*/
    MAX(IFF(r.line_rank = 3, r.product_line, NULL)) AS third_product_line,
    MAX(IFF(r.line_rank = 3, r.line_gmv, NULL))      AS third_gmv,
    MAX(IFF(r.line_rank = 3, r.line_orders, NULL))   AS third_orders,
    ROUND(MAX(IFF(r.line_rank = 3, r.line_gmv, NULL))    / NULLIF(t.total_gmv, 0), 4) AS third_gmv_pct,
    ROUND(MAX(IFF(r.line_rank = 3, r.line_orders, NULL)) / NULLIF(t.total_orders, 0), 4) AS third_orders_pct,

    /* -------------------------
       Rest (Rank 4+ aggregated)
    --------------------------*/
    SUM(IFF(r.line_rank >= 4, r.line_gmv, 0))    AS rest_gmv,
    SUM(IFF(r.line_rank >= 4, r.line_orders, 0)) AS rest_orders,
    ROUND(SUM(IFF(r.line_rank >= 4, r.line_gmv, 0))    / NULLIF(t.total_gmv, 0), 4) AS rest_gmv_pct,
    ROUND(SUM(IFF(r.line_rank >= 4, r.line_orders, 0)) / NULLIF(t.total_orders, 0), 4) AS rest_orders_pct,

    -- optional: how many distinct product lines buyer purchased
    COUNT_IF(r.product_line IS NOT NULL) AS distinct_product_lines

FROM buyer_totals t
JOIN ranked_lines r
  ON t.buyer_id = r.buyer_id
GROUP BY
    t.buyer_id,
    t.total_gmv,
    t.total_orders
ORDER BY
    t.total_gmv DESC;


    
SELECT
    o.buyer_id,
    o.id AS order_id,
    oi.product_line,
    oi.product_type AS raw_product_type,

    CASE
        WHEN oi.product_type ILIKE '%SINGLES%' THEN 'Singles'
        WHEN oi.product_type ILIKE '%SEALED%'  THEN 'Sealed'
        WHEN oi.product_type ILIKE '%Supplies%'THEN 'Supplies'
        WHEN oi.product_type in ('Cards', 'Card')     THEN 'Singles'
        ELSE 'Other'
    END AS product_type,

    oi.ordered_at_et AS order_date,
    oi.total_usd AS gmv
FROM ANALYTICS.CORE.ORDER_ITEMS oi
JOIN ANALYTICS.CORE.ORDERS o
  ON o.id = oi.order_id
WHERE o.is_complete = 'True'



/* full scripts in Tableau */
WITH base_txns_raw AS (
    SELECT
        o.buyer_id,
        o.id AS order_id,                         -- ✅ FIX
        oi.product_line,
        oi.product_type AS raw_product_type,
        CASE
            WHEN oi.product_type ILIKE '%SINGLES%'  THEN 'Singles'
            WHEN oi.product_type ILIKE '%SEALED%'   THEN 'Sealed'
            WHEN oi.product_type ILIKE '%SUPPLIES%' THEN 'Supplies'
            WHEN oi.product_type IN ('Cards','Card') THEN 'Singles'
            ELSE 'Other'
        END AS product_type_group,
        oi.ordered_at_et AS order_date,
        oi.total_usd AS gmv
    FROM ANALYTICS.CORE.ORDER_ITEMS oi
    JOIN ANALYTICS.CORE.ORDERS o
      ON o.id = oi.order_id
    WHERE o.is_complete = 'True'
      AND oi.ordered_at_et >= <Parameters.p_start_date>
      AND oi.ordered_at_et <  <Parameters.p_end_date>
),

base_txns AS (
    SELECT *
    FROM base_txns_raw
    WHERE
      (
        <Parameters.p_product_type> = 'All'
        OR POSITION(',' || product_type_group || ',' IN ',' || <Parameters.p_product_type> || ',') > 0
      )
),
-- buyer × product line metrics
buyer_product_metrics AS (
    SELECT
        buyer_id,
        product_line,
        SUM(gmv) AS line_gmv,
        COUNT(DISTINCT order_id) AS line_orders
    FROM base_txns
    GROUP BY buyer_id, product_line
),

-- totals per buyer
buyer_totals AS (
    SELECT
        buyer_id,
        SUM(line_gmv) AS total_gmv,
        SUM(line_orders) AS total_orders
    FROM buyer_product_metrics
    GROUP BY buyer_id
),

-- rank product lines per buyer (dominant/secondary/third by GMV)
ranked_lines AS (
    SELECT
        m.*,
        ROW_NUMBER() OVER (
            PARTITION BY buyer_id
            ORDER BY  line_orders DESC, line_gmv DESC, product_line
        ) AS line_rank
    FROM buyer_product_metrics m
)

SELECT
    t.buyer_id,

    -- overall totals
    t.total_gmv,
    t.total_orders,

    /* -------------------------
       Rank 1 (Dominant)
    --------------------------*/
    MAX(IFF(r.line_rank = 1, r.product_line, NULL)) AS dominant_product_line,
    MAX(IFF(r.line_rank = 1, r.line_gmv, NULL))      AS dominant_gmv,
    MAX(IFF(r.line_rank = 1, r.line_orders, NULL))   AS dominant_orders,
    ROUND(MAX(IFF(r.line_rank = 1, r.line_gmv, NULL))    / NULLIF(t.total_gmv, 0), 4) AS dominant_gmv_pct,
    ROUND(MAX(IFF(r.line_rank = 1, r.line_orders, NULL)) / NULLIF(t.total_orders, 0), 4) AS dominant_orders_pct,

    /* -------------------------
       Rank 2 (Secondary)
    --------------------------*/
    MAX(IFF(r.line_rank = 2, r.product_line, NULL)) AS secondary_product_line,
    MAX(IFF(r.line_rank = 2, r.line_gmv, NULL))      AS secondary_gmv,
    MAX(IFF(r.line_rank = 2, r.line_orders, NULL))   AS secondary_orders,
    ROUND(MAX(IFF(r.line_rank = 2, r.line_gmv, NULL))    / NULLIF(t.total_gmv, 0), 4) AS secondary_gmv_pct,
    ROUND(MAX(IFF(r.line_rank = 2, r.line_orders, NULL)) / NULLIF(t.total_orders, 0), 4) AS secondary_orders_pct,

    /* -------------------------
       Rank 3 (Third)
    --------------------------*/
    MAX(IFF(r.line_rank = 3, r.product_line, NULL)) AS third_product_line,
    MAX(IFF(r.line_rank = 3, r.line_gmv, NULL))      AS third_gmv,
    MAX(IFF(r.line_rank = 3, r.line_orders, NULL))   AS third_orders,
    ROUND(MAX(IFF(r.line_rank = 3, r.line_gmv, NULL))    / NULLIF(t.total_gmv, 0), 4) AS third_gmv_pct,
    ROUND(MAX(IFF(r.line_rank = 3, r.line_orders, NULL)) / NULLIF(t.total_orders, 0), 4) AS third_orders_pct,

    /* -------------------------
       Rest (Rank 4+ aggregated)
    --------------------------*/
    SUM(IFF(r.line_rank >= 4, r.line_gmv, 0))    AS rest_gmv,
    SUM(IFF(r.line_rank >= 4, r.line_orders, 0)) AS rest_orders,
    ROUND(SUM(IFF(r.line_rank >= 4, r.line_gmv, 0))    / NULLIF(t.total_gmv, 0), 4) AS rest_gmv_pct,
    ROUND(SUM(IFF(r.line_rank >= 4, r.line_orders, 0)) / NULLIF(t.total_orders, 0), 4) AS rest_orders_pct,

    -- optional: how many distinct product lines buyer purchased
    COUNT_IF(r.product_line IS NOT NULL) AS distinct_product_lines

FROM buyer_totals t
JOIN ranked_lines r
  ON t.buyer_id = r.buyer_id
GROUP BY
    t.buyer_id,
    t.total_gmv,
    t.total_orders
ORDER BY
    t.total_gmv DESC