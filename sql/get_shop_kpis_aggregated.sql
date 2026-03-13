CREATE OR REPLACE FUNCTION get_shop_kpis_aggregated(
  p_start    date,
  p_end      date,
  p_stores   text[]  DEFAULT NULL,
  p_is_short boolean DEFAULT false
)
RETURNS TABLE (
  store             text,
  total_revenue     numeric,
  total_kg          numeric,
  total_pcs         numeric,
  total_checks      bigint,
  second_revenue    numeric,
  second_kg         numeric,
  second_pcs        numeric,
  second_checks     bigint,
  aplus_revenue     numeric,
  aplus_kg          numeric,
  aplus_checks      bigint,
  bedding_revenue   numeric,
  bedding_checks    bigint,
  prev_revenue      numeric,
  prev_week_revenue numeric,
  prev_second_kg      numeric,
  prev_week_second_kg numeric
)
LANGUAGE sql
STABLE
AS $$
  WITH periods AS (
    SELECT
      s.store,
      s.revenue,
      s.quantity_pcs,
      s.recorder_id,
      (s.sale_date BETWEEN p_start AND p_end)                                                    AS is_current,
      (s.sale_date BETWEEN (p_start - INTERVAL '1 month')::date AND (p_end - INTERVAL '1 month')::date) AS is_prev,
      (p_is_short AND s.sale_date BETWEEN (p_start - INTERVAL '7 days')::date AND (p_end - INTERVAL '7 days')::date) AS is_prev_week,
      (s.product LIKE '%A+%' OR s.product LIKE '%А+%')                                           AS is_aplus,
      COALESCE(c.category, 'second')                                                              AS category,
      CASE WHEN COALESCE(c.category,'second') = 'second' THEN
        CASE WHEN c.avg_weight_kg > 0 THEN s.quantity_pcs * c.avg_weight_kg ELSE s.quantity_kg END
      ELSE 0 END                                                                                  AS row_kg,
      split_part(COALESCE(s.recorder_id,''), '_', 1)                                             AS check_id
    FROM sales_analytics s
    LEFT JOIN product_classifications c
      ON c.product_group = s.product_group AND c.product = s.product
    WHERE s.store IS NOT NULL
      AND (p_stores IS NULL OR s.store = ANY(p_stores))
      AND (
        s.sale_date BETWEEN p_start AND p_end
        OR s.sale_date BETWEEN (p_start - INTERVAL '1 month')::date AND (p_end - INTERVAL '1 month')::date
        OR (p_is_short AND s.sale_date BETWEEN (p_start - INTERVAL '7 days')::date AND (p_end - INTERVAL '7 days')::date)
      )
  )
  SELECT
    p.store,
    SUM(CASE WHEN is_current THEN revenue ELSE 0 END)::numeric,
    SUM(CASE WHEN is_current AND category='second' THEN row_kg       ELSE 0 END)::numeric,
    SUM(CASE WHEN is_current THEN quantity_pcs ELSE 0 END)::numeric,
    COUNT(DISTINCT CASE WHEN is_current THEN NULLIF(check_id,'') END)::bigint,
    SUM(CASE WHEN is_current AND category='second' THEN revenue      ELSE 0 END)::numeric,
    SUM(CASE WHEN is_current AND category='second' THEN row_kg       ELSE 0 END)::numeric,
    SUM(CASE WHEN is_current AND category='second' THEN quantity_pcs ELSE 0 END)::numeric,
    COUNT(DISTINCT CASE WHEN is_current AND category='second' THEN NULLIF(check_id,'') END)::bigint,
    SUM(CASE WHEN is_current AND category='second' AND is_aplus THEN revenue ELSE 0 END)::numeric,
    SUM(CASE WHEN is_current AND category='second' AND is_aplus THEN row_kg  ELSE 0 END)::numeric,
    COUNT(DISTINCT CASE WHEN is_current AND category='second' AND is_aplus THEN NULLIF(check_id,'') END)::bigint,
    SUM(CASE WHEN is_current AND category='new' THEN revenue ELSE 0 END)::numeric,
    COUNT(DISTINCT CASE WHEN is_current AND category='new' THEN NULLIF(check_id,'') END)::bigint,
    SUM(CASE WHEN is_prev      THEN revenue ELSE 0 END)::numeric,
    SUM(CASE WHEN is_prev_week THEN revenue ELSE 0 END)::numeric,
    SUM(CASE WHEN is_prev      AND category='second' THEN row_kg ELSE 0 END)::numeric,
    SUM(CASE WHEN is_prev_week AND category='second' THEN row_kg ELSE 0 END)::numeric
  FROM periods p
  GROUP BY p.store;
$$;
