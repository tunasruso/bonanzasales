CREATE OR REPLACE FUNCTION get_kpis_aggregated(
  p_start      date,
  p_end        date,
  p_stores     text[]   DEFAULT NULL,
  p_groups     text[]   DEFAULT NULL,
  p_products   text[]   DEFAULT NULL
)
RETURNS TABLE (
  total_revenue    numeric,
  total_kg         numeric,
  total_pcs        numeric,
  total_checks     bigint,
  total_positions  bigint,
  second_revenue   numeric,
  second_kg        numeric,
  second_pcs       numeric,
  second_checks    bigint,
  second_positions bigint,
  new_revenue      numeric,
  new_pcs          numeric,
  new_checks       bigint,
  new_positions    bigint
)
LANGUAGE sql
STABLE
AS $$
  SELECT
    SUM(s.revenue)::numeric,
    SUM(CASE WHEN c.category='second' THEN
      CASE WHEN c.avg_weight_kg > 0 THEN s.quantity_pcs * c.avg_weight_kg ELSE s.quantity_kg END
    ELSE 0 END)::numeric,
    SUM(s.quantity_pcs)::numeric,
    COUNT(DISTINCT NULLIF(split_part(COALESCE(s.recorder_id,''),'_',1),''))::bigint,
    COUNT(*)::bigint,
    SUM(CASE WHEN c.category='second' THEN s.revenue ELSE 0 END)::numeric,
    SUM(CASE WHEN c.category='second' THEN
      CASE WHEN c.avg_weight_kg > 0 THEN s.quantity_pcs * c.avg_weight_kg ELSE s.quantity_kg END
    ELSE 0 END)::numeric,
    SUM(CASE WHEN c.category='second' THEN s.quantity_pcs ELSE 0 END)::numeric,
    COUNT(DISTINCT CASE WHEN c.category='second' THEN NULLIF(split_part(COALESCE(s.recorder_id,''),'_',1),'') END)::bigint,
    COUNT(CASE WHEN c.category='second' THEN 1 END)::bigint,
    SUM(CASE WHEN c.category='new' THEN s.revenue ELSE 0 END)::numeric,
    SUM(CASE WHEN c.category='new' THEN s.quantity_pcs ELSE 0 END)::numeric,
    COUNT(DISTINCT CASE WHEN c.category='new' THEN NULLIF(split_part(COALESCE(s.recorder_id,''),'_',1),'') END)::bigint,
    COUNT(CASE WHEN c.category='new' THEN 1 END)::bigint
  FROM sales_analytics s
  LEFT JOIN product_classifications c
    ON c.product_group = s.product_group AND c.product = s.product
  WHERE s.sale_date BETWEEN p_start AND p_end
    AND (p_stores   IS NULL OR s.store         = ANY(p_stores))
    AND (p_groups   IS NULL OR s.product_group = ANY(p_groups))
    AND (p_products IS NULL OR s.product       = ANY(p_products))
    AND COALESCE(c.category, 'second') != 'exclude';
$$;
