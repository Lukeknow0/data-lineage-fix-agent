SELECT
  customer_id,
  SUM(total_amount) AS lifetime_value
FROM orders
GROUP BY customer_id
ORDER BY customer_id;
