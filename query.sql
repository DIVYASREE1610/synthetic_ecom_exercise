SELECT
  u.name        AS user_name,
  u.email       AS user_email,
  p.name        AS product_name,
  o.quantity,
  p.price,
  o.quantity * p.price AS total_price,
  o.order_date
FROM orders o
JOIN users u
  ON o.user_id = u.user_id
JOIN products p
  ON o.product_id = p.product_id
ORDER BY o.order_date;
