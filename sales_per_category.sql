WITH CTE_sales AS (
    SELECT product_id, SUM(qty * (amount - tax)) AS total_sales
    FROM sales
    WHERE purchased_date BETWEEN '2026-01-01' AND '2026-01-31'
    GROUP BY product_id)

SELECT b.product_category_id, sum(a.total_sales) as total_sales_per_category
FROM CTE_sales a
LEFT JOIN products_category b ON a.product_id = b.product_id
GROUP BY b.product_category_id
