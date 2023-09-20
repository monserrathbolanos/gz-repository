{{ config(schema='transaction') }}

WITH 

  sales AS (SELECT * FROM `gz_raw_data.raw_gz_sales`)

  ,product AS (SELECT * FROM `gz_raw_data.raw_gz_product`)

SELECT
  s.date_date
  ### Key ###
  ,s.orders_id
  ,s.pdt_id AS products_id
  ###########
	-- qty --
	,s.quantity AS qty
  -- revenue --
  ,s.revenue AS turnover
  -- cost --
  ,CAST(p.purchSE_PRICE AS FLOAT64) AS purchase_price
	,ROUND(s.quantity*CAST(p.purchSE_PRICE AS FLOAT64),2) AS purchase_cost
<<<<<<< HEAD
	-- product_margin --
=======
	-- margin --
>>>>>>> 6a3042729f2f09d390c791e4727a875a445ec9b5
	,ROUND(s.revenue-s.quantity*CAST(p.purchSE_PRICE AS FLOAT64),2) AS margin
FROM sales s
INNER JOIN product p ON s.pdt_id = p.products_id