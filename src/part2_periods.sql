-- Active: 1702571800545@@127.0.0.1@5432@retail_analytics

DROP MATERIALIZED VIEW IF EXISTS Periods CASCADE;

CREATE MATERIALIZED VIEW IF NOT EXISTS Periods AS
WITH groups_id AS (
SELECT PI.Customer_ID,
Tr.Transaction_ID,
Tr.Transaction_DateTime,
PG.Group_ID
FROM Personal_Information PI
INNER JOIN Cards Ca ON PI.Customer_ID = Ca.Customer_ID
INNER JOIN Transactions Tr ON Ca.Customer_Card_ID = Tr.Customer_Card_ID
INNER JOIN Checks Ch ON Tr.Transaction_ID = Ch.Transaction_ID
INNER JOIN Product_Grid PG ON Ch.SKU_ID = PG.SKU_ID
),
min_date AS (
SELECT customer_id, group_id, MIN(Transaction_DateTime) AS First_Group_Purchase_Date FROM groups_id
GROUP BY customer_id, group_id
),
max_date AS (
SELECT customer_id, group_id, MAX(Transaction_DateTime) AS Last_Group_Purchase_Date FROM groups_id
GROUP BY customer_id, group_id
),
group_purchase AS (
SELECT customer_id, group_id, COUNT(group_id) AS Group_Purchase FROM groups_id
GROUP BY customer_id, group_id	
),
group_frequency AS (
SELECT DISTINCT gi.Customer_ID, 
gi.Group_ID, 
First_Group_Purchase_Date, 
Last_Group_Purchase_Date, 
Group_Purchase,
(EXTRACT(EPOCH FROM (Last_Group_Purchase_Date - First_Group_Purchase_Date))::FLOAT/86400.0 + 1.0) / Group_Purchase::FLOAT AS Group_Frequency
FROM groups_id gi
INNER JOIN min_date mind ON mind.Customer_ID = gi.Customer_ID AND mind.Group_ID = gi.Group_ID
INNER JOIN max_date maxd ON maxd.Customer_ID = gi.Customer_ID AND maxd.Group_ID = gi.Group_ID
INNER JOIN group_purchase gp ON gp.Customer_ID = gi.Customer_ID AND gp.Group_ID = gi.Group_ID
ORDER BY 1
),
group_discount AS (
SELECT gi.customer_id, gi.transaction_id, gi.group_id,
CASE
WHEN ch.SKU_Discount = 0 THEN 0
ELSE ch.SKU_Discount / ch.SKU_Summ
END AS Group_Discount
FROM groups_id gi
INNER JOIN Checks ch ON gi.Transaction_ID = ch.Transaction_ID
ORDER BY 1, 3
),
group_min_discount AS (
SELECT customer_id, group_id,
MIN(group_discount) FILTER ( WHERE group_discount > 0 ) AS Group_Min_Discount FROM group_discount
GROUP BY customer_id, group_id
),
group_min_discount_no_null AS (
SELECT customer_id, group_id,
CASE 
	WHEN Group_Min_Discount IS NULL THEN 0 
	ELSE Group_Min_Discount 
END Group_Min_Discount
FROM group_min_discount
)
SELECT
gf.Customer_ID, 
gf.Group_ID, 
gf.First_Group_Purchase_Date, 
gf.Last_Group_Purchase_Date, 
gf.Group_Purchase,
gf.Group_Frequency,
gmd.Group_Min_Discount
FROM group_frequency gf
INNER JOIN group_min_discount_no_null gmd 
ON gf.Customer_ID = gmd.Customer_ID AND gf.Group_ID = gmd.Group_ID  
;

-- Тесты представления

SELECT *
FROM Periods;

SELECT *
FROM Periods
WHERE Group_Purchase < 6 AND Group_Purchase > 2;

SELECT *
FROM Periods
WHERE (Group_ID BETWEEN 1 AND 5) AND (Group_Frequency BETWEEN 50 AND 100);

SELECT *
FROM Periods
WHERE Group_Min_Discount = 0 AND Group_Purchase < 4 AND Group_Purchase > 1;
