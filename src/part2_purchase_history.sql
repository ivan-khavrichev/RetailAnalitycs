-- Active: 1702571800545@@127.0.0.1@5432@retail_analytics

DROP MATERIALIZED VIEW IF EXISTS Purchase_History CASCADE;

CREATE MATERIALIZED VIEW IF NOT EXISTS Purchase_History AS
	SELECT PI.Customer_ID,
		Tr.Transaction_ID,
		Tr.Transaction_DateTime,
		PG.Group_ID,
		SUM(S.SKU_Purchase_Price * Ch.SKU_Amount) AS Group_Cost,
		SUM(Ch.SKU_Summ) AS Group_Summ,
		SUM(SKU_Summ_Paid) AS Group_Summ_Paid
	FROM Personal_Information PI
	INNER JOIN Cards Ca
	ON PI.Customer_ID = Ca.Customer_ID
	INNER JOIN Transactions Tr
	ON Ca.Customer_Card_ID = Tr.Customer_Card_ID
	INNER JOIN Checks Ch
	ON Tr.Transaction_ID = Ch.Transaction_ID
	INNER JOIN Product_Grid PG
	ON Ch.SKU_ID = PG.SKU_ID
	INNER JOIN Stores S
	ON PG.SKU_ID = S.SKU_ID AND Tr.Transaction_Store_ID = S.Transaction_Store_ID
	GROUP BY PI.Customer_ID, Tr.Transaction_ID, Tr.Transaction_DateTime, PG.Group_ID
	ORDER BY 1, 4, 2;


-- Тесты представления

SELECT *
FROM Purchase_History;

SELECT *
FROM Purchase_History
WHERE Customer_ID = 5 AND group_id = 2;

SELECT *
FROM Purchase_History
WHERE Group_Summ < 400 AND Group_Cost > 200;

SELECT *
FROM Purchase_History
WHERE Group_Summ < 100 AND Group_Cost < 100;
