DROP FUNCTION IF EXISTS fnc_increased_frequency_visits(TIMESTAMP, TIMESTAMP, FLOAT, FLOAT, FLOAT, FLOAT);
CREATE OR REPLACE FUNCTION fnc_increased_frequency_visits(first_date TIMESTAMP, 
														  last_date TIMESTAMP, 
														  added_number_transactions FLOAT,
														  max_churn_index FLOAT,
														  max_share_transactions FLOAT,
														  allowable_margin_share FLOAT)
RETURNS TABLE(Customer_ID INT,
			  Start_Date TIMESTAMP,
			  End_Date TIMESTAMP,
			  Required_Transactions_Count FLOAT,
			  Group_Name VARCHAR,
			  Offer_Discount_Depth FLOAT) AS $increased_frequency_visits$
BEGIN
RETURN QUERY
		SELECT C.Customer_ID,
			first_date,
			last_date,
			ROUND(EXTRACT(EPOCH FROM (last_date - first_date)) / Customer_Frequency) + added_number_transactions,
			SG.Group_Name,
			GTR.Offer_Discount
		FROM Customers C
		INNER JOIN group_to_reward(max_churn_index, max_share_transactions, allowable_margin_share) GTR
		ON C.Customer_ID = GTR.Cust_ID
		INNER JOIN SKU_Group SG
		ON GTR.Group_ID = SG.Group_ID;
END;
$increased_frequency_visits$ LANGUAGE plpgsql;


DROP FUNCTION IF EXISTS group_to_reward;
CREATE OR REPLACE FUNCTION group_to_reward(customer_churn_rate FLOAT, discount_share FLOAT, margin FLOAT)
RETURNS TABLE(
Cust_ID INT,
Group_id INT,
Offer_Discount FLOAT
)
AS $$
DECLARE
avg_margin FLOAT; 
discount_depth FLOAT;
Person_ID INT := 0;
row RECORD;
BEGIN
FOR row IN ( 
SELECT groups.customer_id, groups.group_id, group_affinity_index,
group_churn_rate, group_discount_share, group_minimum_discount,
ROW_NUMBER() OVER (PARTITION BY groups.customer_id ORDER BY group_affinity_index DESC)
FROM groups
WHERE group_churn_rate <= customer_churn_rate AND group_discount_share < (discount_share / 100.)
ORDER BY customer_id, group_minimum_discount
)
LOOP
avg_margin = (
SELECT AVG(group_summ_paid - group_cost)
FROM Purchase_History ph
WHERE ph.customer_id = row.customer_id
AND ph.group_id = row.group_id
);
discount_depth = (FLOOR ((row.group_minimum_discount * 100) / 5.0) * 5)::numeric(10, 2);
IF (Person_ID != row.customer_id) THEN
    IF (avg_margin > 0  AND row.group_minimum_discount::numeric(10, 2) > 0 
    AND avg_margin * margin / 100. > discount_depth * avg_margin / 100.) THEN
        IF (discount_depth = 0) THEN
        discount_depth = 5;
        END IF;
    RETURN QUERY (SELECT g.customer_id, g.group_id,
    discount_depth AS Offer_Discount_Depth
    FROM groups g
    WHERE row.customer_id = g.customer_id AND
    row.group_id = g.group_id);
    Person_ID = row.customer_id;
    END IF;
END IF;
END LOOP;
END
$$ LANGUAGE plpgsql;


-- Тесты

SELECT *
FROM fnc_increased_frequency_visits('2022-08-18 00:00:00', '2022-08-18 00:00:00', 1, 3, 70, 30);

SELECT *
FROM fnc_increased_frequency_visits('2018-08-17', '2021-05-11', 3, 7, 30, 5)
WHERE Group_Name = 'Пиво'
ORDER BY 1;

SELECT *
FROM fnc_increased_frequency_visits('2020-05-21', '2021-10-23', 3, 5, 50, 20)
WHERE Customer_ID = 1;

SELECT *
FROM fnc_increased_frequency_visits('2021-08-20', '2023-01-20', 2, 4, 70, 60)
WHERE Offer_Discount_Depth BETWEEN 10 AND 20;
