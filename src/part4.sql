DROP FUNCTION IF EXISTS earliest_date();
CREATE OR REPLACE FUNCTION earliest_date()
RETURNS SETOF DATE AS $$ BEGIN
RETURN QUERY
SELECT transaction_datetime::DATE
FROM Transactions
ORDER BY 1 LIMIT 1;
END;
 $$ LANGUAGE plpgsql;


DROP FUNCTION IF EXISTS latest_date();
CREATE OR REPLACE FUNCTION latest_date()
RETURNS SETOF DATE AS $$ BEGIN
RETURN QUERY
SELECT Analysis_Formation::date FROM date_of_analysis;
END;
 $$ LANGUAGE plpgsql;

DROP FUNCTION IF EXISTS avg_check_by_date_period(left_date_border DATE, right_date_border DATE);

CREATE OR REPLACE FUNCTION avg_check_by_date_period(left_date_border DATE, right_date_border DATE) 
RETURNS TABLE (Customer_ID INT, Customer_Average_Check FLOAT) AS $$
BEGIN
IF (left_date_border > right_date_border) THEN
RAISE EXCEPTION 'Wrong date borders!';
END IF;
IF (right_date_border > latest_date()) THEN
right_date_border = latest_date();
ELSEIF (left_date_border > earliest_date()) THEN
END IF;
RETURN QUERY
WITH transaction_within_dates AS (
SELECT tr.Transaction_ID, tr.Transaction_Summ, tr.Customer_Card_ID FROM Transactions tr 
WHERE transaction_datetime::date >= left_date_border AND transaction_datetime::date <= right_date_border
)
SELECT pi.Customer_ID, 
AVG(tr.Transaction_Summ) AS Customer_Average_Check
FROM Personal_Information pi
INNER JOIN Cards ON Cards.Customer_ID = pi.Customer_ID
INNER JOIN transaction_within_dates tr ON Cards.Customer_Card_ID = tr.Customer_Card_ID
GROUP BY 1;

END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION target_avg_check_by_date_period(left_date_border DATE, right_date_border DATE, target_koef FLOAT) 
RETURNS TABLE (Customer_ID INT, Customer_Average_Check FLOAT) AS $$
BEGIN
IF (left_date_border > right_date_border) THEN
RAISE EXCEPTION 'Wrong date borders!';
END IF;
IF (right_date_border > latest_date()) THEN
right_date_border = latest_date();
ELSEIF (left_date_border > earliest_date()) THEN
END IF;
RETURN QUERY
WITH transaction_within_dates AS (
SELECT tr.Transaction_ID, tr.Transaction_Summ, tr.Customer_Card_ID FROM Transactions tr 
WHERE transaction_datetime::date >= left_date_border AND transaction_datetime::date <= right_date_border
)
SELECT pi.Customer_ID, 
AVG(tr.Transaction_Summ) * target_koef AS Customer_Average_Check
FROM Personal_Information pi
INNER JOIN Cards ON Cards.Customer_ID = pi.Customer_ID
INNER JOIN transaction_within_dates tr ON Cards.Customer_Card_ID = tr.Customer_Card_ID
GROUP BY 1;

END;
$$ LANGUAGE plpgsql;

DROP FUNCTION IF EXISTS avg_check_by_transactions_amount(transactions_amount INT);
CREATE OR REPLACE FUNCTION avg_check_by_transactions_amount(transactions_amount INT) 
RETURNS TABLE (C_ID INT, Customer_Average_Check FLOAT) AS $$
BEGIN
RETURN QUERY
WITH total_tr AS (
SELECT pi.Customer_ID, 
tr.Transaction_Summ AS sum_tr,
ROW_NUMBER() OVER (PARTITION BY pi.customer_id ORDER BY tr.transaction_datetime DESC) AS order_tr
FROM Personal_Information pi
INNER JOIN Cards ON Cards.Customer_ID = pi.Customer_ID
INNER JOIN Transactions tr ON Cards.Customer_Card_ID = tr.Customer_Card_ID
ORDER BY 1, 3
),
set_border AS (
SELECT customer_id::INT, sum_tr::INT, order_tr::INT FROM total_tr
WHERE order_tr <= transactions_amount
)
SELECT customer_id::INT, AVG(sum_tr)::FLOAT FROM set_border
GROUP BY 1;
END;
$$ LANGUAGE plpgsql;

DROP FUNCTION target_avg_check_by_transactions_amount(transactions_amount INT, target_koef FLOAT);

CREATE OR REPLACE FUNCTION target_avg_check_by_transactions_amount(transactions_amount INT, target_koef FLOAT) 
RETURNS TABLE (Cust_ID INT, Customer_Average_Check FLOAT) AS $$
BEGIN
RETURN QUERY
WITH total_tr AS (
SELECT pi.Customer_ID, 
tr.Transaction_Summ AS sum_tr,
ROW_NUMBER() OVER (PARTITION BY pi.customer_id ORDER BY tr.transaction_datetime DESC) AS order_tr
FROM Personal_Information pi
INNER JOIN Cards ON Cards.Customer_ID = pi.Customer_ID
INNER JOIN Transactions tr ON Cards.Customer_Card_ID = tr.Customer_Card_ID
ORDER BY 1, 3
),
set_border AS (
SELECT customer_id::INT, sum_tr::INT, order_tr::INT FROM total_tr
WHERE order_tr <= transactions_amount
)
SELECT customer_id::INT, AVG(sum_tr)::FLOAT * target_koef FROM set_border
GROUP BY 1;
END;
$$ LANGUAGE plpgsql;

DROP FUNCTION group_to_reward;
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
group_churn_rate, group_discount_share, group_minimum_discount
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

DROP FUNCTION avg_check_growth_offer;
CREATE OR REPLACE FUNCTION avg_check_growth_offer (
calc_method INT,
left_date DATE,
right_date DATE,
transactions_amount INT,
check_increase_koef FLOAT,
customer_churn_rate FLOAT,
discount_share FLOAT,
margin_share FLOAT
)
RETURNS TABLE(
Customer_ID INT,
Required_Check_Measure FLOAT,
Group_Name VARCHAR,
Offer_Discount_Depth INT
)
AS $$
BEGIN

IF (calc_method = 2) THEN
RETURN QUERY
WITH group_to_reward AS (
SELECT Cust_ID AS CID,
Group_ID,
Offer_Discount AS ODD FROM group_to_reward(customer_churn_rate, discount_share, margin_share)  
)
SELECT tac.Cust_ID::INT,
tac.Customer_Average_Check::FLOAT,
sg.Group_Name::VARCHAR,
gtr.ODD::INT
FROM target_avg_check_by_transactions_amount(transactions_amount, check_increase_koef) tac
INNER JOIN group_to_reward gtr ON gtr.CID = tac.Cust_ID
INNER JOIN SKU_Group sg ON gtr.Group_ID = sg.Group_ID
;
ELSEIF (calc_method = 1) THEN
RETURN QUERY
WITH group_to_reward AS (
SELECT Cust_ID AS CID,
Group_ID,
Offer_Discount AS ODD FROM group_to_reward(customer_churn_rate, discount_share, margin_share)  
)
SELECT tac.Customer_ID::INT,
tac.Customer_Average_Check::FLOAT,
sg.Group_Name::VARCHAR,
gtr.ODD::INT
FROM target_avg_check_by_date_period(left_date, right_date, check_increase_koef) tac
INNER JOIN group_to_reward gtr ON gtr.CID = tac.Customer_ID
INNER JOIN SKU_Group sg ON gtr.Group_ID = sg.Group_ID
;
END IF;

END
$$ LANGUAGE plpgsql;


-- Тесты функции

SELECT Customer_ID, Required_Check_Measure, Group_Name, Offer_Discount_Depth
FROM avg_check_growth_offer(2, null, null, 100, 1.15, 3, 70, 30);

SELECT Customer_ID, Required_Check_Measure, Group_Name, Offer_Discount_Depth
FROM avg_check_growth_offer(1, '2022-01-01', '2023-01-01', null, 1.15, 3, 70, 10);

SELECT Customer_ID, Required_Check_Measure, Group_Name, Offer_Discount_Depth
FROM avg_check_growth_offer(1, '2023-01-01', '2022-01-01', null, 1.15, 3, 70, 10);

SELECT Customer_ID, Required_Check_Measure, Group_Name, Offer_Discount_Depth
FROM avg_check_growth_offer(3, '2023-01-01', '2022-01-01', null, 1.15, 3, 70, 10);
