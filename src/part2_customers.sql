-- Active: 1702571800545@@127.0.0.1@5432@retail_analytics

DROP MATERIALIZED VIEW IF EXISTS Customers CASCADE;
CREATE MATERIALIZED VIEW IF NOT EXISTS Customers AS
WITH avg_check AS (
SELECT pi.Customer_ID, 
AVG(tr.Transaction_Summ) AS Customer_Average_Check
FROM Personal_Information pi
INNER JOIN Cards ON Cards.Customer_ID = pi.Customer_ID
INNER JOIN Transactions tr ON Cards.Customer_Card_ID = tr.Customer_Card_ID
GROUP BY 1
ORDER BY 2 DESC
),
avg_check_segment AS (
SELECT customer_id,
customer_average_check, 
CASE
    WHEN ROW_NUMBER() over() <= (SELECT count(*) FROM avg_check)*0.10 THEN 'High'
    WHEN ROW_NUMBER() over() > (SELECT count(*) FROM avg_check)*0.10 AND ROW_NUMBER() over() <= (SELECT count(*) FROM avg_check)*0.35 THEN 'Medium'
    ELSE 'Low'
END Customer_Average_Check_Segment
FROM avg_check
),
customer_frequency AS (
SELECT pi.Customer_ID,
EXTRACT(EPOCH FROM MAX(tr.transaction_datetime) - MIN(tr.transaction_datetime))::FLOAT/(COUNT(tr.transaction_datetime::date)::FLOAT * 86400.0) AS Customer_Frequency 
FROM Personal_Information pi
INNER JOIN Cards ON Cards.Customer_ID = pi.Customer_ID
INNER JOIN Transactions tr ON Cards.Customer_Card_ID = tr.Customer_Card_ID
GROUP BY 1
ORDER BY 1, 2
),
customer_frequency_segment AS (
SELECT customer_id, Customer_Frequency,
CASE
    WHEN ROW_NUMBER() over(ORDER BY Customer_Frequency) < (SELECT count(*) FROM customer_frequency)*0.1 THEN 'Often'
    WHEN ROW_NUMBER() over(ORDER BY Customer_Frequency) > (SELECT count(*) FROM customer_frequency)*0.1 AND ROW_NUMBER() over() < (SELECT count(*) FROM customer_frequency)*0.35 THEN 'Occasionally'
    ELSE 'Rarely'
END Customer_Frequency_Segment
FROM customer_frequency
),
customer_inactivity AS (
SELECT pi.Customer_ID, EXTRACT(EPOCH FROM(SELECT * FROM date_of_analysis) - MAX(tr.transaction_datetime))::FLOAT/86400.0 AS Customer_Inactive_Period
FROM Personal_Information pi
INNER JOIN Cards ON Cards.Customer_ID = pi.Customer_ID
INNER JOIN Transactions tr ON Cards.Customer_Card_ID = tr.Customer_Card_ID
GROUP BY pi.Customer_ID
ORDER BY 1
),
customer_churn_rate AS (
SELECT pi.Customer_ID, ci.Customer_Inactive_Period/cf.Customer_Frequency AS Customer_Churn_Rate
FROM Personal_Information pi
INNER JOIN customer_inactivity ci ON pi.Customer_ID = ci.Customer_ID
INNER JOIN customer_frequency cf ON pi.Customer_ID = cf.Customer_ID    
),
customer_churn_segment AS (
SELECT customer_id, 
CASE
    WHEN Customer_Churn_Rate >= 0 AND Customer_Churn_Rate < 2 THEN 'Low'
    WHEN Customer_Churn_Rate >= 2 AND Customer_Churn_Rate < 5 THEN 'Medium'
    ELSE 'High'
END Customer_Churn_Segment
FROM customer_churn_rate
),
customer_segment AS (
SELECT acs.customer_id,
CASE
    WHEN acs.Customer_Average_Check_Segment = 'Low' AND cfs.Customer_Frequency_Segment = 'Rarely' AND ccs.Customer_Churn_Segment = 'Low' THEN 1
    WHEN acs.Customer_Average_Check_Segment = 'Low' AND cfs.Customer_Frequency_Segment = 'Rarely' AND ccs.Customer_Churn_Segment = 'Medium' THEN 2
    WHEN acs.Customer_Average_Check_Segment = 'Low' AND cfs.Customer_Frequency_Segment = 'Rarely' AND ccs.Customer_Churn_Segment = 'High' THEN 3
    WHEN acs.Customer_Average_Check_Segment = 'Low' AND cfs.Customer_Frequency_Segment = 'Occasionally' AND ccs.Customer_Churn_Segment = 'Low' THEN 4
    WHEN acs.Customer_Average_Check_Segment = 'Low' AND cfs.Customer_Frequency_Segment = 'Occasionally' AND ccs.Customer_Churn_Segment = 'Medium' THEN 5
    WHEN acs.Customer_Average_Check_Segment = 'Low' AND cfs.Customer_Frequency_Segment = 'Occasionally' AND ccs.Customer_Churn_Segment = 'High' THEN 6
    WHEN acs.Customer_Average_Check_Segment = 'Low' AND cfs.Customer_Frequency_Segment = 'Often' AND ccs.Customer_Churn_Segment = 'Low' THEN 7
    WHEN acs.Customer_Average_Check_Segment = 'Low' AND cfs.Customer_Frequency_Segment = 'Often' AND ccs.Customer_Churn_Segment = 'Medium' THEN 8
    WHEN acs.Customer_Average_Check_Segment = 'Low' AND cfs.Customer_Frequency_Segment = 'Often' AND ccs.Customer_Churn_Segment = 'High' THEN 9
    WHEN acs.Customer_Average_Check_Segment = 'Medium' AND cfs.Customer_Frequency_Segment = 'Rarely' AND ccs.Customer_Churn_Segment = 'Low' THEN 10
    WHEN acs.Customer_Average_Check_Segment = 'Medium' AND cfs.Customer_Frequency_Segment = 'Rarely' AND ccs.Customer_Churn_Segment = 'Medium' THEN 11
    WHEN acs.Customer_Average_Check_Segment = 'Medium' AND cfs.Customer_Frequency_Segment = 'Rarely' AND ccs.Customer_Churn_Segment = 'High' THEN 12
    WHEN acs.Customer_Average_Check_Segment = 'Medium' AND cfs.Customer_Frequency_Segment = 'Occasionally' AND ccs.Customer_Churn_Segment = 'Low' THEN 13
    WHEN acs.Customer_Average_Check_Segment = 'Medium' AND cfs.Customer_Frequency_Segment = 'Occasionally' AND ccs.Customer_Churn_Segment = 'Medium' THEN 14
    WHEN acs.Customer_Average_Check_Segment = 'Medium' AND cfs.Customer_Frequency_Segment = 'Occasionally' AND ccs.Customer_Churn_Segment = 'High' THEN 15
    WHEN acs.Customer_Average_Check_Segment = 'Medium' AND cfs.Customer_Frequency_Segment = 'Often' AND ccs.Customer_Churn_Segment = 'Low' THEN 16
    WHEN acs.Customer_Average_Check_Segment = 'Medium' AND cfs.Customer_Frequency_Segment = 'Often' AND ccs.Customer_Churn_Segment = 'Medium' THEN 17
    WHEN acs.Customer_Average_Check_Segment = 'Medium' AND cfs.Customer_Frequency_Segment = 'Often' AND ccs.Customer_Churn_Segment = 'High' THEN 18
    WHEN acs.Customer_Average_Check_Segment = 'High' AND cfs.Customer_Frequency_Segment = 'Rarely' AND ccs.Customer_Churn_Segment = 'Low' THEN 19
    WHEN acs.Customer_Average_Check_Segment = 'High' AND cfs.Customer_Frequency_Segment = 'Rarely' AND ccs.Customer_Churn_Segment = 'Medium' THEN 20
    WHEN acs.Customer_Average_Check_Segment = 'High' AND cfs.Customer_Frequency_Segment = 'Rarely' AND ccs.Customer_Churn_Segment = 'High' THEN 21
    WHEN acs.Customer_Average_Check_Segment = 'High' AND cfs.Customer_Frequency_Segment = 'Occasionally' AND ccs.Customer_Churn_Segment = 'Low' THEN 22
    WHEN acs.Customer_Average_Check_Segment = 'High' AND cfs.Customer_Frequency_Segment = 'Occasionally' AND ccs.Customer_Churn_Segment = 'Medium' THEN 23
    WHEN acs.Customer_Average_Check_Segment = 'High' AND cfs.Customer_Frequency_Segment = 'Occasionally' AND ccs.Customer_Churn_Segment = 'High' THEN 24
    WHEN acs.Customer_Average_Check_Segment = 'High' AND cfs.Customer_Frequency_Segment = 'Often' AND ccs.Customer_Churn_Segment = 'Low' THEN 25
    WHEN acs.Customer_Average_Check_Segment = 'High' AND cfs.Customer_Frequency_Segment = 'Often' AND ccs.Customer_Churn_Segment = 'Medium' THEN 26
    WHEN acs.Customer_Average_Check_Segment = 'High' AND cfs.Customer_Frequency_Segment = 'Often' AND ccs.Customer_Churn_Segment = 'High' THEN 27
END Customer_Segment
FROM avg_check_segment acs
INNER JOIN customer_frequency_segment cfs ON acs.Customer_ID = cfs.Customer_ID
INNER JOIN customer_churn_segment ccs ON acs.Customer_ID = ccs.Customer_ID
),
total_transactions AS (
SELECT pi.customer_id, COUNT(pi.customer_id) AS total_transactions
FROM Personal_Information pi
JOIN Cards ON pi.customer_id = Cards.customer_id
JOIN Transactions tr ON Cards.customer_card_id = tr.customer_card_id
GROUP BY pi.customer_id
ORDER BY pi.customer_id
),
transactions_by_store AS (
SELECT pi.customer_id, tr.transaction_store_id, 
COUNT(tr.transaction_store_id) AS transactions_by_store, MAX(tr.transaction_datetime) AS max_date
FROM Personal_Information pi
JOIN Cards ON pi.customer_id = Cards.customer_id
JOIN Transactions tr ON Cards.customer_card_id = tr.customer_card_id
GROUP BY pi.customer_id, tr.transaction_store_id
ORDER BY pi.customer_id
),
transactions_fractions AS (
SELECT tbs.customer_id, transaction_store_id, transactions_by_store, tt.total_transactions,
transactions_by_store::float/tt.total_transactions::float AS fractions, tbs.max_date
FROM transactions_by_store tbs
INNER JOIN total_transactions tt ON tt.customer_id = tbs.customer_id 
),
max_transactions_fractions AS (
SELECT customer_id, MAX(fractions) AS max_fr, MAX(max_date) AS max_date
FROM transactions_fractions
GROUP BY customer_id
ORDER BY 1
),
max_transactions_fractions_unique AS (
SELECT tf.customer_id, tf.transaction_store_id, fractions, max_fr,
CASE
    WHEN max_fr = fractions THEN 1
    ELSE 0
END is_max, mtf.max_date
FROM transactions_fractions tf
INNER JOIN max_transactions_fractions mtf ON tf.customer_id = mtf.customer_id
),
max_transactions_fractions_unique_count AS (
SELECT customer_id, SUM(is_max) AS maxes
FROM max_transactions_fractions_unique
GROUP BY customer_id
),
all_stores AS (
SELECT pi.customer_id, tr.transaction_store_id, tr.transaction_datetime,
ROW_NUMBER() OVER(PARTITION BY pi.customer_id ORDER BY tr.transaction_datetime DESC) AS visited_last
FROM Personal_Information pi
JOIN Cards ON pi.customer_id = Cards.customer_id
JOIN Transactions tr ON Cards.customer_card_id = tr.customer_card_id
ORDER BY pi.customer_id, tr.transaction_datetime DESC     
),
store_1 AS (
SELECT customer_id, transaction_store_id AS store1,
transaction_datetime AS datetime_1
FROM all_stores
WHERE visited_last = 1     
),
store_2 AS (
SELECT customer_id, transaction_store_id AS store2,
transaction_datetime AS datetime_2
FROM all_stores
WHERE visited_last = 2     
),
store_3 AS (
SELECT customer_id, transaction_store_id AS store3,
transaction_datetime AS datetime_3
FROM all_stores
WHERE visited_last = 3     
),
last_visited_stores AS (
SELECT s1.customer_id, store1, datetime_1, store2, datetime_2, store3, datetime_3 
FROM store_1 s1
INNER JOIN store_2 s2 ON s1.customer_id = s2.customer_id
INNER JOIN store_3 s3 ON s1.customer_id = s3.customer_id
),
last_visited_stores_and_fractions AS (
SELECT tf.customer_id, transaction_store_id, fractions, store1, store2, store3, 
datetime_1, datetime_2, datetime_3
FROM transactions_fractions tf
INNER JOIN last_visited_stores lvs ON tf.customer_id = lvs.customer_id
),
max_fr_stores AS (
SELECT lvsaf.customer_id, transaction_store_id, lvsaf.fractions, store1, store2, store3, maxes, max_fr,
datetime_1, datetime_2, datetime_3, max_date 
FROM last_visited_stores_and_fractions lvsaf
INNER JOIN max_transactions_fractions_unique_count mtfuc ON mtfuc.customer_id = lvsaf.customer_id
INNER JOIN max_transactions_fractions mtf ON mtf.customer_id = lvsaf.customer_id
),
by_equality AS (
SELECT customer_id, transaction_store_id FROM max_fr_stores
WHERE store1 = store2 AND store1 = store3 AND store2 = store3 AND transaction_store_id = store1
),
by_fractions_with_1_max AS (
SELECT customer_id, transaction_store_id FROM max_fr_stores
WHERE fractions = max_fr AND maxes = 1
),
by_fractions_with_several_maxes AS (
SELECT * FROM max_fr_stores
WHERE maxes > 1 AND fractions = max_fr 
),
by_fractions_with_several_maxes_sum AS (
SELECT DISTINCT customer_id,
CASE
    WHEN datetime_1 = max_date THEN store1
    WHEN datetime_2 = max_date THEN store2
    WHEN datetime_3 = max_date THEN store3
ELSE store1
END store_id
FROM by_fractions_with_several_maxes
),
customer_primary_store AS (
SELECT * FROM by_equality
UNION
SELECT * FROM by_fractions_with_1_max
UNION
SELECT * FROM by_fractions_with_several_maxes_sum
),
customer_primary_store_named AS (
SELECT customer_id, transaction_store_id AS Customer_Primary_Store FROM customer_primary_store
ORDER BY 1
)
SELECT ac.Customer_ID,
ac.Customer_Average_Check,
acs.Customer_Average_Check_Segment,
cf.Customer_Frequency,
cfs.Customer_Frequency_Segment,
ci.Customer_Inactive_Period,
ccr.Customer_Churn_Rate,
ccs.Customer_Churn_Segment,
cs.Customer_Segment,
cpsn.Customer_Primary_Store
FROM avg_check ac
INNER JOIN avg_check_segment acs ON ac.Customer_ID = acs.Customer_ID
INNER JOIN customer_frequency cf ON ac.Customer_ID = cf.Customer_ID
INNER JOIN customer_frequency_segment cfs ON ac.Customer_ID = cfs.Customer_ID
INNER JOIN customer_inactivity ci ON ac.Customer_ID = ci.Customer_ID
INNER JOIN customer_churn_rate ccr ON ac.Customer_ID = ccr.Customer_ID
INNER JOIN customer_churn_segment ccs ON ac.Customer_ID = ccs.Customer_ID
INNER JOIN customer_segment cs ON ac.Customer_ID = cs.Customer_ID
INNER JOIN customer_primary_store_named cpsn ON ac.Customer_ID = cpsn.Customer_ID
ORDER BY 1
;

-- Тесты представления

SELECT * FROM Customers;

SELECT * FROM Customers
WHERE customer_id = 1 OR customer_id = 3
ORDER BY 1;

SELECT * FROM Customers
WHERE Customer_Average_Check_Segment = 'High' OR Customer_Average_Check_Segment = 'Medium'
ORDER BY 1;

WITH max_seg AS (
SELECT MAX(customer_segment) AS max
FROM Customers 
)
SELECT customer_id, customer_average_check, customer_frequency, customer_inactive_period, customer_primary_store FROM Customers
WHERE Customer_Segment = (SELECT max FROM max_seg);

SELECT * FROM Customers
WHERE customer_average_check > 750;
