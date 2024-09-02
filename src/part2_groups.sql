-- Active: 1702571800545@@127.0.0.1@5432@retail_analytics

DROP MATERIALIZED VIEW IF EXISTS Groups;

CREATE MATERIALIZED VIEW IF NOT EXISTS Groups AS
-- Формирование списка SKU для каждого клиента
-- CORRECT
WITH sku_list AS (
SELECT DISTINCT
pi.customer_id,
s.group_id
FROM checks ch
INNER JOIN transactions t ON ch.transaction_id = t.transaction_id
INNER JOIN cards c ON t.customer_card_id = c.customer_card_id
INNER JOIN product_grid s ON ch.sku_id = s.sku_id
LEFT JOIN Personal_Information pi ON pi.customer_id = c.customer_id
WHERE transaction_datetime < (SELECT analysis_formation FROM date_of_analysis)
ORDER BY 1, 2
),
-- Расчет востребованности
-- CORRECT
affinity_index AS (
SELECT
sl.customer_id,
sl.group_id,
p.Group_Purchase / COUNT(DISTINCT ph.transaction_id)::NUMERIC AS Group_Affinity_Index
FROM sku_list sl
INNER JOIN Periods p ON sl.customer_id = p.customer_id AND sl.group_id = p.group_id
INNER JOIN Purchase_History ph ON sl.customer_id = ph.customer_id
WHERE ph.transaction_datetime BETWEEN p.First_Group_Purchase_Date AND p.Last_Group_Purchase_Date
GROUP BY sl.customer_id, sl.group_id, p.Group_Purchase
),
-- Расчет стабильности потребления группы
-- Интерваллы потребления
-- CORRECT
intervals_stability AS (
SELECT ph.customer_id,
ph.group_id,
EXTRACT(day FROM (ph.transaction_datetime - LAG(ph.transaction_datetime, 1, ph.transaction_datetime)
OVER (PARTITION BY ph.customer_id, ph.group_id ORDER BY ph.transaction_datetime))) AS day_intervals
FROM Purchase_History ph
ORDER BY ph.customer_id, ph.group_id
),
-- Абсолютное отклонение интервала от средней частоты покупок группы.
-- CORRECT
absolute_deviation AS (
SELECT intervals_stability.customer_id,
intervals_stability.group_id,
CASE
WHEN (intervals_stability.day_intervals - p.Group_Frequency) < 0
THEN (intervals_stability.day_intervals - p.Group_Frequency) * -1
ELSE (intervals_stability.day_intervals - p.Group_Frequency)
END AS abs_deviation
FROM intervals_stability
INNER JOIN Periods p ON p.group_id = intervals_stability.group_id
AND p.customer_id = intervals_stability.customer_id
),
-- Расчет маржи
-- CORRECT
margin_calc1 AS (
SELECT ph.customer_id,
ph.group_id,
ph.transaction_datetime,
sum(ph.Group_Summ_Paid) OVER (PARTITION BY ph.customer_id, ph.group_id) AS Group_Summ_Paid,
sum(ph.Group_Cost) OVER (PARTITION BY ph.customer_id, ph.group_id) AS Group_Cost,
-- в обратном порядке
row_number() OVER (ORDER BY ph.transaction_datetime DESC) AS rn
FROM Purchase_History ph
),
margin_calc AS (
SELECT margin_calc1.customer_id,
margin_calc1.group_id,
Group_Summ_Paid - Group_Cost AS Group_Margin
FROM margin_calc1
ORDER BY margin_calc1.customer_id, margin_calc1.group_id
),
-- Анализ скидок
discount_analysis AS (
SELECT DISTINCT pi.customer_id,
s.group_id,
count(ch.transaction_id) OVER (PARTITION BY pi.customer_id, s.group_id)::NUMERIC / p.Group_Purchase AS Group_Discount_Share,
COALESCE(min(p.Group_Min_Discount) OVER (PARTITION BY p.customer_id, p.group_id),
0) AS Group_Minimum_Discount
FROM checks ch
INNER JOIN product_grid s on ch.sku_id = s.sku_id
INNER JOIN transactions t ON t.transaction_id = ch.transaction_id
INNER JOIN cards c on t.customer_card_id = c.customer_card_id
LEFT JOIN Personal_Information pi on pi.customer_id = c.customer_id
INNER JOIN Periods p on c.customer_id = p.customer_id AND p.group_id = s.group_id
WHERE p.Group_Min_Discount > 0 AND ch.sku_discount > 0
ORDER BY 1, 2
),
discount_Average AS (
SELECT ph.customer_id,
ph.group_id,
sum(ph.Group_Summ_Paid) / sum(ph.Group_Summ) AS Group_Average_Discount
FROM purchase_History ph
INNER JOIN transactions t on ph.transaction_id = t.transaction_id
INNER JOIN checks ch ON t.transaction_id = ch.transaction_id
WHERE ch.sku_discount > 0
GROUP BY 1, 2
ORDER BY 1, 2
)
-- Основной запрос, объединяющий все вычисления
SELECT
    DISTINCT ai.customer_id,
    ai.group_id,
    ai.Group_Affinity_Index,
    EXTRACT(DAY FROM (SELECT analysis_formation FROM date_of_analysis) - p.Last_Group_Purchase_Date)::FLOAT / p.Group_Frequency::FLOAT AS Group_Churn_Rate,
    avg(si.abs_deviation / p.Group_Frequency)
    OVER (PARTITION BY p.customer_id, p.group_id) AS Group_Stability_Index, --Индекс стабильности
    mc.Group_Margin,
    da.Group_Discount_Share,
    da.Group_Minimum_Discount,
    davg.Group_Average_Discount
FROM Periods p
INNER JOIN affinity_index ai ON ai.customer_id = p.customer_id AND ai.group_id = p.group_id
INNER JOIN Personal_Information pi ON p.customer_id = pi.customer_id
INNER JOIN cards c ON c.customer_id = pi.customer_id
INNER JOIN transactions t ON c.customer_card_id = t.customer_card_id
INNER JOIN absolute_deviation si ON p.customer_id = si.customer_id AND p.group_id = si.group_id
INNER JOIN margin_calc mc ON p.customer_id = mc.customer_id AND p.group_id = mc.group_id
INNER JOIN discount_analysis da ON p.customer_id = da.customer_id AND p.group_id = da.group_id
INNER JOIN discount_Average davg ON p.customer_id = davg.customer_id AND p.group_id = davg.group_id
ORDER BY ai.customer_id, ai.group_id;

-- Тесты представления
SELECT * FROM Groups;

SELECT * FROM Groups
WHERE customer_id = 1 AND group_id = 7 OR customer_id = 3 AND group_id = 1;

SELECT * FROM Groups
WHERE group_margin < 0
ORDER BY group_margin;

SELECT * FROM Groups
WHERE group_affinity_index = 1
ORDER BY 1, 2;
