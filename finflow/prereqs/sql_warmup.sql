-- 1
-- Calculates the total transaction volume for each transaction type.
-- Helps identify which types of transactions are most common and require monitoring and fraud detection.
-- Here, TRANSFER CASH_IN are the most common transaction types, so they should be monitored more closely for fraud detection.
SELECT type, SUM(amount) AS transaction_volume
FROM transactions
GROUP BY type;



-- 2
-- Calculates the 90th percentile of transaction amounts for each transaction type.
-- Helps identify a threshold amount for transactions, useful for anomaly detection.
SELECT type, QUANTILE_CONT(amount, 0.9) AS "90th_percentile"
FROM transactions
GROUP BY type;



-- 3
-- Identifies sender accounts with more than 3 transactions.
-- Zero rows were returned, which is expected because the dataset is small so most accounts have only 1 transaction.
-- However, in a full dataset, it is critical for spotting automated bots or account takeovers.
SELECT nameOrig
FROM transactions
GROUP BY nameOrig
HAVING COUNT(*) > 3;



-- 5
-- Calculates the percentage of fraud transactions in full drain transactions.
-- 4% were fraud, which is a high percentage, so full drain transactions are more likely to be fraud.
SELECT SUM(isFraud) / COUNT(*) * 100.0 AS fraud_percentage
FROM transactions
WHERE newbalanceOrig = 0 AND oldbalanceOrg > 0;



-- 4
-- Calculates the cumulative total of TRANSFER amounts for each step (1 hour).
-- Helps identify periods where transfer activity increases sharply.
-- Here, step 7 increased much more than the others, which may indicate suspicious or unusual behavior.
SELECT step, SUM(amount_per_step) OVER (ORDER BY step) AS Running_Cumulative_Total
FROM (
    SELECT step, SUM(amount) AS amount_per_step
    FROM transactions
    WHERE type = 'TRANSFER'
    GROUP BY step
) AS step_totals
ORDER BY step;