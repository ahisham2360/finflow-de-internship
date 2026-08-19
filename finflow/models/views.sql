CREATE VIEW v_monthly_volume AS
SELECT
    FLOOR((t.sim_week-1) / 4) + 1 AS sim_month,
    tt.type_name AS transaction_type,
    SUM(f.amount) AS total_transaction_amount,
    COUNT(*) AS transaction_count
FROM fact_transactions f
JOIN dim_transaction_type tt
    ON f.transaction_type_id = tt.id
JOIN dim_time t
    ON f.step = t.step
GROUP BY
    FLOOR((t.sim_week-1) / 4) + 1,
    tt.type_name;



CREATE VIEW v_fraud_by_type AS
SELECT
    tt.type_name AS transaction_type,
    SUM(f.is_fraud) AS fraud_count,
    COUNT(*) AS total_count,
    SUM(f.is_fraud) * 1.0 / COUNT(*) AS fraud_rate
FROM fact_transactions f
JOIN dim_transaction_type tt
    ON f.transaction_type_id = tt.id
GROUP BY tt.type_name;



CREATE VIEW v_monthly_complaints AS
SELECT
    product,
    strftime(date_received, '%Y-%m') AS year_month,
    COUNT(*) AS complaint_count
FROM complaints
GROUP BY
    product,
    strftime(date_received, '%Y-%m');



CREATE VIEW v_balance_anomalies AS
SELECT
    transaction_id,
    transaction_type_id
FROM fact_transactions
WHERE
    ABS(balance_drain) > 1
    AND is_fraud = FALSE;