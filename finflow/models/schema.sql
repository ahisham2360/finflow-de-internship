CREATE TABLE dim_transaction_type (
    id INTEGER PRIMARY KEY,
    type_name VARCHAR
);

CREATE TABLE dim_account (
    id INTEGER PRIMARY KEY,
    name VARCHAR
);

CREATE TABLE dim_time (
    step INTEGER PRIMARY KEY,
    sim_day INTEGER,
    sim_week INTEGER,
    hour_of_day INTEGER
);

CREATE TABLE complaints (
    complaint_id INTEGER PRIMARY KEY,
    date_received TIMESTAMP,
    product VARCHAR,
    sub_product VARCHAR,
    issue VARCHAR,
    company VARCHAR,
    state VARCHAR,
    resolution VARCHAR
);

CREATE TABLE fact_transactions (
    transaction_id INTEGER PRIMARY KEY,
    step INTEGER REFERENCES dim_time(step),
    transaction_type_id INTEGER REFERENCES dim_transaction_type(id),
    amount DOUBLE,
    log_amount DOUBLE,
    balance_drain DOUBLE,
    sender_account_id INTEGER REFERENCES dim_account(id),
    receiver_account_id INTEGER REFERENCES dim_account(id),
    is_fraud BOOLEAN,
    is_flagged_fraud BOOLEAN,
    old_balance_sender DOUBLE,
    new_balance_sender DOUBLE,
    old_balance_receiver DOUBLE,
    new_balance_receiver DOUBLE
);