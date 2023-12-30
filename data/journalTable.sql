CREATE TABLE IF NOT EXISTS journal
(
    journal_id       SERIAL PRIMARY KEY,
    journal_name     TEXT UNIQUE
);