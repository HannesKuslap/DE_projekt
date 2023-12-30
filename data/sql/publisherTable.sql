CREATE TABLE IF NOT EXISTS publisher
(
    publisher_id       SERIAL PRIMARY KEY,
    publisher_name     TEXT UNIQUE
);