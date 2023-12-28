CREATE TABLE IF NOT EXISTS author
(
    author_id       SERIAL PRIMARY KEY,
    first_name      TEXT,
    last_name       TEXT,
    middle_name     TEXT
);