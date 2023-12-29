CREATE TABLE IF NOT EXISTS article
(
    article_id              SERIAL PRIMARY KEY,
    title                   TEXT,
    publication_date        TEXT,
    doi                     TEXT,
    journal_id INTEGER REFERENCES journal(journal_id),
    license_id INTEGER REFERENCES license(license_id)
);
