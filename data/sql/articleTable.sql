CREATE TABLE IF NOT EXISTS article
(
    article_id              SERIAL PRIMARY KEY,
    title                   TEXT,
    publication_date        TEXT,
    doi                     TEXT,
    reference_by_count      TEXT,
    publisher_id    INTEGER REFERENCES publisher(publisher_id),
    license_id      INTEGER REFERENCES license(license_id)
);
