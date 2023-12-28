CREATE TABLE IF NOT EXISTS article
(
    article_id      SERIAL PRIMARY KEY,
    submitter       TEXT,
    title           TEXT,
    comments        TEXT,
    journal_ref     TEXT,
    doi             TEXT,
    report_no       TEXT,
    categories      TEXT,
    license         TEXT,
    abstract        TEXT
);
