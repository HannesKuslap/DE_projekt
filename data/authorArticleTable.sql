CREATE TABLE IF NOT EXISTS article_authors
(
    article_id  INTEGER REFERENCES article(article_id),
    author_id  INTEGER REFERENCES author(author_id),
    PRIMARY KEY (article_id, author_id)
);