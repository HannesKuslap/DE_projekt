CREATE TABLE IF NOT EXISTS article_categories
(
    article_id      INTEGER REFERENCES article(article_id),
    category_id     INTEGER REFERENCES categories(category_id),
    PRIMARY KEY (article_id, category_id)
);