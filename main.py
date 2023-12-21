import json
import uuid
from scholarly import scholarly

from src import Article, Author
from src.database import create_tables, session, drop_tables
from src.tables.article_author import ArticleAuthor
from sqlalchemy import select, and_

from src.tables.article_category import ArticleCategory
from src.tables.category import Category


def read_json(line):
    one_article = json.loads(line)
    return one_article

def create_article_table(one_article):
    article = Article()
    article.id = uuid.uuid4()
    article.authors = one_article['authors']
    article.journal_reference = one_article['journal-ref']
    article.publication_year = one_article['versions'][0]['created']
    article.categories = one_article['categories']
    session.add(article)
    return article

def create_category_table(category_name):
    existing_category = session.execute(
        select(Category).where(Category.name == category_name)).first()

    if existing_category is None:
        category = Category()
        category.id = uuid.uuid4()
        category.name = category_name
        session.add(category)
        return category
    else:
        return existing_category[0]

def create_article_category_table(article_table, category_table):
    article_category = ArticleCategory()
    article_category.id = uuid.uuid4()
    article_category.category_id = category_table.id
    article_category.article_id = article_table.id
    session.add(article_category)

def get_hindex(firstname, lastname):
    search_query = scholarly.search_author(str(firstname) + " " + str(lastname))
    try:
        scholar_author = scholarly.fill(next(search_query), sections=['indices'])
        hindex = scholar_author['hindex']
        return int(hindex)
    except:
        return None

def load_data(jsonfile):
    with open(jsonfile, "r") as json_file:
        n = 0
        for line in json_file:
            one_article = read_json(line)
            article_table = create_article_table(one_article)
            for split_categories in one_article['categories'].split(" "):
                category_table = create_category_table(split_categories)
            create_article_category_table(article_table, category_table)

            for name in one_article['authors_parsed']:
                article_author = ArticleAuthor()

                existing_author_id = session.execute(
                    select(Author.id).where(
                        and_(Author.firstname == name[1],
                             Author.lastname == name[0]))).first()
                if existing_author_id is None:
                    author = Author()
                    author.firstname = name[1]
                    author.lastname = name[0]
                    author.hindex = get_hindex(name[1], name[0])
                    author.id = uuid.uuid4()
                    article_author.author_id = author.id
                    session.add(author)

                else:
                    article_author.author_id = existing_author_id[0]

                article_author.article_id = article_table.id
                session.add(article_author)

            session.commit()
            n += 1
            if n == 10000:
                break

# Tavaliselt on main kõige all
if __name__ == '__main__':
    drop_tables()
    create_tables()
    load_data("arxiv-metadata-oai-snapshot.json")
    select(Category.name)

