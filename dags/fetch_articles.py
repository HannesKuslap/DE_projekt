import json
import os
import re
import psycopg2
import glob
# from crossref.restful import Works
from psycopg2 import sql
from datetime import datetime, timedelta
from airflow.sensors.filesystem import FileSensor
from neo4jdb import Neo4jGraph
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.operators.empty import EmptyOperator

DEFAULT_ARGS = {
    'owner': 'Tartu',
    'depends_on_past': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=2)
}

DATA_FOLDER = '/tmp/data'

arxiv_data_dag = DAG(
    dag_id='article_data',  # name of dag
    schedule_interval='*/10 * * * *',  # execute once every 10 minutes
    start_date=datetime(2022, 9, 14, 9, 15, 0),
    catchup=False,  # in case execution has been paused, should it execute everything in between
    template_searchpath=DATA_FOLDER,  # the PostgresOperator will look for files in this folder
    default_args=DEFAULT_ARGS,  # args assigned to all operators
)


def get_jsons_in_folder(folder_path, file_extension):
    all_files = os.listdir(folder_path)
    selected_files = [file for file in all_files if file.endswith(file_extension)]
    return selected_files


def get_existing_files():
    """
    Read the list of existing files from a text file.
    """
    existing_files = set()

    if os.path.exists(DATA_FOLDER):
        with open(f"{DATA_FOLDER}/fileList.txt", 'r') as file:
            existing_files = set(file.read().splitlines())

    return existing_files


def update_existing_files(new_file):
    """
    Update the list of existing files in the text file.
    """
    with open(f"{DATA_FOLDER}/fileList.txt", 'a') as file:
        file.write(new_file + '\n')


def get_latest_file():
    list_of_files = glob.glob(DATA_FOLDER + '/*.json')
    latest_file = max(list_of_files, key=os.path.getctime)
    return latest_file


def check_if_file_is_new(**kwargs):
    file_to_check = get_latest_file()
    existing_files = get_existing_files()
    if file_to_check in existing_files:
        raise ValueError('File not parsed completely/correctly')
    else:
        update_existing_files(file_to_check)
        kwargs['ti'].xcom_push(key='latest_file', value=file_to_check)


"""""
def request_crossrefapi(doi):
    works = Works()
    try:
        response = [works.doi(doi).get(k) for k in ['is-referenced-by-count', 'subject', 'type', 'publisher']]
    except:
        response = None

    return response
"""""


################################################ INSERT YOUR IPV4 IP HERE, IDK WHY BUT LOCALHOST DOESNT WORK
def connect_to_PostgreSQL():
    conn = psycopg2.connect(
        host='192.168.1.136',
        user='airflow',
        password='airflow',
        database='airflow',
        port='5432'
    )
    return conn


def insert_data(**kwargs):
    # Use XCom to get the latest file from the 'new_files' task
    jsonfile = kwargs['ti'].xcom_pull(task_ids='new_files', key="latest_file")

    # Establish a connection to PostgreSQL
    conn = connect_to_PostgreSQL()

    # Create a cursor
    with conn.cursor() as cur:
        # Insert data into the tables
        with open(jsonfile, encoding="UTF8") as f:
            for data in f:
                for one_article in json.loads(data):

                    # Insert into the article table
                    insert_article_query = sql.SQL(
                        'INSERT INTO article (title, publication_date, doi) '
                        'VALUES (%s, %s, %s) RETURNING article_id'
                    )

                    # Execute the query and get the article_id
                    cur.execute(insert_article_query, (
                        one_article['title'], one_article['update_date'],
                        one_article['doi'],
                    ))
                    article_id = cur.fetchone()[0]

                    # Insert into the author table and link to the article
                    for author in one_article['authors_parsed']:
                        insert_author_query = sql.SQL(
                            'INSERT INTO author (first_name, last_name, middle_name) '
                            'VALUES (%s, %s, %s) RETURNING author_id'
                        )

                        # Execute the query and get the author_id
                        cur.execute(insert_author_query, (author[1], author[0], author[2],))
                        author_id = cur.fetchone()[0]

                        # Link the author to the article
                        cur.execute('INSERT INTO article_authors (author_id, article_id) VALUES (%s, %s)',
                                    (author_id, article_id))

                    # Insert into the journal table
                    insert_journal_query = sql.SQL(
                        'INSERT INTO journal (journal_name) '
                        'VALUES (%s) ON CONFLICT (journal_name) DO NOTHING RETURNING journal_id'
                    )
                    category = re.split(r'[:,]', str(one_article['journal-ref']))[0]
                    # Execute the query and get the journal_id
                    cur.execute(insert_journal_query, (category,))
                    journal_id = cur.fetchone()[0] if cur.rowcount > 0 else None

                    if journal_id is None:
                        cur.execute('SELECT journal_id FROM journal WHERE journal_name = %s',
                                    (category,))
                        journal_id = cur.fetchone()[0] if cur.rowcount > 0 else None

                    # Link the article to the journal
                    if journal_id is not None:
                        cur.execute('UPDATE article SET journal_id = %s WHERE article_id = %s',
                                    (journal_id, article_id))

                    # Insert into the license table
                    insert_license_query = sql.SQL(
                        'INSERT INTO license (license_type)'
                        'VALUES (%s) ON CONFLICT (license_type) DO NOTHING RETURNING license_id'
                    )

                    # Execute the query and get the license_id
                    cur.execute(insert_license_query, (one_article['license'],))
                    license_id = cur.fetchone()[0] if cur.rowcount >0 else None

                    if license_id is None:
                        # License already exists, fetch the existing license_id
                        cur.execute('SELECT license_id FROM license WHERE license_type = %s', (one_article['license'],))
                        license_id = cur.fetchone()[0] if cur.rowcount > 0 else None

                    # Link the article to the license
                    if license_id is not None:
                        cur.execute('UPDATE article SET license_id = %s WHERE article_id = %s', (license_id, article_id))


                    # Insert into the categories table
                    for category in one_article['categories'].split(" "):
                        insert_category_query = sql.SQL(
                            'INSERT INTO categories (category_name)'
                            'VALUES (%s) ON CONFLICT (category_name) DO NOTHING RETURNING category_id'
                        )

                        # Execute the query and get the category_id
                        cur.execute(insert_category_query, (category,))
                        category_id = cur.fetchone()[0] if cur.rowcount > 0 else None

                        if category_id is None:
                            # Category already exists, fetch the existing category_id
                            cur.execute('SELECT category_id FROM categories WHERE category_name = %s', (category,))
                            category_id = cur.fetchone()[0] if cur.rowcount > 0 else None

                        # Link the article to the category
                        if category_id is not None:
                            cur.execute('INSERT INTO article_categories (article_id, category_id) VALUES (%s, %s)',
                                        (article_id, category_id))

        # Commit the transaction
        conn.commit()

    # Close the connection
    conn.close()


def insert_to_graph(**kwargs):
    jsonfile = kwargs['ti'].xcom_pull(task_ids='new_files', key="latest_file")
    # Instantiate Neo4jGraph
    neo4j_graph = Neo4jGraph(uri="bolt://host.docker.internal:7687", auth=("neo4j", "Lammas123"))

    with open(jsonfile, 'r', encoding='utf-8') as file:
        data = json.load(file)

    # Process the data and create nodes and relationships
    for i in range(len(data)):
        author_names = data[i]['authors_parsed']
        author_nodes = [neo4j_graph.create_author_node(name) for name in author_names]

        article_node = neo4j_graph.create_article_node(data[i])

        for author_node in author_nodes:
            neo4j_graph.create_written_by_relationship(author_node, article_node)

    # Creating reference links after all article nodes are in the database
    # Json file shoud have field called "references":[doi, doi, doi]
    # for i in range(len(data)):
    # references = data[i].get('references')
    # if references:
    # article_node = neo4j_graph.graph.run(
    # "MATCH (a:Article {doi: $doi}) RETURN a",
    # doi=data[i]['doi']
    # ).evaluate()

    # neo4j_graph.create_references_relationships(article_node, references)


start = EmptyOperator(
    task_id='start',
    dag=arxiv_data_dag
)

create_articleTable = SQLExecuteQueryOperator(
    task_id='create_articleTable',
    dag=arxiv_data_dag,
    trigger_rule='none_failed',
    conn_id='airflow_pg',
    sql='articleTable.sql',
    autocommit=True
)
create_categoriesTable = SQLExecuteQueryOperator(
    task_id='create_categoriesTable',
    dag=arxiv_data_dag,
    trigger_rule='none_failed',
    conn_id='airflow_pg',
    sql='categoriesTable.sql',
    autocommit=True
)
create_journalTable = SQLExecuteQueryOperator(
    task_id='create_journalTable',
    dag=arxiv_data_dag,
    trigger_rule='none_failed',
    conn_id='airflow_pg',
    sql='journalTable.sql',
    autocommit=True
)
create_licenseTable = SQLExecuteQueryOperator(
    task_id='create_licenseTable',
    dag=arxiv_data_dag,
    trigger_rule='none_failed',
    conn_id='airflow_pg',
    sql='licenseTable.sql',
    autocommit=True
)

create_authorTable = SQLExecuteQueryOperator(
    task_id='create_authorTable',
    dag=arxiv_data_dag,
    trigger_rule='none_failed',
    conn_id='airflow_pg',
    sql='authorTable.sql',
    autocommit=True
)

create_articleAuthorTable = SQLExecuteQueryOperator(
    task_id='create_articleAuthorTable',
    dag=arxiv_data_dag,
    trigger_rule='none_failed',
    conn_id='airflow_pg',
    sql='authorArticleTable.sql',
    autocommit=True
)

create_articleCategoriesTable = SQLExecuteQueryOperator(
    task_id='create_articleCategoriesTable',
    dag=arxiv_data_dag,
    trigger_rule='none_failed',
    conn_id='airflow_pg',
    sql='articleCategoriesTable.sql',
    autocommit=True
)

sense_file = FileSensor(
    task_id="wait_for_file",
    dag=arxiv_data_dag,
    fs_conn_id="airflow_pg",
    filepath="/tmp/data/*.json",
    poke_interval=60 * 1,
    timeout=300
)

start >> [create_authorTable, create_journalTable, create_licenseTable, create_categoriesTable] >> create_articleTable
create_articleTable >> [create_articleAuthorTable, create_articleCategoriesTable] >> sense_file

new_files = PythonOperator(
    task_id="new_files",
    dag=arxiv_data_dag,
    python_callable=check_if_file_is_new,
    trigger_rule='none_failed'
)

ingest_file = PythonOperator(
    task_id="process_file",
    dag=arxiv_data_dag,
    trigger_rule="none_failed",
    python_callable=get_latest_file
)

populate_tables = PythonOperator(
    task_id=f'populate_tables',
    dag=arxiv_data_dag,
    trigger_rule='all_done',
    python_callable=insert_data
)

populate_graph = PythonOperator(
    task_id=f'populate_graph',
    dag=arxiv_data_dag,
    trigger_rule='all_done',
    python_callable=insert_to_graph
)
sense_file >> new_files >> ingest_file

ingest_file >>  populate_graph

