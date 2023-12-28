import json
from datetime import datetime, timedelta
from neo4jdb import Neo4jGraph

import psycopg2
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator

# from airflow.providers.postgres.transfers.json_to_postgres import JsonToPostgresOperator


DEFAULT_ARGS = {
    'owner': 'Tartu',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=2)
}
# Not sure how to import automatically, it requires a login. Make sure kaggle.json (account API) is in /users/.kaggle
DATASET_NAME = "Cornell-University/arxiv"  # 'https://www.kaggle.com/datasets/Cornell-University/arxiv/data'

DATA_FOLDER = '/tmp/data'

arxiv_data_dag = DAG(
    dag_id='article_data',  # name of dag
    schedule_interval='0 0 * * 0',  # execute once a month
    start_date=datetime(2022, 9, 14, 9, 15, 0),
    catchup=False,  # in case execution has been paused, should it execute everything in between
    template_searchpath=DATA_FOLDER,  # the PostgresOperator will look for files in this folder
    default_args=DEFAULT_ARGS,  # args assigned to all operators
)

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


def insert_data(jsonfile, **kwargs):
    conn = connect_to_PostgreSQL()
    cur = conn.cursor()

    # Insert data into the table
    with open(jsonfile, 'r') as f:
        for line in f:
            one_article = json.loads(line)
            cur.execute(
                'INSERT INTO article (submitter,title,comments,journal_ref,doi,report_no,categories,license,abstract) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)',
                (one_article['submitter'], one_article['title'], one_article['comments'], one_article['journal-ref'],
                 one_article['doi'], one_article['report-no'], one_article['categories'], one_article['license'],
                 one_article['abstract'],))

            cur.execute('SELECT LASTVAL()')
            article_id = cur.fetchone()[0]
            for author in one_article['authors_parsed']:
                cur.execute('INSERT INTO author (first_name, last_name, middle_name) VALUES (%s, %s, %s)',
                            (author[0], author[1], author[2],))
                cur.execute('SELECT LASTVAL()')
                author_id = cur.fetchone()[0]
                if article_id is not None and author_id is not None:
                    cur.execute('INSERT INTO article_authors (author_id, article_id) VALUES (%s, %s)',
                                (author_id, article_id))
            conn.commit()

    # Close the connections
    cur.close()
    conn.close()

def insert_to_graph(jsonfile, **kwargs):
    # Instantiate Neo4jGraph
    neo4j_graph = Neo4jGraph(uri="bolt://localhost:7687", auth=("neo4j", "Lammas123"))

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
    #for i in range(len(data)):
        #references = data[i].get('references')
        #if references:
            #article_node = neo4j_graph.graph.run(
                #"MATCH (a:Article {doi: $doi}) RETURN a",
                #doi=data[i]['doi']
            #).evaluate()

            #neo4j_graph.create_references_relationships(article_node, references)

            

################################################ INSERT YOUR KAGGLE.JSON USERNAME AND PASSWORD HERE SO IT CAN DOWNLOAD
################################################ THE DATASET AUTOMATICALLY
ingest_data = BashOperator(
    task_id='ingest_data',
    dag=arxiv_data_dag,
    trigger_rule='none_failed',
    bash_command="pip install kaggle && export KAGGLE_USERNAME=karlerikk && export "
                 "KAGGLE_KEY=066a3046434375f98aa99de687292b61 && kaggle datasets download -d "
                 "'Cornell-University/arxiv' -p '/tmp/data'"
)

create_articleTable = PostgresOperator(
    task_id='create_articleTable',
    dag=arxiv_data_dag,
    trigger_rule='none_failed',
    postgres_conn_id='airflow_pg',
    sql='articleTable.sql',
    autocommit=True
)

create_authorTable = PostgresOperator(
    task_id='create_authorTable',
    dag=arxiv_data_dag,
    trigger_rule='none_failed',
    postgres_conn_id='airflow_pg',
    sql='authorTable.sql',
    autocommit=True
)

create_articleAuthorTable = PostgresOperator(
    task_id='create_articleAuthorTable',
    dag=arxiv_data_dag,
    trigger_rule='none_failed',
    postgres_conn_id='airflow_pg',
    sql='authorArticleTable.sql',
    autocommit=True
)

ingest_data >> create_articleTable >> create_authorTable >> create_articleAuthorTable

populate_tables = PythonOperator(
    task_id='populate_tables',
    dag=arxiv_data_dag,
    trigger_rule='none_failed',
    python_callable=insert_data,
    op_kwargs={
        'jsonfile': DATA_FOLDER + "/arxiv-metadata-oai-snapshot.json"
    }
)

create_authorTable >> populate_tables

populate_graph = PythonOperator(
    task_id='populate_graph',
    dag=arxiv_data_dag,
    trigger_rule='none_failed',
    python_callable=insert_data,
    op_kwargs={
        'jsonfile': DATA_FOLDER + "/arxiv-metadata-oai-snapshot.json"
    }
)

create_authorTable >> populate_graph

"""insert_to_db = PostgresOperator(
    task_id='insert_to_db',
    dag=arxiv_data_dag,
    postgres_conn_id='airflow_pg',
    trigger_rule='none_failed',
    sql= ,
    autocommit=True,
)

transform_data >> insert_to_db"""
