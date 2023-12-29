# How to run the project

1.  Open Neo4j desktop, start a new project and add a new local DBMS, setting the name to anything and password to Lammas123

2.  Click on the created DBMS and under Plugins tab install Graph Data Science Library, then start the local DBMS

3.  In /dags/fetch_articles.py find the function connect_to_PostgreSQL() and insert your IPv4 address under 'host'.

4.  Run 'docker compose up' in the project root

5.  After the compose up has finished, open localhost:8080 in your browser, userame and password are both 'airflow'

6.  Under dropdown menu Admin, choose Connections and click the '+' sign to add a new connection

7.  Insert the following values: ID - 'airflow_pg', type - 'Postgres', host - 'postgres', schema - 'airflow', login - 'airflow', password - 'airflow'. Port 5432

8.  Press 'test' down below to test the connection, if successful, press 'save'

9.  Go to DAGs and run article_data DAG to start the pipeline