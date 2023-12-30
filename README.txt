# How to run the project

1.  In /dags/fetch_articles.py find the function connect_to_PostgreSQL() and insert your IPv4 address under 'host'.

2.  Run 'docker compose up' in the project root

3.  After the compose up has finished, open localhost:8080 in your browser, userame and password are both 'airflow'

4.  Under dropdown menu Admin, choose Connections and click the '+' sign to add a new connection

5.  Insert the following values: ID - 'airflow_pg', type - 'Postgres', host - 'postgres', schema - 'airflow', login - 'airflow', password - 'airflow'. Port 5432

6.  Press 'test' down below to test the connection, if successful, press 'save'

7.  Go to DAGs and run article_data DAG to start the pipeline

8.  Open localhost:7474 to open neo4j. Connection - choose bolt and use this url after it: host.docker.internal:7687, username - neo4j, password - Lammas123

9.  To open pgadmin, type localhost:5050 into your browser. Email - admin@admin.com, password - root