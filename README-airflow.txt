Kuidas airflow tööle saada?

1. Vana hea 'docker compose up' (nüüd on uus compose.yaml)
2. Seejärel brauseris 'localhost:8080'. (võtab veidi aega) Password ja username on mõlemad: 'airflow'
3. Üleval valikutest vali Admin -> Connections ning vajuta '+' märki.
4. Järgnevad lahtrid on ID - 'airflow_pg', type - 'Postgres', host - 'postgres', schema - 'airflow', login - 'airflow', password - 'airflow'. Port on 5432.
5. Keri alla ja vajuta 'test', kui on korras, siis 'save'.
6. Peamine .py fail on nüüdsest /dags/fetch_articles.py. Mine sinna faili ning sul tuleb kahes kohas täita 'lüngad'.
7. Otsi üles koht kuhu pead oma ipv4 kirjutama ning otsi üles koht kuhu pead oma kaggle api andmed sisestama.
8. Kui need on tehtud saad jooksutada koodi airflow -> DAGs -> article_data -> play nupp.
9. Kui kasutada PyCharm, siis on võimalik ühendada see database'iga ning on võimalik näha tabeleid. Kui ei ole, siis vaata moodle's practice session 01 alates ~35:00.
