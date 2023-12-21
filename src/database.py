from sqlalchemy import MetaData
from sqlalchemy.orm import declarative_base
import sqlalchemy as db
from sqlalchemy.orm import Session

#engine puhul on vajalik muuta sulgude sees olevat stringi.
#Vaatab seda stringi tükk haaval: admin:root@192.168.1.86:5432/project
#Tegemist on nüüd postgres-iga. 'admin:root' on vastavalt 'username:password', need saad compose.yaml failist
#'192.168.1.86' on sinu ip ja '5432' on port compose.yaml failist.
#'project' on serveri mingi database-i nimi, mille sa tekitasid pgAdmin leheküljel.
engine = db.create_engine("postgresql+psycopg2://admin:root@192.168.1.86:5432/project")

conn = engine.connect()
Base = declarative_base()

session = Session(engine)

meta = MetaData()

def create_tables():
    Base.metadata.create_all(engine)

def drop_tables():
    Base.metadata.drop_all(engine)