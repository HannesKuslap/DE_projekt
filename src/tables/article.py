import uuid

from sqlalchemy import Column, BigInteger, String, Date, UUID

from src.database import Base

class Article(Base):
    __tablename__ = "article"

    # Igal tabelil on ID
    id = Column(UUID(), primary_key= True, default= uuid.uuid4)

    authors = Column(String(4096))
    publication_year = Column(Date())
    journal_reference = Column(String(1024))
    categories = Column(String(128))