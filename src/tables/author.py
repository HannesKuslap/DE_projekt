import uuid

from sqlalchemy import Column, BigInteger, String, UUID, Integer

from src.database import Base

class Author(Base):
    __tablename__ = "author"

    # Igal tabelil on ID, uuid tekitab unique id
    id = Column(UUID(), primary_key= True, default= uuid.uuid4)

    firstname = Column(String(100))
    lastname = Column(String(100))
    hindex = Column(Integer(), nullable=True)

