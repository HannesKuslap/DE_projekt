import uuid

from sqlalchemy import Column, BigInteger, String, Date, UUID

from src.database import Base

class Category(Base):
    __tablename__ = "category"

    # Igal tabelil on ID
    id = Column(UUID(), primary_key= True, default= uuid.uuid4)

    name = Column(String(128))