import uuid

from sqlalchemy import Column, BigInteger, String, Date, ForeignKey, UUID
from sqlalchemy.orm import relationship

from src.database import Base

class ArticleCategory(Base):
    __tablename__ = "article&category"

    # Igal tabelil on ID
    id = Column(UUID(), primary_key= True, default= uuid.uuid4)

    category_id = Column(UUID(), ForeignKey("category.id"))
    article_id = Column(UUID(), ForeignKey("article.id"))

    category = relationship("Category")
    article = relationship("Article")


