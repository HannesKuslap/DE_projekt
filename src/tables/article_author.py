import uuid

from sqlalchemy import Column, BigInteger, String, Date, ForeignKey, UUID
from sqlalchemy.orm import relationship

from src.database import Base

class ArticleAuthor(Base):
    __tablename__ = "article&author"

    # Igal tabelil on ID
    id = Column(UUID(), primary_key= True, default= uuid.uuid4)

    author_id = Column(UUID(), ForeignKey("author.id"))
    article_id = Column(UUID(), ForeignKey("article.id"))

    author = relationship("Author")
    article = relationship("Article")


