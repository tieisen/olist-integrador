from sqlalchemy import Column, String, DateTime, Integer
from database.database import Base

class TokenSankhya(Base):
    __tablename__ = "token_sankhya"

    id = Column(Integer, primary_key=True, index=True)
    dh_solicitacao = Column(DateTime(timezone=True))
    token_criptografado = Column(String, nullable=False)
    dh_expiracao_token = Column(DateTime(timezone=True), nullable=False)

