from sqlalchemy import Column, Integer, String, DateTime, ForeignKey, Boolean
from sqlalchemy.sql import func
from sqlalchemy.orm import relationship
from database.database import Base

class LogProduto(Base):
    __tablename__ = "log_produtos"

    id = Column(Integer, primary_key=True, index=True)
    log_id = Column(Integer, ForeignKey("log.id", ondelete="CASCADE"))
    codprod = Column(Integer)
    idprod = Column(Integer)
    dh_atualizacao = Column(DateTime(timezone=True), server_default=func.now())
    campo = Column(String, nullable=True)
    valor_old = Column(String, nullable=True)
    valor_new = Column(String, nullable=True)
    sucesso = Column(Boolean, default=True)
    obs = Column(String, nullable=True)

    logs = relationship("Log", back_populates="produtos")