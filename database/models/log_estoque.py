from sqlalchemy import Column, Integer, String, DateTime, ForeignKey, Boolean
from sqlalchemy.sql import func
from sqlalchemy.orm import relationship
from database.database import Base

class LogEstoque(Base):
    __tablename__ = "log_estoque"

    id = Column(Integer, primary_key=True, index=True)
    log_id = Column(Integer, ForeignKey("log.id", ondelete="CASCADE"))
    codprod = Column(Integer)
    idprod = Column(Integer)
    dh_atualizacao = Column(DateTime(timezone=True), server_default=func.now())
    qtdmov = Column(Integer)
    status_estoque = Column(Boolean, default=True)
    status_lotes = Column(Boolean, nullable=True)
    obs = Column(String, nullable=True)

    logs = relationship("Log", back_populates="estoques")