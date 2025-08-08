from sqlalchemy import Column, Integer, String, DateTime, Boolean
from sqlalchemy.orm import relationship
from database.models import log_produto, log_estoque, log_pedido
from database.database import Base

class Log(Base):
    __tablename__ = "log"

    id = Column(Integer, primary_key=True, index=True)
    dh_execucao = Column(DateTime(timezone=True))
    contexto = Column(String)
    de = Column(String)
    para = Column(String)
    sucesso = Column(Boolean)

    produtos = relationship("LogProduto", back_populates="logs", cascade="all, delete-orphan")
    estoques = relationship("LogEstoque", back_populates="logs", cascade="all, delete-orphan")
    pedidos = relationship("LogPedido", back_populates="logs", cascade="all, delete-orphan")
