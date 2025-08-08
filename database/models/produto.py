from sqlalchemy import Column, Integer, DateTime, Boolean
from database.database import Base

class Produto(Base):
    __tablename__ = "produto"

    id = Column(Integer, primary_key=True, index=True, autoincrement=True)
    cod_snk = Column(Integer, nullable=False, unique=True) 
    cod_olist = Column(Integer, nullable=True)
    dh_cadastro = Column(DateTime(timezone=True), nullable=False)
    dh_atualizado = Column(DateTime(timezone=True), nullable=True)
    pendencia = Column(Boolean, default=False)    
    