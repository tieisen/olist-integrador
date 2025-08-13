from sqlalchemy import Column, Integer, String, DateTime
from database.database import Base

class Venda(Base):
    __tablename__ = "venda"

    id = Column(Integer, primary_key=True, index=True, autoincrement=True)
    id_loja = Column(Integer, nullable=False)
    dh_pedido = Column(DateTime(timezone=True))
    id_pedido = Column(Integer, nullable=False, unique=True)
    cod_pedido = Column(String, nullable=False)
    num_pedido = Column(Integer, nullable=False)
    id_separacao = Column(Integer, nullable=True)
    num_nota = Column(Integer, nullable=True)
    id_nota = Column(Integer, nullable=True)
    dh_nota_emissao = Column(DateTime(timezone=True), nullable=True)
    id_financeiro = Column(Integer, nullable=True)
    dh_baixa_financeiro = Column(DateTime(timezone=True), nullable=True)
    dh_cancelamento_pedido = Column(DateTime(timezone=True), nullable=True)
    dh_cancelamento_nota = Column(DateTime(timezone=True), nullable=True)
    nunota_pedido = Column(Integer, nullable=True, unique=True)
    dh_importacao_pedido_snk = Column(DateTime(timezone=True), nullable=True)
    dh_confirmacao_pedido_snk = Column(DateTime(timezone=True), nullable=True)
    dh_faturamento_snk = Column(DateTime(timezone=True), nullable=True)
    nunota_nota = Column(Integer, nullable=True)
    dh_confirmacao_nota_snk = Column(DateTime(timezone=True), nullable=True)


    
    