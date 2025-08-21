from sqlalchemy import Column, Integer, String, DateTime, ForeignKey, Boolean, CheckConstraint
from sqlalchemy.sql import func, text
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

class LogPedido(Base):
    __tablename__ = "log_pedido"

    __table_args__ = (
        CheckConstraint("evento IN ('I', 'C', 'F')", name='ck_log_pedido_evento'),
    )

    id = Column(Integer, primary_key=True, index=True)
    log_id = Column(Integer, ForeignKey("log.id", ondelete="CASCADE"))
    id_loja = Column(Integer)
    id_pedido = Column(Integer)
    pedido_ecommerce = Column(String)
    id_nota = Column(Integer, nullable=True)
    nunota_pedido = Column(Integer, nullable=True)
    nunota_nota = Column(Integer, nullable=True)    
    dh_atualizacao = Column(DateTime(timezone=True), server_default=func.now())
    evento = Column(String, nullable=False)
    status = Column(Boolean, default=True)
    obs = Column(String, nullable=True)

    logs = relationship("Log", back_populates="pedidos")

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

class Produto(Base):
    __tablename__ = "produto"

    id = Column(Integer, primary_key=True, index=True, autoincrement=True)
    cod_snk = Column(Integer, nullable=False, unique=True) 
    cod_olist = Column(Integer, nullable=True)
    dh_cadastro = Column(DateTime(timezone=True), nullable=False)
    dh_atualizado = Column(DateTime(timezone=True), nullable=True)
    pendencia = Column(Boolean, default=False)

class TokenOlist(Base):
    __tablename__ = "token_olist"

    id = Column(Integer, primary_key=True, index=True)
    dh_solicitacao = Column(DateTime(timezone=True),server_default=func.now())
    token_criptografado = Column(String, nullable=False)
    dh_expiracao_token = Column(DateTime(timezone=True), nullable=False)
    refresh_token_criptografado = Column(String, nullable=False)
    dh_expiracao_refresh_token = Column(DateTime(timezone=True), nullable=False)
    id_token_criptografado = Column(String, nullable=False)

class TokenSankhya(Base):
    __tablename__ = "token_sankhya"

    id = Column(Integer, primary_key=True, index=True)
    dh_solicitacao = Column(DateTime(timezone=True),server_default=func.now())
    token_criptografado = Column(String, nullable=False)
    dh_expiracao_token = Column(DateTime(timezone=True), nullable=False)

class Venda(Base):
    __tablename__ = "venda"

    id = Column(Integer, primary_key=True, autoincrement=True)
    id_loja = Column(Integer, nullable=False)
    dh_pedido = Column(DateTime(timezone=True),server_default=func.now())
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
    nunota_pedido = Column(Integer, nullable=True)
    dh_importacao_pedido_snk = Column(DateTime(timezone=True), nullable=True)
    dh_confirmacao_pedido_snk = Column(DateTime(timezone=True), nullable=True)
    dh_faturamento_snk = Column(DateTime(timezone=True), nullable=True)
    nunota_nota = Column(Integer, nullable=True)
    dh_confirmacao_nota_snk = Column(DateTime(timezone=True), nullable=True)