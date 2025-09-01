from sqlalchemy import Column, Integer, String, DateTime, ForeignKey, Boolean, CheckConstraint
from sqlalchemy.sql import text
from sqlalchemy.orm import relationship
from database.database import Base

class Empresa(Base):    
    __tablename__ = "empresa"

    id = Column(Integer, primary_key=True, index=True)
    codigo_snk = Column(Integer, nullable=False)
    nome = Column(String, nullable=False)
    cnpj = Column(String, nullable=False)
    serie_nfe = Column(String, nullable=False)
    client_id = Column(String, nullable=False)
    client_secret = Column(String, nullable=False)
    admin_email = Column(String, nullable=False)
    admin_senha = Column(String, nullable=False)
    status = Column(Boolean, default=True)
    
    log = relationship("Log", back_populates="empresa", cascade="all, delete-orphan")
    produto = relationship("Produto", back_populates="empresa", cascade="all, delete-orphan")
    olist = relationship("Olist", back_populates="empresa", cascade="all, delete-orphan")
    sankhya = relationship("Sankhya", back_populates="empresa", cascade="all, delete-orphan")
    ecommerce = relationship("Ecommerce", back_populates="empresa", cascade="all, delete-orphan")
    pedido = relationship("Pedido", back_populates="empresa", cascade="all, delete-orphan")

class Produto(Base):
    __tablename__ = "produto"

    id = Column(Integer, primary_key=True, index=True, autoincrement=True)
    cod_snk = Column(Integer, nullable=False) 
    cod_olist = Column(Integer, nullable=True)
    dh_cadastro = Column(DateTime(timezone=True), nullable=False, server_default=text('CURRENT_DATE'))
    dh_atualizado = Column(DateTime(timezone=True), nullable=True, onupdate=text('CURRENT_DATE'))
    pendencia = Column(Boolean, default=False)
    empresa_id = Column(Integer, ForeignKey("empresa.id"), nullable=False)

    empresa = relationship("Empresa", back_populates="produto")
    estoque = relationship("Estoque", back_populates="produto")
    log_produto = relationship("LogProduto", back_populates="produto")
    log_estoque = relationship("LogEstoque", back_populates="produto")

class Olist(Base):
    __tablename__ = "olist"

    id = Column(Integer, primary_key=True, index=True)
    dh_solicitacao = Column(DateTime(timezone=True),server_default=text('CURRENT_DATE'))
    token_criptografado = Column(String, nullable=False)
    dh_expiracao_token = Column(DateTime(timezone=True), nullable=False)
    refresh_token_criptografado = Column(String, nullable=False)
    dh_expiracao_refresh_token = Column(DateTime(timezone=True), nullable=False)
    id_token_criptografado = Column(String, nullable=False)
    empresa_id = Column(Integer, ForeignKey("empresa.id"), nullable=False)

    empresa = relationship("Empresa", back_populates="olist")

class Sankhya(Base):
    __tablename__ = "sankhya"

    id = Column(Integer, primary_key=True, index=True)
    dh_solicitacao = Column(DateTime(timezone=True),server_default=text('CURRENT_DATE'))
    token_criptografado = Column(String, nullable=False)
    dh_expiracao_token = Column(DateTime(timezone=True), nullable=False)
    empresa_id = Column(Integer, ForeignKey("empresa.id"), nullable=False)

    empresa = relationship("Empresa", back_populates="sankhya")

class Ecommerce(Base):
    __tablename__ = "ecommerce"

    id = Column(Integer, primary_key=True, index=True)
    id_loja = Column(Integer, nullable=False)
    nome = Column(String, nullable=False)
    status = Column(Boolean, default=True)
    empresa_id = Column(Integer, ForeignKey("empresa.id"), nullable=False)

    empresa = relationship("Empresa", back_populates="ecommerce")

class Pedido(Base):
    __tablename__ = "pedido"

    id = Column(Integer, primary_key=True, autoincrement=True)
    id_pedido = Column(Integer, nullable=False, unique=True)
    dh_pedido = Column(DateTime(timezone=True),server_default=text('CURRENT_DATE'))    
    cod_pedido = Column(String, nullable=False)
    num_pedido = Column(Integer, nullable=False)
    dh_cancelamento_pedido = Column(DateTime(timezone=True), nullable=True)
    id_separacao = Column(Integer, nullable=True)
    nunota_pedido = Column(Integer, nullable=True)
    dh_importacao_pedido = Column(DateTime(timezone=True), nullable=True)
    dh_confirmacao_pedido = Column(DateTime(timezone=True), nullable=True)
    ecommerce_id = Column(Integer, ForeignKey("ecommerce.id"), nullable=False)

    ecommerce = relationship("Ecommerce", back_populates="venda")
    nota = relationship("Nota", back_populates="pedido", cascade="all, delete-orphan")

class Nota(Base):
    __tablename__ = "nota"

    id = Column(Integer, primary_key=True, autoincrement=True)
    id_nota = Column(Integer, nullable=True)
    dh_nota_emissao = Column(DateTime(timezone=True), nullable=True)
    num_nota = Column(Integer, nullable=True)
    id_financeiro = Column(Integer, nullable=True)
    dh_baixa_financeiro = Column(DateTime(timezone=True), nullable=True)
    dh_faturamento = Column(DateTime(timezone=True), nullable=True)
    nunota_nota = Column(Integer, nullable=True)
    dh_confirmacao_nota = Column(DateTime(timezone=True), nullable=True)
    dh_cancelamento_nota = Column(DateTime(timezone=True), nullable=True)
    pedido_id = Column(Integer, ForeignKey("pedido.id"), nullable=False)

    pedido = relationship("Pedido", back_populates="nota")

class LogEstoque(Base):
    __tablename__ = "log_estoque"

    id = Column(Integer, primary_key=True, index=True)
    codprod = Column(Integer)
    idprod = Column(Integer)
    dh_atualizacao = Column(DateTime(timezone=True), server_default=text('CURRENT_DATE'))
    qtdmov = Column(Integer)
    status_estoque = Column(Boolean, default=True)
    status_lotes = Column(Boolean, nullable=True)
    obs = Column(String, nullable=True)
    log_id = Column(Integer, ForeignKey("log.id"))

    log = relationship("Log", back_populates="estoque")

class LogPedido(Base):
    __tablename__ = "log_pedido"

    __table_args__ = (
        # Recebimento, Inclusão, Confirmação, Faturamento, caNcelamento, Devolução
        CheckConstraint("evento IN ('R', 'I', 'C', 'F', 'N', 'D')", name='ck_log_pedido_evento'),
    )

    id = Column(Integer, primary_key=True, index=True)    
    id_loja = Column(Integer)
    id_pedido = Column(Integer)
    pedido_ecommerce = Column(String)
    id_nota = Column(Integer, nullable=True)
    nunota_pedido = Column(Integer, nullable=True)
    nunota_nota = Column(Integer, nullable=True)    
    dh_atualizacao = Column(DateTime(timezone=True), server_default=text('CURRENT_DATE'))
    evento = Column(String, nullable=False)
    status = Column(Boolean, default=True)
    obs = Column(String, nullable=True)
    log_id = Column(Integer, ForeignKey("log.id"))

    log = relationship("Log", back_populates="pedido")

class LogProduto(Base):
    __tablename__ = "log_produto"

    id = Column(Integer, primary_key=True, index=True)
    codprod = Column(Integer)
    idprod = Column(Integer)
    dh_atualizacao = Column(DateTime(timezone=True), server_default=text('CURRENT_DATE'))
    campo = Column(String, nullable=True)
    valor_old = Column(String, nullable=True)
    valor_new = Column(String, nullable=True)
    sucesso = Column(Boolean, default=True)
    obs = Column(String, nullable=True)
    log_id = Column(Integer, ForeignKey("log.id"))

    log = relationship("Log", back_populates="produto")

class Log(Base):
    __tablename__ = "log"

    id = Column(Integer, primary_key=True, index=True)
    dh_execucao = Column(DateTime(timezone=True))
    contexto = Column(String)
    de = Column(String)
    para = Column(String)
    sucesso = Column(Boolean)
    empresa_id = Column(Integer, ForeignKey("empresa.id"), nullable=False)

    produto = relationship("LogProduto", back_populates="log", cascade="all, delete-orphan")
    estoque = relationship("LogEstoque", back_populates="log", cascade="all, delete-orphan")
    pedido = relationship("LogPedido", back_populates="log", cascade="all, delete-orphan")
    empresa = relationship("Empresa", back_populates="log")
