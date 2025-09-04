from sqlalchemy import Column, Integer, String, DateTime, ForeignKey, Boolean, CheckConstraint
from sqlalchemy.sql import text
from sqlalchemy.orm import relationship
from database.database import Base

class Empresa(Base):    
    __tablename__ = "empresa"

    id = Column(Integer, primary_key=True, index=True, autoincrement=True)
    codigo_snk = Column(Integer, nullable=False)
    nome = Column(String, nullable=False)
    cnpj = Column(String, nullable=False)
    serie_nfe = Column(String, nullable=False)
    client_id = Column(String, nullable=False)
    client_secret = Column(String, nullable=False)
    admin_email = Column(String, nullable=False)
    admin_senha = Column(String, nullable=False)
    dh_criacao = Column(DateTime(timezone=True), nullable=False, server_default=text('CURRENT_TIMESTAMP'))
    dh_atualizacao = Column(DateTime(timezone=True), nullable=True, onupdate=text('CURRENT_TIMESTAMP'))
    ativo = Column(Boolean, default=True)
    
    log_ = relationship("Log", back_populates="empresa_", cascade="all, delete-orphan")
    produto_ = relationship("Produto", back_populates="empresa_", cascade="all, delete-orphan")
    olist_ = relationship("Olist", back_populates="empresa_", cascade="all, delete-orphan")
    sankhya_ = relationship("Sankhya", back_populates="empresa_", cascade="all, delete-orphan")
    ecommerce_ = relationship("Ecommerce", back_populates="empresa_", cascade="all, delete-orphan")    

class Produto(Base):
    __tablename__ = "produto"

    id = Column(Integer, primary_key=True, index=True, autoincrement=True)
    cod_snk = Column(Integer, nullable=False) 
    cod_olist = Column(Integer, nullable=True)
    dh_criacao = Column(DateTime(timezone=True), nullable=False, server_default=text('CURRENT_TIMESTAMP'))    
    dh_atualizacao = Column(DateTime(timezone=True), nullable=True)
    pendencia = Column(Boolean, default=False)
    empresa_id = Column(Integer, ForeignKey("empresa.id"), nullable=False)

    empresa_ = relationship("Empresa", back_populates="produto_")    
    log_produto_ = relationship("LogProduto", back_populates="produto_")

class Olist(Base):
    __tablename__ = "olist"

    id = Column(Integer, primary_key=True, index=True, autoincrement=True)
    dh_solicitacao = Column(DateTime(timezone=True), nullable=False, server_default=text('CURRENT_TIMESTAMP'))
    token_criptografado = Column(String, nullable=False)
    dh_expiracao_token = Column(DateTime(timezone=True), nullable=False)
    refresh_token_criptografado = Column(String, nullable=False)
    dh_expiracao_refresh_token = Column(DateTime(timezone=True), nullable=False)
    id_token_criptografado = Column(String, nullable=False)
    empresa_id = Column(Integer, ForeignKey("empresa.id"), nullable=False)

    empresa_ = relationship("Empresa", back_populates="olist_")

class Sankhya(Base):
    __tablename__ = "sankhya"

    id = Column(Integer, primary_key=True, index=True, autoincrement=True)
    dh_solicitacao = Column(DateTime(timezone=True), nullable=False, server_default=text('CURRENT_TIMESTAMP'))
    token_criptografado = Column(String, nullable=False)
    dh_expiracao_token = Column(DateTime(timezone=True), nullable=False)
    empresa_id = Column(Integer, ForeignKey("empresa.id"), nullable=False)

    empresa_ = relationship("Empresa", back_populates="sankhya_")

class Ecommerce(Base):
    __tablename__ = "ecommerce"

    id = Column(Integer, primary_key=True, index=True, autoincrement=True)
    id_loja = Column(Integer, nullable=False)
    nome = Column(String, nullable=False)
    dh_criacao = Column(DateTime(timezone=True), nullable=False, server_default=text('CURRENT_TIMESTAMP'))
    dh_atualizacao = Column(DateTime(timezone=True), nullable=True, onupdate=text('CURRENT_TIMESTAMP'))    
    ativo = Column(Boolean, default=True)
    empresa_id = Column(Integer, ForeignKey("empresa.id"), nullable=False)

    empresa_ = relationship("Empresa", back_populates="ecommerce_")
    pedido_ = relationship("Pedido", back_populates="ecommerce_", cascade="all, delete-orphan")

class Pedido(Base):
    __tablename__ = "pedido"

    id = Column(Integer, primary_key=True, index=True, autoincrement=True)
    id_pedido = Column(Integer, nullable=False, unique=True)
    dh_pedido = Column(DateTime(timezone=True), nullable=False, server_default=text('CURRENT_TIMESTAMP'))
    cod_pedido = Column(String, nullable=False)
    num_pedido = Column(Integer, nullable=False)
    dh_cancelamento = Column(DateTime(timezone=True), nullable=True)
    id_separacao = Column(Integer, nullable=True)
    nunota = Column(Integer, nullable=True)
    dh_importacao = Column(DateTime(timezone=True), nullable=True)
    dh_confirmacao = Column(DateTime(timezone=True), nullable=True)
    dh_faturamento = Column(DateTime(timezone=True), nullable=True)
    ecommerce_id = Column(Integer, ForeignKey("ecommerce.id"), nullable=False)

    ecommerce_ = relationship("Ecommerce", back_populates="pedido_")
    nota_ = relationship("Nota", back_populates="pedido_", cascade="all, delete-orphan")
    log_pedido_ = relationship("LogPedido", back_populates="pedido_", cascade="all, delete-orphan")

class Nota(Base):
    __tablename__ = "nota"

    id = Column(Integer, primary_key=True, index=True, autoincrement=True)
    id_nota = Column(Integer, nullable=True)
    dh_emissao = Column(DateTime(timezone=True), nullable=True)
    numero = Column(Integer, nullable=True)
    serie = Column(String, nullable=True)
    chave_acesso = Column(String, nullable=True)
    id_financeiro = Column(Integer, nullable=True)
    dh_baixa_financeiro = Column(DateTime(timezone=True), nullable=True)
    nunota = Column(Integer, nullable=True)
    dh_confirmacao = Column(DateTime(timezone=True), nullable=True)
    dh_cancelamento = Column(DateTime(timezone=True), nullable=True)
    pedido_id = Column(Integer, ForeignKey("pedido.id"), nullable=False)

    pedido_ = relationship("Pedido", back_populates="nota_")
    devolucao_ = relationship("Devolucao", back_populates="nota_", cascade="all, delete-orphan")

class Devolucao(Base):
    __tablename__ = "devolucao"

    id = Column(Integer, primary_key=True, index=True, autoincrement=True)
    id_nota = Column(Integer, nullable=True)
    dh_emissao = Column(DateTime(timezone=True), nullable=True)
    numero = Column(Integer, nullable=True)
    serie = Column(String, nullable=True)
    chave_acesso = Column(String, nullable=True)
    nunota = Column(Integer, nullable=True)
    dh_confirmacao = Column(DateTime(timezone=True), nullable=True)
    dh_cancelamento = Column(DateTime(timezone=True), nullable=True)
    nota_id = Column(Integer, ForeignKey("nota.id"), nullable=True)

    nota_ = relationship("Nota", back_populates="devolucao_")

class LogEstoque(Base):
    __tablename__ = "log_estoque"

    id = Column(Integer, primary_key=True, index=True, autoincrement=True)
    codprod = Column(Integer)
    idprod = Column(Integer)
    dh_atualizacao = Column(DateTime(timezone=True), nullable=False, server_default=text('CURRENT_TIMESTAMP'))
    qtdmov = Column(Integer)
    status_estoque = Column(Boolean, default=True)
    status_lotes = Column(Boolean, nullable=True)
    obs = Column(String, nullable=True)
    log_id = Column(Integer, ForeignKey("log.id"))

    log_ = relationship("Log", back_populates="log_estoque_")

class LogPedido(Base):
    __tablename__ = "log_pedido"

    __table_args__ = (
        # Recebimento, Inclusão, Confirmação, Faturamento, caNcelamento, Devolução
        CheckConstraint("evento IN ('R', 'I', 'C', 'F', 'N', 'D')", name='ck_log_pedido_evento'),
    )

    id = Column(Integer, primary_key=True, index=True, autoincrement=True)
    dh_atualizacao = Column(DateTime(timezone=True), nullable=False, server_default=text('CURRENT_TIMESTAMP'))
    evento = Column(String, nullable=False)
    status = Column(Boolean, default=True)
    obs = Column(String, nullable=True)
    log_id = Column(Integer, ForeignKey("log.id"))
    pedido_id = Column(Integer, ForeignKey("pedido.id"))    

    log_ = relationship("Log", back_populates="log_pedido_")
    pedido_ = relationship("Pedido", back_populates="log_pedido_")

class LogProduto(Base):
    __tablename__ = "log_produto"

    id = Column(Integer, primary_key=True, index=True, autoincrement=True)
    codprod = Column(Integer)
    idprod = Column(Integer)
    dh_atualizacao = Column(DateTime(timezone=True), nullable=False, server_default=text('CURRENT_TIMESTAMP'))
    campo = Column(String, nullable=True)
    valor_old = Column(String, nullable=True)
    valor_new = Column(String, nullable=True)
    sucesso = Column(Boolean, default=True)
    obs = Column(String, nullable=True)
    log_id = Column(Integer, ForeignKey("log.id"))
    produto_id = Column(Integer, ForeignKey("produto.id"))

    log_ = relationship("Log", back_populates="log_produto_")
    produto_ = relationship("Produto", back_populates="log_produto_")

class Log(Base):
    __tablename__ = "log"

    id = Column(Integer, primary_key=True, index=True, autoincrement=True)
    dh_execucao = Column(DateTime(timezone=True), nullable=False, server_default=text('CURRENT_TIMESTAMP'))
    contexto = Column(String, nullable=False)
    de = Column(String, nullable=False)
    para = Column(String, nullable=False)
    sucesso = Column(Boolean)
    empresa_id = Column(Integer, ForeignKey("empresa.id"), nullable=False)

    log_produto_ = relationship("LogProduto", back_populates="log_", cascade="all, delete-orphan")
    log_estoque_ = relationship("LogEstoque", back_populates="log_", cascade="all, delete-orphan")
    log_pedido_ = relationship("LogPedido", back_populates="log_", cascade="all, delete-orphan")
    empresa_ = relationship("Empresa", back_populates="log_")
