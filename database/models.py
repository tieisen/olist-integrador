from sqlalchemy import Column, Integer, String, DateTime, ForeignKey, Boolean, CheckConstraint, JSON
from sqlalchemy.sql import text
from sqlalchemy.orm import relationship
from database.database import Base

class Empresa(Base):

    __tablename__ = "empresa"

    id = Column(Integer, primary_key=True, index=True, autoincrement=True)
    dh_criacao = Column(DateTime(timezone=True), nullable=False, server_default=text('CURRENT_TIMESTAMP'))
    dh_atualizacao = Column(DateTime(timezone=True), nullable=True, onupdate=text('CURRENT_TIMESTAMP'))
    ativo = Column(Boolean, default=True)
    snk_codemp = Column(Integer, nullable=False)
    nome = Column(String, nullable=False)
    cnpj = Column(String, nullable=False)
    serie_nfe = Column(String, nullable=True)
    client_id = Column(String, nullable=True)
    client_secret = Column(String, nullable=True)
    olist_admin_email = Column(String, nullable=True)
    olist_admin_senha = Column(String, nullable=True)
    olist_id_fornecedor_padrao = Column(Integer, nullable=True)
    olist_id_deposito_padrao = Column(Integer, nullable=True)
    olist_dias_busca_pedidos = Column(Integer, nullable=True)
    olist_situacao_busca_pedidos = Column(Integer, nullable=True)
    olist_id_conta_destino = Column(Integer, nullable=True)
    olist_id_categoria_padrao = Column(Integer, nullable=True)
    olist_id_marca_padrao = Column(Integer, nullable=True)
    snk_token = Column(String, nullable=True)
    snk_appkey = Column(String, nullable=True)
    snk_admin_email = Column(String, nullable=True)
    snk_admin_senha = Column(String, nullable=True)
    snk_timeout_token_min = Column(Integer, nullable=True)
    snk_top_pedido = Column(Integer, nullable=True)
    snk_top_venda = Column(Integer, nullable=True)
    snk_top_transferencia = Column(Integer, nullable=True)
    snk_top_devolucao = Column(Integer, nullable=True)
    snk_top_baixa_estoque = Column(Integer, nullable=True)    
    snk_codvend = Column(Integer, nullable=True)
    snk_codcencus = Column(Integer, nullable=True)
    snk_codnat = Column(Integer, nullable=True)
    snk_codnat_transferencia = Column(Integer, nullable=True)
    snk_codtipvenda = Column(Integer, nullable=True)
    snk_codusu_integracao = Column(Integer, nullable=True)
    snk_codtab_transf = Column(Integer, nullable=True)
    snk_codlocal_estoque = Column(String, nullable=True)
    snk_codlocal_venda = Column(Integer, nullable=True)
    snk_codlocal_ecommerce = Column(String, nullable=True)
    snk_codparc = Column(Integer, nullable=True)
    snk_codemp_fornecedor = Column(Integer, nullable=False)    
    snk_obs_transferencia = Column(String, nullable=True)
    
    log_ = relationship("Log", back_populates="empresa_", cascade="all, delete-orphan", passive_deletes=True)
    produto_ = relationship("Produto", back_populates="empresa_", cascade="all, delete-orphan", passive_deletes=True)
    olist_ = relationship("Olist", back_populates="empresa_", cascade="all, delete-orphan", passive_deletes=True)
    sankhya_ = relationship("Sankhya", back_populates="empresa_", cascade="all, delete-orphan", passive_deletes=True)
    ecommerce_ = relationship("Ecommerce", back_populates="empresa_", cascade="all, delete-orphan", passive_deletes=True)

class Produto(Base):
    __tablename__ = "produto"

    id = Column(Integer, primary_key=True, index=True, autoincrement=True)
    codprod = Column(Integer, nullable=False) 
    idprod = Column(Integer, nullable=True)
    dh_criacao = Column(DateTime(timezone=True), nullable=False, server_default=text('CURRENT_TIMESTAMP'))    
    dh_atualizacao = Column(DateTime(timezone=True), nullable=True)
    pendencia = Column(Boolean, default=False)
    empresa_id = Column(Integer, ForeignKey("empresa.id", ondelete="CASCADE"), nullable=False)

    empresa_ = relationship("Empresa", back_populates="produto_")    
    log_produto_ = relationship("LogProduto", back_populates="produto_")

class Olist(Base):
    __tablename__ = "olist"

    id = Column(Integer, primary_key=True, index=True, autoincrement=True)
    dh_solicitacao = Column(DateTime(timezone=True), nullable=False, server_default=text('CURRENT_TIMESTAMP'))
    token = Column(String, nullable=False)
    dh_expiracao_token = Column(DateTime(timezone=True), nullable=False)
    refresh_token = Column(String, nullable=False)
    dh_expiracao_refresh_token = Column(DateTime(timezone=True), nullable=False)
    id_token = Column(String, nullable=False)
    empresa_id = Column(Integer, ForeignKey("empresa.id", ondelete="CASCADE"), nullable=False)

    empresa_ = relationship("Empresa", back_populates="olist_")

class Sankhya(Base):
    __tablename__ = "sankhya"

    id = Column(Integer, primary_key=True, index=True, autoincrement=True)
    dh_solicitacao = Column(DateTime(timezone=True), nullable=False, server_default=text('CURRENT_TIMESTAMP'))
    token = Column(String, nullable=False)
    dh_expiracao_token = Column(DateTime(timezone=True), nullable=False)
    empresa_id = Column(Integer, ForeignKey("empresa.id", ondelete="CASCADE"), nullable=False)

    empresa_ = relationship("Empresa", back_populates="sankhya_")

class Ecommerce(Base):
    __tablename__ = "ecommerce"

    id = Column(Integer, primary_key=True, index=True, autoincrement=True)
    id_loja = Column(Integer, nullable=False)
    nome = Column(String, nullable=False)
    id_conta_destino = Column(Integer, default=0, nullable=False)
    id_categoria_financeiro = Column(Integer, default=0, nullable=False)
    id_deposito = Column(Integer, nullable=True)
    dh_criacao = Column(DateTime(timezone=True), nullable=False, server_default=text('CURRENT_TIMESTAMP'))
    dh_atualizacao = Column(DateTime(timezone=True), nullable=True, onupdate=text('CURRENT_TIMESTAMP'))    
    importa_pedido_lote = Column(Boolean, default=True)
    limite_pedido_lote = Column(Integer, nullable=True)
    ativo = Column(Boolean, default=True)
    empresa_id = Column(Integer, ForeignKey("empresa.id", ondelete="CASCADE"), nullable=False)

    empresa_ = relationship("Empresa", back_populates="ecommerce_")
    pedido_ = relationship("Pedido", back_populates="ecommerce_", cascade="all, delete-orphan", passive_deletes=True)

class Pedido(Base):
    __tablename__ = "pedido"

    id = Column(Integer, primary_key=True, index=True, autoincrement=True)
    id_pedido = Column(Integer, nullable=False, unique=True)
    dh_pedido = Column(DateTime(timezone=True), nullable=False, server_default=text('CURRENT_TIMESTAMP'))
    cod_pedido = Column(String, nullable=False)
    num_pedido = Column(Integer, nullable=False)
    dados_pedido = Column(JSON, nullable=True)
    dh_cancelamento = Column(DateTime(timezone=True), nullable=True)
    id_separacao = Column(Integer, nullable=True)
    nunota = Column(Integer, nullable=True)
    dh_importacao = Column(DateTime(timezone=True), nullable=True)
    dh_confirmacao = Column(DateTime(timezone=True), nullable=True)
    dh_faturamento = Column(DateTime(timezone=True), nullable=True)
    ecommerce_id = Column(Integer, ForeignKey("ecommerce.id", ondelete="CASCADE"), nullable=False)

    ecommerce_ = relationship("Ecommerce", back_populates="pedido_")
    nota_ = relationship("Nota", back_populates="pedido_", cascade="all, delete-orphan", passive_deletes=True)
    log_pedido_ = relationship("LogPedido", back_populates="pedido_", cascade="all, delete-orphan", passive_deletes=True)

class Nota(Base):
    __tablename__ = "nota"

    id = Column(Integer, primary_key=True, index=True, autoincrement=True)
    id_nota = Column(Integer, nullable=False)
    dh_emissao = Column(DateTime(timezone=True), nullable=True)
    numero = Column(Integer, nullable=False)
    serie = Column(String, nullable=False)
    chave_acesso = Column(String, nullable=True)
    id_financeiro = Column(Integer, nullable=True)
    dh_baixa_financeiro = Column(DateTime(timezone=True), nullable=True)
    nunota = Column(Integer, nullable=True)
    dh_confirmacao = Column(DateTime(timezone=True), nullable=True)
    baixa_estoque_ecommerce = Column(Boolean, default=False)
    dh_cancelamento = Column(DateTime(timezone=True), nullable=True)
    cancelado_sankhya = Column(Boolean, default=False)
    pedido_id = Column(Integer, ForeignKey("pedido.id", ondelete="CASCADE"), nullable=False)

    pedido_ = relationship("Pedido", back_populates="nota_")
    devolucao_ = relationship("Devolucao", back_populates="nota_", cascade="all, delete-orphan", passive_deletes=True)

class Devolucao(Base):
    __tablename__ = "devolucao"

    id = Column(Integer, primary_key=True, index=True, autoincrement=True)
    id_nota = Column(Integer, nullable=False)
    dh_emissao = Column(DateTime(timezone=True), nullable=True)
    numero = Column(Integer, nullable=False)
    serie = Column(String, nullable=False)
    chave_acesso = Column(String, nullable=True)
    nunota = Column(Integer, nullable=True)
    dh_confirmacao = Column(DateTime(timezone=True), nullable=True)
    dh_cancelamento = Column(DateTime(timezone=True), nullable=True)
    nota_id = Column(Integer, ForeignKey("nota.id", ondelete="CASCADE"), nullable=False)

    nota_ = relationship("Nota", back_populates="devolucao_")

class LogEstoque(Base):
    __tablename__ = "log_estoque"

    id = Column(Integer, primary_key=True, index=True, autoincrement=True)
    codprod = Column(Integer)
    idprod = Column(Integer)
    qtdmov = Column(Integer)
    sucesso = Column(Boolean, default=True)
    status_lotes = Column(Boolean, nullable=True)
    obs = Column(String, nullable=True)
    log_id = Column(Integer, ForeignKey("log.id", ondelete="CASCADE"))

    log_ = relationship("Log", back_populates="log_estoque_")

class LogPedido(Base):
    __tablename__ = "log_pedido"

    __table_args__ = (
        # Recebimento, Inclusão, Confirmação, Faturamento, caNcelamento, Devolução
        CheckConstraint("evento IN ('R', 'I', 'C', 'F', 'N', 'D')", name='ck_log_pedido_evento'),
    )

    id = Column(Integer, primary_key=True, index=True, autoincrement=True)
    evento = Column(String, nullable=False)
    sucesso = Column(Boolean, default=True)
    obs = Column(String, nullable=True)
    log_id = Column(Integer, ForeignKey("log.id", ondelete="CASCADE"))
    pedido_id = Column(Integer, ForeignKey("pedido.id", ondelete="CASCADE"))    

    log_ = relationship("Log", back_populates="log_pedido_")
    pedido_ = relationship("Pedido", back_populates="log_pedido_")

class LogProduto(Base):
    __tablename__ = "log_produto"

    id = Column(Integer, primary_key=True, index=True, autoincrement=True)
    codprod = Column(Integer)
    idprod = Column(Integer)
    campo = Column(String, nullable=True)
    valor_old = Column(String, nullable=True)
    valor_new = Column(String, nullable=True)
    sucesso = Column(Boolean, default=True)
    obs = Column(String, nullable=True)
    log_id = Column(Integer, ForeignKey("log.id", ondelete="CASCADE"))
    produto_id = Column(Integer, ForeignKey("produto.id", ondelete="CASCADE"))

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
    empresa_id = Column(Integer, ForeignKey("empresa.id", ondelete="CASCADE"), nullable=False)

    log_produto_ = relationship("LogProduto", back_populates="log_", cascade="all, delete-orphan", passive_deletes=True)
    log_estoque_ = relationship("LogEstoque", back_populates="log_", cascade="all, delete-orphan", passive_deletes=True)
    log_pedido_ = relationship("LogPedido", back_populates="log_", cascade="all, delete-orphan", passive_deletes=True)
    empresa_ = relationship("Empresa", back_populates="log_")
