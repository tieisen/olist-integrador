from sqlalchemy import Column, Integer, String, DateTime, ForeignKey, Boolean, CheckConstraint
from sqlalchemy.sql import func
from sqlalchemy.orm import relationship
from database.database import Base

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