from sqlalchemy.orm import Session
from datetime import datetime
from database.models.log_pedido import LogPedido
from database.models.log import Log
from database.schemas.log_pedido import LogPedidoBase
from database.dependencies import get_db

def read_by_id(id: int):
    db: Session = next(get_db())
    try:
        dh_log_pedido = db.query(LogPedido).filter(LogPedido.id == id).first()
        return dh_log_pedido
    finally:
        db.close()

def read_by_nunota_pedido(nunota: int):
    db: Session = next(get_db())
    try:
        dh_log_pedido = db.query(LogPedido).filter(LogPedido.nunota_pedido == nunota).first()
        return dh_log_pedido
    finally:
        db.close()

def read_by_logid_status_false(log_id: int):
    db: Session = next(get_db())
    try:
        dh_log_pedido = db.query(LogPedido).filter(LogPedido.log_id == log_id, LogPedido.status.is_(False)).first()
        return dh_log_pedido
    finally:
        db.close()        

def read_last():
    db: Session = next(get_db())    
    return db.query(LogPedido).filter(Log.contexto == 'pedido').order_by(LogPedido.id.desc()).first()        

def read_all(dtini: datetime, dtfim: datetime):
    db: Session = next(get_db())
    try:    
        dh_log_pedido = db.query(LogPedido).filter(LogPedido.dh_atualizacao >= dtini, LogPedido.dh_atualizacao <= dtfim).all()
        return dh_log_pedido
    finally:
        db.close()        

def create(log: LogPedidoBase):
    db: Session = next(get_db())
    try:
        dh_log_pedido = LogPedido(**log.model_dump())
        db.add(dh_log_pedido)
        db.commit()
        db.refresh(dh_log_pedido)
        return dh_log_pedido
    finally:
        db.close() 