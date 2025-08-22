from database.database import SessionLocal
from database.models import LogPedido
from datetime import datetime

def criar(log_id:int,id_loja:int,id_pedido:int,pedido_ecommerce:str,evento:str='I',nunota_pedido:int=0,status:bool=True,obs:str=None):
    session = SessionLocal()
    novo_log = LogPedido(log_id=log_id,
                         dh_atualizacao=datetime.now(),
                         id_loja=id_loja,
                         id_pedido=id_pedido,
                         pedido_ecommerce=pedido_ecommerce,
                         evento=evento,
                         nunota_pedido=nunota_pedido,
                         status=status,
                         obs=obs)
    session.add(novo_log)
    session.commit()
    session.refresh(novo_log)
    session.close()
    return True

def buscar_id(log_id: int):
    session = SessionLocal()
    log = session.query(LogPedido).filter(LogPedido.log_id == log_id).all()
    session.close()
    return log

def buscar_id_pedido(id_pedido: int):
    session = SessionLocal()
    log = session.query(LogPedido).filter(LogPedido.id_pedido == id_pedido).first()
    session.close()
    return log

def buscar_nunota_pedido(nunota_pedido: int):
    session = SessionLocal()
    log = session.query(LogPedido).filter(LogPedido.nunota_pedido == nunota_pedido).first()
    session.close()
    return log

def buscar_status_false(log_id: int):
    session = SessionLocal()
    log = session.query(LogPedido).filter(LogPedido.log_id == log_id, LogPedido.status.is_(False)).first()
    session.close()
    return log
