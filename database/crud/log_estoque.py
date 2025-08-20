from database.database import SessionLocal
from database.models import LogEstoque
from datetime import datetime

def criar(log_id:int,codprod:int=0,idprod:int=0,qtdmov:int=0,obs:str=None):
    session = SessionLocal()
    if not obs:
        obs = 'Sem alterações pendentes'

    novo_log = LogEstoque(log_id=log_id,
                          dh_atualizacao=datetime.now(),
                          codprod=codprod,
                          idprod=idprod,
                          qtdmov=qtdmov,
                          obs=obs)
    session.add(novo_log)
    session.commit()
    session.refresh(novo_log)
    session.close()
    return True

def buscar_codprod(codprod: int):
    session = SessionLocal()
    log = session.query(LogEstoque).filter(LogEstoque.codprod == codprod).order_by(LogEstoque.dh_atualizacao.desc()).all()
    session.close()
    return log

def buscar_status_false(log_id: int):
    session = SessionLocal()
    log = session.query(LogEstoque).filter(LogEstoque.log_id == log_id, LogEstoque.status_estoque.is_(False)).first()
    session.close()
    return log
