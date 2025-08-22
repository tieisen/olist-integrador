from database.database import SessionLocal
from database.models import LogProduto
from database.models import Log
from datetime import datetime

def criar(log_id:int=None,codprod:int=None,idprod:int=None,campo:str=None,valor_old:str=None,valor_new:str=None,sucesso:bool=True,obs:str=None):
    session = SessionLocal()
    novo_log = LogProduto(dh_atualizacao=datetime.now(),
                          log_id=log_id,
                          codprod=codprod,
                          idprod=idprod,
                          campo=campo,
                          sucesso=sucesso,
                          valor_old=str(valor_old),
                          valor_new=str(valor_new),
                          obs=obs)
    session.add(novo_log)
    session.commit()
    session.refresh(novo_log)
    session.close()
    return True

def buscar_todos_codprod(codprod: int):
    session = SessionLocal()
    log = session.query(LogProduto).filter(LogProduto.codprod == codprod).all()
    session.close()
    return log

def buscar_ultimo_codprod(codprod: int):
    session = SessionLocal()
    ultimo_log = session.query(LogProduto).filter(Log.contexto == 'produto', LogProduto.codprod == codprod).order_by(LogProduto.log_id.desc()).first()
    if not ultimo_log:
        session.close()
        return False
    log = session.query(LogProduto).filter(LogProduto.log_id == ultimo_log.id, LogProduto.codprod == codprod).all()
    session.close()
    return log

def buscar_ultimo():
    session = SessionLocal()
    ultimo_log = session.query(LogProduto).filter(Log.contexto == 'produto').order_by(LogProduto.log_id.desc()).first()
    if not ultimo_log:
        session.close()
        return False
    log = session.query(LogProduto).filter(LogProduto.log_id == ultimo_log.id).all()
    session.close()
    return log

def buscar_status_false(log_id: int):
    session = SessionLocal()
    log = session.query(LogProduto).filter(LogProduto.log_id == log_id, LogProduto.sucesso != 1).first()
    session.close()
    return log

def buscar_id(log_id: int):
    session = SessionLocal()
    log = session.query(LogProduto).filter(LogProduto.log_id == log_id).all()
    session.close()
    return log