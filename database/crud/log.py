from database.database import SessionLocal
from database.models import Log

def criar(de:str, para:str, contexto:str=None):
    session = SessionLocal()
    novo_log = Log(de=de,
                   para=para,
                   contexto=contexto)
    session.add(novo_log)
    session.commit()
    session.refresh(novo_log)
    session.close()
    return novo_log.id

def atualizar(id:int, sucesso:bool):
    session = SessionLocal()
    log = session.query(Log).filter(Log.id == id).first()
    if not log:
        session.close()
        return False
    setattr(log, "sucesso", sucesso)
    session.commit()
    session.refresh(log)
    session.close()
    return True

def buscar_de(de: str):
    session = SessionLocal()
    log = session.query(Log).filter(Log.de == de).first()
    session.close()
    return log

def buscar_id(id: int):
    session = SessionLocal()
    log = session.query(Log).filter(Log.id == id).first()
    session.close()
    return log

def buscar_para_contexto(para: str, contexto: str):
    session = SessionLocal()
    log = session.query(Log).filter(Log.para == para, Log.contexto == contexto).order_by(Log.id.desc()).first()
    session.close()
    return log
  