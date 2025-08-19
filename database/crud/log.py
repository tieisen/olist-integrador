from sqlalchemy.orm import Session
from database.models.log import Log
from database.schemas.log import LogBase
from database.dependencies import get_db

def read_by_de(de: str):
    db: Session = next(get_db())
    log = db.query(Log).filter(Log.de == de).all()
    db.close()
    return log

def read_by_id(id: int):
    db: Session = next(get_db())
    log = db.query(Log).filter(Log.id == id).first()
    db.close()
    return log

def read_last_by_para_contexto(para: str, contexto: str):
    db: Session = next(get_db())
    log = db.query(Log).filter(Log.para == para, Log.contexto == contexto).order_by(Log.id.desc()).first()
    db.close()
    return log

# def read_all(db: Session):
#     return db.query(Log).all()

def create(log: LogBase):
    db: Session = next(get_db())
    try:
        db_log = Log(**log.model_dump())
        db.add(db_log)
        db.commit()
        db.refresh(db_log)
        db.close()
        return db_log.id
    finally:
        db.close()   

def update(id: int, log: LogBase):
    db: Session = next(get_db())
    try:
        db_log = db.query(Log).filter(Log.id == id).first()
        if db_log is None:
            return None
        for key, value in log.model_dump(exclude_unset=True).items():
            setattr(db_log, key, value)
        db.commit()
        db.refresh(db_log)
        db.close()
        return db_log
    finally:
        db.close()   