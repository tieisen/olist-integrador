from sqlalchemy.orm import Session
from datetime import datetime
from database.models.log_estoque import LogEstoque
from database.models.log import Log
from database.schemas.log_estoque import LogEstoqueBase
from database.dependencies import get_db

def read_by_codprod(codprod: int):
    db: Session = next(get_db())
    try:
        db_log_estoque = db.query(LogEstoque).filter(LogEstoque.codprod == codprod).all()
        return db_log_estoque
    finally:
        db.close()

def read_by_logid_status_estoque_false(log_id: int):
    db: Session = next(get_db())
    try:
        db_log_estoque = db.query(LogEstoque).filter(LogEstoque.log_id == log_id, LogEstoque.status_estoque.is_(False)).first()
        return db_log_estoque
    finally:
        db.close()

def read_last():
    db: Session = next(get_db())    
    return db.query(LogEstoque).filter(Log.contexto == 'estoque').order_by(LogEstoque.id.desc()).first()        

def read_all(dtini: datetime, dtfim: datetime):
    db: Session = next(get_db())
    try:    
        db_log_estoque = db.query(LogEstoque).filter(LogEstoque.dh_atualizacao >= dtini, LogEstoque.dh_atualizacao <= dtfim).all()
        return db_log_estoque
    finally:
        db.close()        

def create(log: LogEstoqueBase):
    db: Session = next(get_db())
    try:
        db_log_estoque = LogEstoque(**log.model_dump())
        db.add(db_log_estoque)
        db.commit()
        db.refresh(db_log_estoque)
        return db_log_estoque
    finally:
        db.close() 