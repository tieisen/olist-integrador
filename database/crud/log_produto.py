from sqlalchemy.orm import Session
from datetime import datetime
from database.models.log_produto import LogProduto
from database.models.log import Log
from database.schemas.log_produto import LogProdutoBase
from database.dependencies import get_db

def read_by_codprod(codprod: int):
    db: Session = next(get_db())
    try:
        db_log_produto = db.query(LogProduto).filter(LogProduto.codprod == codprod).all()
        return db_log_produto
    finally:
        db.close()

def read_last():
    db: Session = next(get_db())    
    return db.query(LogProduto).filter(Log.contexto == 'produto').order_by(LogProduto.id.desc()).first()        

def read_all(dtini: datetime, dtfim: datetime):
    db: Session = next(get_db())
    try:    
        db_log_produto = db.query(LogProduto).filter(LogProduto.dh_atualizacao >= dtini, LogProduto.dh_atualizacao <= dtfim).all()
        return db_log_produto
    finally:
        db.close()        

def create(log: LogProdutoBase):
    db: Session = next(get_db())
    try:
        db_log_produto = LogProduto(**log.model_dump())
        db.add(db_log_produto)
        db.commit()
        db.refresh(db_log_produto)
        return db_log_produto
    finally:
        db.close() 