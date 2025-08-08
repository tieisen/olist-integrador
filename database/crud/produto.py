from sqlalchemy.orm import Session
from sqlalchemy.exc import IntegrityError
from datetime import datetime
from database.models.produto import Produto
from database.schemas.produto import ProdutoBase
from database.dependencies import get_db

def create(cod_snk:int, cod_olist:int):
    db: Session = next(get_db())
    try:
        produto = ProdutoBase(
            cod_snk=cod_snk,
            cod_olist=cod_olist,
            dh_cadastro=datetime.now()
        )
        db_produto = Produto(**produto.model_dump())
        db.add(db_produto)
        db.commit()
        db.close()
        return True
    except IntegrityError:
        print(f"Erro de integridade. Produto {cod_snk} já existe na base.")
        pass
    finally:
        db.close()

def update(cod_snk: int, pendencia: bool):
    db: Session = next(get_db())
    try:
        db_produto = db.query(Produto).filter(Produto.cod_snk == cod_snk).first()
        if db_produto is None:
            db.close()
            return None
        setattr(db_produto, "pendencia", pendencia)
        if not pendencia:
            setattr(db_produto, "dh_atualizado", datetime.now())
        db.commit()
        db.close()
        return True
    finally:
        db.close()

def read_pendencias():
    db: Session = next(get_db())
    try:
        db_produto = db.query(Produto).filter(Produto.pendencia.is_(True)).all()
        db.close()
        return db_produto
    finally:
        db.close()        

def read_by_id(id: int):
    db: Session = next(get_db())
    try:
        db_produto = db.query(Produto).filter(Produto.id == id).first()
        db.close()
        return db_produto
    finally:
        db.close()        

def read_by_olist(cod_olist: int):
    db: Session = next(get_db())
    try:
        db_produto = db.query(Produto).filter(Produto.cod_olist == cod_olist).first()
        db.close()
        return db_produto
    finally:
        db.close()

def read_by_snk(cod_snk: int):
    db: Session = next(get_db())
    try:
        db_produto = db.query(Produto).filter(Produto.cod_snk == cod_snk).first()
        db.close()
        return db_produto
    finally:
        db.close()