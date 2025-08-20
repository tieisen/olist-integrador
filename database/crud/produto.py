from database.database import SessionLocal
from datetime import datetime
from database.models import Produto

def criar(cod_snk:int, cod_olist:int):
    session = SessionLocal()
    produto = session.query(Produto).filter(Produto.cod_snk == cod_snk).first()
    if produto:
        session.close()
        return False        
    novo_produto = Produto(cod_snk=cod_snk,
                           cod_olist=cod_olist,
                           dh_cadastro=datetime.now())
    session.add(novo_produto)
    session.commit()
    session.refresh(novo_produto)
    session.close()
    return True

def atualizar(cod_snk: int, pendencia: bool):
    session = SessionLocal()
    produto = session.query(Produto).filter(Produto.cod_snk == cod_snk).first()
    if not produto:
        session.close()
        return False
    setattr(produto, "pendencia", pendencia)
    if not pendencia:
        setattr(produto, "dh_atualizado", datetime.now())
    session.commit()
    session.close()
    return True

def buscar_pendencias():
    session = SessionLocal()
    produto = session.query(Produto).filter(Produto.pendencia.is_(True)).all()
    session.close()
    return produto

def buscar_olist(cod_olist: int):
    session = SessionLocal()
    produto = session.query(Produto).filter(Produto.cod_olist == cod_olist).first()
    session.close()
    return produto    

def buscar_snk(cod_snk: int):
    session = SessionLocal()
    produto = session.query(Produto).filter(Produto.cod_snk == cod_snk).first()
    session.close()
    return produto