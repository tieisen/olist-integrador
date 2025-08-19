from sqlalchemy.orm import Session
from sqlalchemy.exc import IntegrityError
from datetime import datetime
from database.models.venda import Venda
from database.schemas.venda import VendaBase
from database.dependencies import get_db

def open():
    return next(get_db())

def close(db: Session):
    db.close()
    return True

def create(db:Session, id_loja:int, id_pedido:int, cod_pedido:str, num_pedido:int):
    #db: Session = next(get_db())
    try:
        venda = VendaBase(
            id_loja=id_loja,
            id_pedido=id_pedido,
            cod_pedido=cod_pedido,
            num_pedido=num_pedido            
        )
        db_venda = Venda(**venda.model_dump())
        db.add(db_venda)
        db.commit()
        #db.close()
        return True
    except IntegrityError:
        #db.close()
        print(f"Erro de integridade. Um pedido com o ID {id_pedido} já existe na base.")
        return False

def read_last_venda_dt():
    db: Session = next(get_db())
    try:
        db_venda = db.query(Venda).order_by(Venda.id.desc()).first()
        db.close()
        return db_venda.dh_pedido if db_venda else None
    except Exception as e:
        db.close()
        print(f"Erro ao ler a última venda: {e}")
        return None

def read_new_venda_to_snk():
    db: Session = next(get_db())
    try:
        db_venda = db.query(Venda).filter(Venda.nunota_pedido.is_(None),
                                          Venda.dh_cancelamento_pedido.is_(None)).order_by(Venda.num_pedido).all()
        db.close()
        return db_venda
    except Exception as e:
        print(f"Erro ao ler as vendas: {e}")
        db.close()
        return None

def read_valida_cancelamentos(lista_ids:list):
    db: Session = next(get_db())
    try:
        db_venda = db.query(Venda).filter(Venda.id_pedido.in_(lista_ids),
                                          Venda.dh_cancelamento_pedido.is_(None)).all()
        db.close()
        return db_venda
    except Exception as e:
        print(f"Erro ao validar cancelamentos: {e}")
        db.close()
        return None    

def read_valida_importados_cancelados():
    db: Session = next(get_db())
    try:
        db_venda = db.query(Venda).filter(Venda.nunota_pedido.isnot(None),
                                          Venda.nunota_nota.is_(None),
                                          Venda.dh_cancelamento_pedido.isnot(None)).order_by(Venda.num_pedido).all()
        db.close()
        return db_venda
    except Exception as e:
        print(f"Erro ao validar importados cancelados: {e}")
        db.close()
        return None

def read_venda_confirmar_snk():
    db: Session = next(get_db())
    try:
        db_venda = db.query(Venda).filter(Venda.nunota_pedido.is_not(None),
                                          Venda.dh_cancelamento_pedido.is_(None),
                                          Venda.dh_confirmacao_pedido_snk.is_(None)).order_by(Venda.num_pedido).all()
        db.close()
        return db_venda
    finally:
        db.close()

def read_venda_faturar_snk():
    db: Session = next(get_db())
    try:
        db_venda = db.query(Venda).filter(Venda.nunota_pedido.is_not(None),
                                          Venda.dh_confirmacao_pedido_snk.is_not(None),
                                          Venda.dh_cancelamento_pedido.is_(None),
                                          Venda.dh_faturamento_snk.is_(None)).order_by(Venda.num_pedido).all()
        db.close()
        return db_venda
    finally:
        db.close()

def read_faturar_olist():
    db: Session = next(get_db())
    try:
        db_venda = db.query(Venda).filter(Venda.dh_cancelamento_pedido.is_(None),
                                          Venda.id_separacao.isnot(None),
                                          Venda.id_nota.is_(None)).order_by(Venda.num_pedido).all()
        db.close()
        return db_venda
    finally:
        db.close()

def read_venda_faturada_confirmar_snk():
    db: Session = next(get_db())
    try:
        db_venda = db.query(Venda).filter(Venda.nunota_nota.is_not(None),
                                          Venda.dh_faturamento_snk.is_not(None),
                                          Venda.dh_cancelamento_pedido.is_(None),
                                          Venda.dh_confirmacao_nota_snk.is_(None)).order_by(Venda.num_pedido).all()
        db.close()
        return db_venda
    finally:
        db.close()

def read_pendente_nota_olist():
    db: Session = next(get_db())
    try:
        db_venda = db.query(Venda).filter(Venda.dh_faturamento_snk.isnot(None),
                                          Venda.dh_cancelamento_pedido.is_(None),
                                          Venda.id_nota.is_(None)).order_by(Venda.num_pedido).all()
        db.close()
        return db_venda
    finally:
        db.close()

def read_nota_autorizar():
    db: Session = next(get_db())
    try:
        db_venda = db.query(Venda).filter(Venda.dh_cancelamento_pedido.is_(None),
                                          Venda.id_nota.isnot(None),
                                          Venda.dh_nota_emissao.is_(None)).order_by(Venda.num_pedido).all()
        db.close()
        return db_venda
    finally:
        db.close()

def read_pendente_fin_olist():
    db: Session = next(get_db())
    try:
        db_venda = db.query(Venda).filter(Venda.id_nota.isnot(None),
                                          Venda.dh_cancelamento_pedido.is_(None),
                                          Venda.id_financeiro.is_(None)).order_by(Venda.num_nota).all()
        db.close()
        return db_venda
    finally:
        db.close()

def read_by_idpedido(id_pedido: int):
    db: Session = next(get_db())
    try:
        db_venda = db.query(Venda).filter(Venda.id_pedido == id_pedido).first()
        db.close()
        return db_venda
    finally:
        db.close()

def read_by_list_idpedido(ids: list[int]):
    db: Session = next(get_db())
    try:
        db_venda = db.query(Venda).filter(Venda.id_pedido.in_(ids)).all()
        db.close()
        return db_venda
    finally:
        db.close()

def read_list_separacao_pendente(ids: list[int]):
    db: Session = next(get_db())
    try:
        db_venda = db.query(Venda).filter(Venda.id_pedido.in_(ids),
                                          Venda.id_separacao.is_(None),
                                          Venda.dh_cancelamento_pedido.is_(None),
                                          Venda.dh_confirmacao_nota_snk.is_(None)).all()
        db.close()
        return db_venda
    finally:
        db.close()

def read_separacao_pendente(id_pedido: int):
    db: Session = next(get_db())
    try:
        db_venda = db.query(Venda).filter(Venda.id_pedido == id_pedido,
                                          Venda.id_separacao.is_(None),
                                          Venda.dh_cancelamento_pedido.is_(None),
                                          Venda.dh_confirmacao_nota_snk.is_(None)).all()
        db.close()
        return db_venda
    finally:
        db.close()        

def read_separacao_checkout():
    db: Session = next(get_db())
    try:
        db_venda = db.query(Venda).filter(Venda.dh_cancelamento_pedido.is_(None),
                                          Venda.id_separacao.isnot(None),                                          
                                          Venda.id_nota.isnot(None),                                          
                                          Venda.dh_confirmacao_nota_snk.is_(None)).all()
        db.close()
        return db_venda
    finally:
        db.close()        

def read_separacao_pedido(cod_pedido: str):
    db: Session = next(get_db())
    try:
        db_venda = db.query(Venda).filter(Venda.cod_pedido == cod_pedido).first()
        db.close()
        return db_venda.id_separacao
    finally:
        db.close()        

def read_perdidos():
    db: Session = next(get_db())
    try:
        db_venda = db.query(Venda).filter(Venda.nunota_pedido.is_(None),
                                          Venda.dh_cancelamento_pedido.is_(None)).order_by(Venda.num_pedido).all()
        db.close()
        return db_venda
    finally:
        db.close()

def delete_by_idpedido(id_pedido: int):
    db: Session = next(get_db())
    try:
        db_venda = db.query(Venda).filter(Venda.id_pedido == id_pedido).first()
        if db_venda is None:
            db.close()
            return None
        db.delete(db_venda)
        db.commit()
        db.close()
        return True
    finally:
        db.close()      

def update_new_venda_to_snk(id_pedido: int, nunota_pedido: int, dh_pedido: str=None):
    db: Session = next(get_db())
    try:
        db_venda = db.query(Venda).filter(Venda.id_pedido == id_pedido).first()
        if db_venda is None:
            db.close()
            return None
        setattr(db_venda, "nunota_pedido", nunota_pedido)
        if dh_pedido:
            setattr(db_venda, "dh_importacao_pedido_snk", datetime.strptime(dh_pedido,'%d/%m/%Y'))
        else:
            setattr(db_venda, "dh_importacao_pedido_snk", datetime.now())
        db.commit()
        db.close()
        return True
    finally:
        db.close()

def update_venda_confirmar_snk(nunota_pedido: int, dh_confirmado: str=None):
    db: Session = next(get_db())
    try:
        db_venda = db.query(Venda).filter(Venda.nunota_pedido == nunota_pedido).first()
        if db_venda is None:
            db.close()
            return None
        if dh_confirmado:
            setattr(db_venda, "dh_confirmacao_pedido_snk", datetime.strptime(dh_confirmado,'%d/%m/%Y'))
        else:
            setattr(db_venda, "dh_confirmacao_pedido_snk", datetime.now())
        db.commit()
        db.close()
        return True
    finally:
        db.close()

def update_venda_fatura_snk(nunota_pedido: int, nunota_nota:int, dh_faturado:str=None):
    db: Session = next(get_db())
    try:
        db_venda = db.query(Venda).filter(Venda.nunota_pedido == nunota_pedido).first()
        if db_venda is None:
            db.close()
            return None
        setattr(db_venda, "nunota_nota", nunota_nota)
        if dh_faturado:
            setattr(db_venda, "dh_faturamento_snk", datetime.strptime(dh_faturado,'%d/%m/%Y'))
        else:
            setattr(db_venda, "dh_faturamento_snk", datetime.now())
        db.commit()
        db.close()
        return True
    finally:
        db.close()

def update_gera_nf_olist(cod_pedido:str, num_nota:int, id_nota:int):
    db: Session = next(get_db())
    try:
        db_venda = db.query(Venda).filter(Venda.cod_pedido == cod_pedido).first()
        if db_venda is None:
            db.close()
            return None
        setattr(db_venda, "num_nota", num_nota)
        setattr(db_venda, "id_nota", id_nota)
        db.commit()
        db.close()
        return True
    finally:
        db.close()

def update_nota_autorizada(id_nota:int, dh_nota:str=None):
    db: Session = next(get_db())
    try:
        db_venda = db.query(Venda).filter(Venda.id_nota == id_nota).first()
        if db_venda is None:
            db.close()
            return None
        if dh_nota:
            setattr(db_venda, "dh_nota_emissao", datetime.strptime(dh_nota,'%Y-%m-%d %H:%M:%S'))
        else:
            setattr(db_venda, "dh_nota_emissao", datetime.now())
        db.commit()
        db.close()
        return True
    finally:
        db.close()

def update_faturado_olist(cod_pedido:str, num_nota:int, id_nota:int, dh_nota:str=None):
    db: Session = next(get_db())
    try:
        db_venda = db.query(Venda).filter(Venda.cod_pedido == cod_pedido).first()
        if db_venda is None:
            db.close()
            return None
        setattr(db_venda, "num_nota", num_nota)
        setattr(db_venda, "id_nota", id_nota)
        if dh_nota:
            setattr(db_venda, "dh_nota_emissao", datetime.strptime(dh_nota,'%Y-%m-%d %H:%M:%S'))
        else:
            setattr(db_venda, "dh_nota_emissao", datetime.now())
        db.commit()
        db.close()
        return True
    finally:
        db.close()

def update_nota_confirma_snk(nunota_nota:int, dh_confirmado:str=None):
    db: Session = next(get_db())
    try:
        db_venda = db.query(Venda).filter(Venda.nunota_nota == nunota_nota).first()
        if db_venda is None:
            db.close()
            return None
        if dh_confirmado:
            setattr(db_venda, "dh_confirmacao_nota_snk", datetime.strptime(dh_confirmado,'%d/%m/%Y'))
        else:
            #setattr(db_venda, "dh_confirmacao_nota_snk", datetime.now())
            setattr(db_venda, "dh_confirmacao_nota_snk", db_venda.dh_faturamento_snk)
        db.commit()
        db.close()
        return True
    finally:
        db.close()

def update_baixa_financeiro(num_nota:int, id_financeiro:int, dh_baixa:str=None):
    db: Session = next(get_db())
    try:
        db_venda = db.query(Venda).filter(Venda.num_nota == num_nota).first()
        if db_venda is None:
            return None        
        setattr(db_venda, "id_financeiro", id_financeiro)
        if dh_baixa:
            setattr(db_venda, "dh_baixa_financeiro", datetime.strptime(dh_baixa,'%Y-%m-%d'))
        else:
            setattr(db_venda, "dh_baixa_financeiro", datetime.now())
        db.commit()
        db.close()
        return True
    finally:
        db.close()

def update_nota_cancelada(num_nota:int):
    db: Session = next(get_db())
    try:
        db_venda = db.query(Venda).filter(Venda.num_nota == num_nota).first()
        if db_venda is None:
            db.close()
            return None
        setattr(db_venda, "dh_cancelamento_nota", datetime.now())
        db.commit()
        db.close()
        return True
    finally:
        db.close()

def update_pedido_cancelado_olist(id_pedido:int):
    db: Session = next(get_db())
    try:
        db_venda = db.query(Venda).filter(Venda.id_pedido == id_pedido).first()
        if db_venda is None:
            db.close()
            return None
        setattr(db_venda, "dh_cancelamento_pedido", datetime.now())
        db.commit()
        db.close()
        return True
    finally:
        db.close()

def update_pedido_cancelado_snk(id_pedido:int):
    db: Session = next(get_db())
    try:
        db_venda = db.query(Venda).filter(Venda.id_pedido == id_pedido).first()
        if db_venda is None:
            db.close()
            return None
        setattr(db_venda, "dh_confirmacao_pedido_snk", None)
        setattr(db_venda, "nunota_pedido", None)
        db.commit()
        db.close()
        return True
    finally:
        db.close()

def update_separacao(id_pedido: int, id_separacao: int):
    db: Session = next(get_db())
    try:
        db_venda = db.query(Venda).filter(Venda.id_pedido == id_pedido).first()
        if db_venda is None:
            db.close()
            return None
        setattr(db_venda, "id_separacao", id_separacao)
        db.commit()
        db.close()
        return True
    except:
        db.close()
        return False