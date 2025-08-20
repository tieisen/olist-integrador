from database.database import SessionLocal
from datetime import datetime
from sqlalchemy import func
from database.models import Venda

def criar(id_loja:int, id_pedido:int, cod_pedido:str, num_pedido:int):
    session = SessionLocal()
    venda = session.query(Venda).filter(Venda.id_pedido == id_pedido).first()
    if venda:
        session.close()
        return False
    
    nova_venda = Venda(id_loja=id_loja,
                       id_pedido=id_pedido,
                       dh_pedido=datetime.now(),
                       cod_pedido=cod_pedido,
                       num_pedido=num_pedido)
    session.add(nova_venda)
    session.commit()
    session.refresh(nova_venda)
    session.close()
    return True

def buscar_importar():
    session = SessionLocal()
    venda = session.query(Venda).filter(Venda.nunota_pedido.is_(None),
                                        Venda.dh_cancelamento_pedido.is_(None)).order_by(Venda.num_pedido).all()
    session.close()
    return venda

def buscar_hoje():
    session = SessionLocal()
    hoje = datetime.now().date()
    venda = session.query(Venda).filter(func.date(Venda.dh_pedido) == hoje, Venda.dh_cancelamento_pedido.is_(None)).order_by(Venda.num_pedido).all()
    session.close()
    return venda

def buscar_lista_id(ids:list[int]):
    session = SessionLocal()
    venda = session.query(Venda).filter(Venda.id_pedido.in_(ids)).all()
    session.close()
    return venda

def buscar_confirmar():
    session = SessionLocal()
    venda = session.query(Venda).filter(Venda.nunota_pedido.is_not(None),
                                        Venda.dh_cancelamento_pedido.is_(None),
                                        Venda.dh_confirmacao_pedido_snk.is_(None)).order_by(Venda.num_pedido).all()
    session.close()
    return venda

def buscar_confirmar_nota():
    session = SessionLocal()
    venda = session.query(Venda).filter(Venda.nunota_nota.is_not(None),
                                        Venda.dh_faturamento_snk.is_not(None),
                                        Venda.dh_cancelamento_pedido.is_(None),
                                        Venda.dh_confirmacao_nota_snk.is_(None)).order_by(Venda.num_pedido).all()
    session.close()
    return venda

def buscar_faturar():
    session = SessionLocal()
    venda = session.query(Venda).filter(Venda.nunota_pedido.is_not(None),
                                        Venda.dh_cancelamento_pedido.is_(None),
                                        Venda.dh_confirmacao_pedido_snk.is_not(None),                                        
                                        Venda.dh_faturamento_snk.is_(None)).order_by(Venda.num_pedido).all()
    session.close()
    return venda

def buscar_idseparacao(cod_pedido:int):
    session = SessionLocal()
    venda = session.query(Venda).filter(Venda.cod_pedido == cod_pedido).first()
    session.close()
    return venda.id_separacao

def buscar_importadas_cancelar():
    session = SessionLocal()
    venda = session.query(Venda).filter(Venda.nunota_pedido.isnot(None),
                                        Venda.nunota_nota.is_(None),
                                        Venda.dh_cancelamento_pedido.isnot(None)).order_by(Venda.num_pedido).all()
    session.close()
    return venda

def buscar_autorizar():
    session = SessionLocal()
    venda = session.query(Venda).filter(Venda.dh_cancelamento_pedido.is_(None),
                                        Venda.id_nota.isnot(None),
                                        Venda.dh_nota_emissao.is_(None)).order_by(Venda.num_pedido).all()
    session.close()
    return venda

def buscar_sem_nota():
    session = SessionLocal()
    venda = session.query(Venda).filter(Venda.dh_faturamento_snk.isnot(None),
                                        Venda.dh_cancelamento_pedido.is_(None),
                                        Venda.id_nota.is_(None)).order_by(Venda.num_pedido).all()
    session.close()
    return venda

def buscar_financeiro():
    session = SessionLocal()
    venda = session.query(Venda).filter(Venda.id_nota.isnot(None),
                                        Venda.dh_cancelamento_pedido.is_(None),
                                        Venda.id_financeiro.is_(None)).order_by(Venda.num_pedido).all()
    session.close()
    return venda

def buscar_separacao():
    session = SessionLocal()
    venda = session.query(Venda).filter(Venda.dh_cancelamento_pedido.is_(None),
                                        Venda.id_separacao.isnot(None),                                          
                                        Venda.id_nota.isnot(None),                                          
                                        Venda.dh_confirmacao_nota_snk.is_(None)).order_by(Venda.num_pedido).all()
    session.close()
    return venda

def buscar_separacao_idpedido(id_pedido:int):
    session = SessionLocal()
    venda = session.query(Venda).filter(Venda.id_pedido == id_pedido,
                                        Venda.id_separacao.is_(None),
                                        Venda.dh_cancelamento_pedido.is_(None),
                                        Venda.dh_confirmacao_nota_snk.is_(None)).order_by(Venda.num_pedido).all()
    session.close()
    return venda

def validar_cancelamentos(lista_ids:list):
    session = SessionLocal()
    venda = session.query(Venda).filter(Venda.id_pedido.in_(lista_ids),
                                        Venda.dh_cancelamento_pedido.is_(None)).order_by(Venda.num_pedido).all()    
    session.close()
    return venda

def atualizar_importada(id_pedido:int,nunota_pedido:int):
    session = SessionLocal()
    venda = session.query(Venda).filter(Venda.id_pedido == id_pedido).first()
    if not venda:
        session.close()
        return False
    setattr(venda, "nunota_pedido", nunota_pedido)
    setattr(venda, "dh_importacao_pedido_snk", datetime.now())
    session.commit()
    session.close()
    return True

def atualizar_separacao(id_pedido: int, id_separacao: int):
    session = SessionLocal()
    venda = session.query(Venda).filter(Venda.id_pedido == id_pedido).first()
    if not venda:
        session.close()
        return False
    setattr(venda, "id_separacao", id_separacao)    
    session.commit()
    session.close()
    return True

def atualizar_confirmada(nunota_pedido:int, dh_confirmado: str=None):
    session = SessionLocal()
    venda = session.query(Venda).filter(Venda.nunota_pedido == nunota_pedido).first()
    if not venda:
        session.close()
        return False
    if dh_confirmado:
        setattr(venda, "dh_confirmacao_pedido_snk", datetime.strptime(dh_confirmado,'%d/%m/%Y'))
    else:
        setattr(venda, "dh_confirmacao_pedido_snk", datetime.now())
    session.commit()
    session.close()
    return True

def atualizar_confirmada_nota(nunota_nota:int, dh_confirmado:str=None):
    session = SessionLocal()
    venda = session.query(Venda).filter(Venda.nunota_nota == nunota_nota).first()
    if not venda:
        session.close()
        return False
    if dh_confirmado:
        setattr(venda, "dh_confirmacao_nota_snk", datetime.strptime(dh_confirmado,'%d/%m/%Y'))
    else:
        setattr(venda, "dh_confirmacao_nota_snk", datetime.now())
    session.commit()
    session.close()
    return True

def atualizar_faturada(nunota_pedido:int,nunota_nota:int,dh_faturamento:str=None):
    session = SessionLocal()
    venda = session.query(Venda).filter(Venda.nunota_pedido == nunota_pedido).first()
    if not venda:
        session.close()
        return False
    setattr(venda, "nunota_nota", nunota_nota)
    if dh_faturamento:
        setattr(venda, "dh_faturamento_snk", datetime.strptime(dh_faturamento,'%Y-%m-%d %H:%M:%S'))
    else:
        setattr(venda, "dh_faturamento_snk", datetime.now())
    session.commit()
    session.close()
    return True

def atualizar_nf_gerada(cod_pedido:str, num_nota:int, id_nota:int, dh_nota:str=None):
    session = SessionLocal()
    venda = session.query(Venda).filter(Venda.cod_pedido == cod_pedido).first()
    if not venda:
        session.close()
        return False
    setattr(venda, "num_nota", num_nota)
    setattr(venda, "id_nota", id_nota)
    if dh_nota:
        setattr(venda, "dh_nota_emissao", datetime.strptime(dh_nota,'%Y-%m-%d %H:%M:%S'))
    else:
        setattr(venda, "dh_nota_emissao", datetime.now())     
    session.commit()
    session.close()
    return True

def atualizar_nf_autorizada(id_nota:int, dh_nota:str=None):
    session = SessionLocal()
    venda = session.query(Venda).filter(Venda.id_nota == id_nota).first()
    if not venda:
        session.close()
        return False
    if dh_nota:
        setattr(venda, "dh_nota_emissao", datetime.strptime(dh_nota,'%Y-%m-%d %H:%M:%S'))
    else:
        setattr(venda, "dh_nota_emissao", datetime.now())    
    session.commit()
    session.close()
    return True

def atualizar_financeiro(num_nota:int, id_financeiro:int, dh_baixa:str=None):
    session = SessionLocal()
    venda = session.query(Venda).filter(Venda.num_nota == num_nota).first()
    if not venda:
        session.close()
        return False
    setattr(venda, "id_financeiro", id_financeiro)
    if dh_baixa:
        setattr(venda, "dh_baixa_financeiro", datetime.strptime(dh_baixa,'%Y-%m-%d %H:%M:%S'))
    else:
        setattr(venda, "dh_baixa_financeiro", datetime.now())    
    session.commit()
    session.close()
    return True

def atualizar_cancelada(id_pedido:int):
    session = SessionLocal()
    venda = session.query(Venda).filter(Venda.id_pedido == id_pedido).first()
    if not venda:
        session.close()
        return False
    setattr(venda, "dh_cancelamento_pedido", datetime.now())
    session.commit()
    session.close()
    return True
