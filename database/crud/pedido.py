from database.database import AsyncSessionLocal
from database.models import Pedido, Ecommerce
from datetime import datetime
from src.utils.log import Log
import os
import logging
from dotenv import load_dotenv

load_dotenv('keys/.env')
logger = logging.getLogger(__name__)
logging.basicConfig(filename=Log().buscar_path(),
                    encoding='utf-8',
                    format=os.getenv('LOGGER_FORMAT'),
                    datefmt='%Y-%m-%d %H:%M:%S',
                    level=logging.INFO)

async def criar(id_loja:int, id_pedido:int, cod_pedido:str, num_pedido:int):
    async with AsyncSessionLocal() as session:
        pedido = await session.query(Pedido).filter(Pedido.id_pedido == id_pedido).first()
        if pedido:
            return False

        ecommerce = await session.query(Ecommerce).filter(Ecommerce.id_loja == id_loja).first()
        if not ecommerce:
            logger.error(f"Ecommerce não encontrado para o pedido {id_pedido} na loja {id_loja}")
            return False

        novo_pedido = Pedido(id_pedido=id_pedido,
                             cod_pedido=cod_pedido,
                             num_pedido=num_pedido,
                             ecommerce_id=ecommerce.id)
        session.add(novo_pedido)
        await session.commit()
        return True

async def buscar_importar(ecommerce_id:int):
    async with AsyncSessionLocal() as session:
        pedidos = await session.query(Pedido).filter(Pedido.nunota.is_(None),
                                                     Pedido.dh_cancelamento.is_(None),
                                                     Pedido.ecommerce_id == ecommerce_id).order_by(Pedido.num_pedido).all()
        return pedidos

async def buscar_confirmar(ecommerce_id:int):
    async with AsyncSessionLocal() as session:
        pedidos = await session.query(Pedido).filter(Pedido.nunota.isnot(None),
                                                     Pedido.dh_cancelamento.is_(None),
                                                     Pedido.dh_confirmacao.is_(None),
                                                     Pedido.ecommerce_id == ecommerce_id).order_by(Pedido.num_pedido).all()
        return pedidos

async def buscar_faturar(ecommerce_id:int):
    async with AsyncSessionLocal() as session:
        pedidos = await session.query(Pedido).filter(Pedido.nunota.isnot(None),
                                                     Pedido.dh_cancelamento.is_(None),
                                                     Pedido.dh_confirmacao.isnot(None),
                                                     Pedido.dh_faturamento.is_(None),
                                                     Pedido.ecommerce_id == ecommerce_id).order_by(Pedido.num_pedido).all()
        return pedidos

async def buscar_cancelar(ecommerce_id:int):
    async with AsyncSessionLocal() as session:
        pedidos = await session.query(Pedido).filter(Pedido.nunota.isnot(None),
                                                     Pedido.dh_cancelamento.isnot(None),
                                                     Pedido.dh_faturamento.is_(None),
                                                     Pedido.ecommerce_id == ecommerce_id).order_by(Pedido.num_pedido).all()
        return pedidos

async def atualizar_separacao(id_pedido: int, id_separacao: int):
    async with AsyncSessionLocal() as session:
        pedido = await session.query(Pedido).filter(Pedido.id_pedido == id_pedido).first()
        if not pedido:
            return False
        setattr(pedido, "id_separacao", id_separacao)    
        await session.commit()
        return True

async def atualizar_importado(id_pedido:int,nunota:int):
    async with AsyncSessionLocal() as session:
        pedido = await session.query(Pedido).filter(Pedido.id_pedido == id_pedido).first()
        if not pedido:
            return False
        setattr(pedido, "nunota", nunota)
        setattr(pedido, "dh_importacao", datetime.now())
        await session.commit()
        return True

async def atualizar_confirmado(nunota:int, dh_confirmado: str=None):
    async with AsyncSessionLocal() as session:
        pedido = await session.query(Pedido).filter(Pedido.nunota == nunota).first()
        if not pedido:
            return False
        if dh_confirmado:
            setattr(pedido, "dh_confirmacao_pedido_snk", datetime.strptime(dh_confirmado,'%d/%m/%Y'))
        else:
            setattr(pedido, "dh_confirmacao_pedido_snk", datetime.now())
        await session.commit()
        return True

async def atualizar_confirmado_lote(nunota:int, dh_confirmado: str=None):
    async with AsyncSessionLocal() as session:
        pedidos = await session.query(Pedido).filter(Pedido.nunota == nunota).all()
        if not pedidos:
            return False
        for pedido in pedidos:
            if dh_confirmado:
                setattr(pedido, "dh_confirmacao", datetime.strptime(dh_confirmado,'%d/%m/%Y'))
            else:
                setattr(pedido, "dh_confirmacao", datetime.now())
        await session.commit()
        return True

async def atualizar_faturado(nunota:int,dh_faturamento:str=None):
    async with AsyncSessionLocal() as session:
        pedido = await session.query(Pedido).filter(Pedido.nunota == nunota).first()
        if not pedido:
            return False
        if dh_faturamento:
            setattr(pedido, "dh_faturamento", datetime.strptime(dh_faturamento,'%Y-%m-%d %H:%M:%S'))
        else:
            setattr(pedido, "dh_faturamento", datetime.now())
        await session.commit()
        return True

async def atualizar_faturado_lote(nunota:int,dh_faturamento:str=None):
    async with AsyncSessionLocal() as session:
        pedidos = await session.query(Pedido).filter(Pedido.nunota == nunota).all()
        if not pedidos:
            return False
        for pedido in pedidos:
            if dh_faturamento:
                setattr(pedido, "dh_faturamento", datetime.strptime(dh_faturamento,'%Y-%m-%d %H:%M:%S'))
            else:
                setattr(pedido, "dh_faturamento", datetime.now())
        await session.commit()
        return True

async def validar_nao_cancelados(lista_ids:list):
    async with AsyncSessionLocal() as session:
        pedidos_nao_cancelados = await session.query(Pedido).filter(Pedido.id_pedido.in_(lista_ids),
                                                                    Pedido.dh_cancelamento.is_(None)).order_by(Pedido.num_pedido).all()
        return pedidos_nao_cancelados