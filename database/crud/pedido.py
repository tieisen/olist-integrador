from database.database import AsyncSessionLocal
from database.models import Pedido, Ecommerce
from datetime import datetime
from src.utils.log import Log
from sqlalchemy.future import select
from src.utils.db import validar_dados, formatar_retorno
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

COLUNAS_CRIPTOGRAFADAS = None

async def criar(
        id_loja:int,
        id_pedido:int,
        cod_pedido:str,
        num_pedido:int,
        **kwargs
    ):
    
    if kwargs:
        kwargs = validar_dados(modelo=Pedido,
                               kwargs=kwargs,
                               colunas_criptografadas=COLUNAS_CRIPTOGRAFADAS)
        if not kwargs:
            return False

    async with AsyncSessionLocal() as session:
        result = await session.execute(
            select(Pedido).where(Pedido.id_pedido == id_pedido)
        )
        pedido = result.scalar_one_or_none()
        if pedido:
            print(f"Pedido {id_pedido} já existe na base")
            return False

        result = await session.execute(
            select(Ecommerce).where(Ecommerce.id_loja == id_loja)
        )
        ecommerce = result.scalar_one_or_none()
        if not ecommerce:
            print(f"Ecommerce não encontrado para o pedido {id_pedido} na loja {id_loja}")
            return False

        try:
            novo_pedido = Pedido(id_pedido=id_pedido,
                                 cod_pedido=cod_pedido,
                                 num_pedido=num_pedido,
                                 ecommerce_id=ecommerce.id,
                                 **kwargs)
            session.add(novo_pedido)
            await session.commit()
            await session.refresh(novo_pedido)            
            return True
        except Exception as e:
            print(f"Erro ao criar pedido {id_pedido}: {e}")
            return False

async def buscar(
        id_pedido:int=None,
        num_pedido:int=None,
        cod_pedido:str=None
    ) -> dict:

    if not any([id_pedido, num_pedido, cod_pedido]):
        print("Nenhum parâmetro informado")
        return False
    async with AsyncSessionLocal() as session:
        if id_pedido:
            result = await session.execute(
                select(Pedido).where(Pedido.id_pedido == id_pedido)
            )
        if num_pedido:
            result = await session.execute(
                select(Pedido).where(Pedido.num_pedido == num_pedido)
            )
        if cod_pedido:
            result = await session.execute(
                select(Pedido).where(Pedido.cod_pedido == cod_pedido)
            )
        pedido = result.scalar_one_or_none()
        if not pedido:
            return False
        dados_pedido = formatar_retorno(colunas_criptografadas=COLUNAS_CRIPTOGRAFADAS,
                                        retorno=pedido)
        return dados_pedido

async def atualizar(
        id_pedido:int=None,
        num_pedido:int=None,
        **kwargs
    ):

    if not any([id_pedido, num_pedido]):
        print("Nenhum parâmetro informado")
        return False

    if kwargs:
        kwargs = validar_dados(modelo=Pedido,
                               kwargs=kwargs,
                               colunas_criptografadas=COLUNAS_CRIPTOGRAFADAS)
        if not kwargs:
            return False
            
    async with AsyncSessionLocal() as session:
        if id_pedido:
            result = await session.execute(
                select(Pedido).where(Pedido.id_pedido == id_pedido)
            )
        if num_pedido:
            result = await session.execute(
                select(Pedido).where(Pedido.num_pedido == num_pedido)
            )

        pedido = result.scalar_one_or_none()
        if not pedido:
            print(f"Pedido não encontrado. Parâmetro: {id_pedido or num_pedido}")
            return False
        
        for key, value in kwargs.items():
            setattr(pedido, key, value)
            
        await session.commit()
        return True        

async def atualizar_separacao(
        id_pedido: int,
        id_separacao: int
    ):
    async with AsyncSessionLocal() as session:
        result = await session.execute(
            select(Pedido).where(Pedido.id_pedido == id_pedido)
        )
        pedido = result.scalar_one_or_none()
        if not pedido:
            print(f"Pedido não encontrado. Parâmetro: {id_pedido}")
            return False
        setattr(pedido, "id_separacao", id_separacao)    
        await session.commit()
        return True        

async def buscar_importar(ecommerce_id:int):
    async with AsyncSessionLocal() as session:
        result = await session.execute(
            select(Pedido).where(Pedido.dh_importacao.is_(None),
                                 Pedido.dh_cancelamento.is_(None),
                                 Pedido.id_separacao.isnot(None),
                                 Pedido.ecommerce_id == ecommerce_id).order_by(Pedido.num_pedido)
        )
        pedidos = result.scalars().all()
        return pedidos

async def atualizar_importado(
        id_pedido:int,
        nunota:int
    ):
    async with AsyncSessionLocal() as session:
        result = await session.execute(
            select(Pedido).where(Pedido.id_pedido == id_pedido)
        )
        pedido = result.scalar_one_or_none()
        if not pedido:
            print(f"Pedido não encontrado. Parâmetro: {id_pedido}")
            return False        
        setattr(pedido, "nunota", nunota)
        setattr(pedido, "dh_importacao", datetime.now())
        await session.commit()
        return True

async def buscar_confirmar(ecommerce_id:int):
    async with AsyncSessionLocal() as session:
        result = await session.execute(
            select(Pedido).where(Pedido.dh_importacao.isnot(None),
                                 Pedido.dh_confirmacao.is_(None),
                                 Pedido.ecommerce_id == ecommerce_id).order_by(Pedido.num_pedido)
        )
        pedidos = result.scalars().all()
        return pedidos

async def atualizar_confirmado(
        nunota:int,
        dh_confirmado: str=None
    ):
    async with AsyncSessionLocal() as session:
        result = await session.execute(
            select(Pedido).filter(Pedido.nunota == nunota)
        )
        pedidos = result.scalars().all()
        if not pedidos:
            print(f"Pedido não encontrado. Parâmetro: {nunota}")
            return False
        for pedido in pedidos:
            if pedido.dh_confirmacao:
                # Pedido já confirmado,
                continue
            setattr(pedido, "dh_confirmacao", datetime.strptime(dh_confirmado,'%d/%m/%Y') if dh_confirmado else datetime.now())
        await session.commit()
        return True

async def buscar_checkout(ecommerce_id:int):
    async with AsyncSessionLocal() as session:
        result = await session.execute(
            select(Pedido).where(Pedido.dh_cancelamento.is_(None),
                                 Pedido.dh_faturamento.is_(None),
                                 Pedido.id_separacao.isnot(None),
                                 Pedido.ecommerce_id == ecommerce_id).order_by(Pedido.num_pedido)
        )
        pedidos = result.scalars().all()
        dados_pedidos = formatar_retorno(colunas_criptografadas=COLUNAS_CRIPTOGRAFADAS,
                                         retorno=pedidos)           
        return dados_pedidos
    
async def buscar_faturar(ecommerce_id:int):
    async with AsyncSessionLocal() as session:
        result = await session.execute(
            select(Pedido).where(Pedido.dh_confirmacao.isnot(None),
                                 Pedido.dh_faturamento.is_(None),
                                 Pedido.ecommerce_id == ecommerce_id).order_by(Pedido.num_pedido)
        )
        pedidos = result.scalars().all()
        return pedidos

async def atualizar_faturado(
        nunota:int,
        dh_faturamento: str=None
    ):
    async with AsyncSessionLocal() as session:
        result = await session.execute(
            select(Pedido).filter(Pedido.nunota == nunota)
        )
        pedidos = result.scalars().all()
        if not pedidos:
            print(f"Pedido não encontrado. Parâmetro: {nunota}")
            return False
        for pedido in pedidos:
            if pedido.dh_faturamento:
                # Pedido já faturado
                continue
            setattr(pedido, "dh_faturamento", datetime.strptime(dh_faturamento,'%d/%m/%Y') if dh_faturamento else datetime.now())
        await session.commit()
        return True

async def atualizar_cancelado(
        id_pedido:int,
        dh_cancelamento: str=None
    ):
    async with AsyncSessionLocal() as session:
        result = await session.execute(
            select(Pedido).filter(Pedido.id_pedido == id_pedido)
        )
        pedido = result.scalar_one_or_none()
        if not pedido:
            print(f"Pedido não encontrado. Parâmetro: {id_pedido}")
            return False
        setattr(pedido, "dh_cancelamento", datetime.strptime(dh_cancelamento,'%d/%m/%Y') if dh_cancelamento else datetime.now())
        await session.commit()
        return True

async def resetar(id_pedido:int):
    async with AsyncSessionLocal() as session:
        result = await session.execute(
            select(Pedido).filter(Pedido.id_pedido == id_pedido)
        )
        pedido = result.scalar_one_or_none()
        if not pedido:
            print(f"Pedido não encontrado. Parâmetro: {id_pedido}")
            return False
        setattr(pedido, "id_separacao", None)
        setattr(pedido, "nunota", None)
        setattr(pedido, "dh_importacao", None)
        setattr(pedido, "dh_confirmacao", None)
        setattr(pedido, "dh_faturamento", None)
        setattr(pedido, "dh_cancelamento", None)
        await session.commit()
        return True    

async def buscar_cancelar(ecommerce_id:int):
    async with AsyncSessionLocal() as session:
        result = await session.execute(
            select(Pedido).filter(Pedido.nunota.isnot(None),
                                  Pedido.dh_cancelamento.isnot(None),
                                  Pedido.ecommerce_id == ecommerce_id).order_by(Pedido.num_pedido)
        )
        pedidos = result.scalars().all()
        return pedidos
