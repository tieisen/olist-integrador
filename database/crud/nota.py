import os
import logging
from dotenv import load_dotenv
from database.database import AsyncSessionLocal
from database.models import Nota, Pedido
from datetime import datetime
from src.utils.log import Log
from sqlalchemy.future import select
from src.utils.db import validar_dados

load_dotenv('keys/.env')
logger = logging.getLogger(__name__)
logging.basicConfig(filename=Log().buscar_path(),
                    encoding='utf-8',
                    format=os.getenv('LOGGER_FORMAT'),
                    datefmt='%Y-%m-%d %H:%M:%S',
                    level=logging.INFO)

COLUNAS_CRIPTOGRAFADAS = None

async def criar(
        pedido_id:int,
        id_nota:int,
        numero:int,
        serie:str,
        **kwargs
    ):

    if kwargs:
        kwargs = validar_dados(modelo=Nota,
                               kwargs=kwargs,
                               colunas_criptografadas=COLUNAS_CRIPTOGRAFADAS)
        if not kwargs:
            return False

    async with AsyncSessionLocal() as session:
        result = await session.execute(
            select(Nota)
            .where(Nota.id_nota == id_nota)
        )
        nota = result.scalar_one_or_none()
        if nota:
            print(f"Nota {id_nota} já existe no ID {nota.id}")
            return False
        
        result = await session.execute(
            select(Nota)
            .where(Nota.pedido_id == pedido_id,
                   Nota.dh_cancelamento.isnot(None))
        )
        pedido = result.scalar_one_or_none()
        if pedido:
            print(f"Pedido {pedido_id} já foi atendido na nota {pedido.numero}")
            return False
        
        nova_nota = Nota(pedido_id=pedido_id,
                         id_nota=id_nota,
                         numero=numero,
                         serie=serie,
                         **kwargs)
        session.add(nova_nota)
        await session.commit()
        return True

async def buscar_emitir(ecommerce_id:int):
    async with AsyncSessionLocal() as session:
        result = await session.execute(
            select(Pedido)
            .where(Pedido.dh_faturamento.isnot(None),
                   Pedido.ecommerce_id == ecommerce_id,
                   ~Pedido.nota_.any())
            .order_by(Pedido.num_pedido)
        )
        return result.scalars().all()
    
async def buscar_autorizar(ecommerce_id:int):
    async with AsyncSessionLocal() as session:
        result = await session.execute(
            select(Nota)
            .where(Nota.id.isnot(None),
                   Nota.dh_emissao.is_(None),
                   Nota.pedido_.has(Pedido.ecommerce_id == ecommerce_id))
            .order_by(Nota.id)
        )
        return result.scalars().all()

async def atualizar_autorizada(
        id_nota:int,
        chave_acesso:str,
        dh_emissao:str=None
    ):
    async with AsyncSessionLocal() as session:
        result = await session.execute(
            select(Nota)
            .where(Nota.id_nota == id_nota)
        )
        nota = result.scalar_one_or_none()
        if not nota:
            print(f"Nota não encontrada. Parâmetro: {id_nota}")
            return False
        setattr(nota, "dh_emissao", datetime.strptime(dh_emissao,'%Y-%m-%d') if dh_emissao else datetime.now())
        setattr(nota, "chave_acesso", chave_acesso)
        await session.commit()
        return True

async def buscar_financeiro(ecommerce_id:int):
    async with AsyncSessionLocal() as session:
        result = await session.execute(
            select(Nota)
            .where(Nota.dh_cancelamento.is_(None),
                   Nota.id_financeiro.is_(None),
                   Nota.pedido_.has(Pedido.ecommerce_id == ecommerce_id))
        )
        return result.scalars().all()

async def atualizar_financeiro(
        id_nota:int,
        id_financeiro:int
    ):
    async with AsyncSessionLocal() as session:
        result = await session.execute(
            select(Nota)
            .where(Nota.id_nota == id_nota)
        )
        nota = result.scalar_one_or_none()
        if not nota:
            print(f"Nota não encontrada. Parâmetro: {id_nota}")
            return False
        
        if nota.id_financeiro:
            print(f"Nota já possui financeiro. Parâmetro: {id_nota}")
            return False
        
        setattr(nota, "id_financeiro", id_financeiro)
        await session.commit()
        return True

async def buscar_financeiro_baixar(ecommerce_id:int):
    async with AsyncSessionLocal() as session:
        result = await session.execute(
            select(Nota)
            .where(Nota.dh_cancelamento.is_(None),
                   Nota.id_financeiro.isnot(None),
                   Nota.dh_baixa_financeiro.is_(None),
                   Nota.pedido_.has(Pedido.ecommerce_id == ecommerce_id))
        )
        return result.scalars().all()    

async def atualizar_financeiro_baixar(
        id_financeiro:int,
        dh_baixa:str=None
    ):
    async with AsyncSessionLocal() as session:
        result = await session.execute(
            select(Nota)
            .where(Nota.id_financeiro == id_financeiro)
        )
        nota = result.scalar_one_or_none()
        if not nota:
            print(f"Financiero da nota não encontrado. Parâmetro: {id_financeiro}")
            return False
        if nota.dh_baixa_financeiro:
            print(f"Nota já foi baixada. Parâmetro: {id_financeiro}")
            return False
        setattr(nota, "dh_baixa_financeiro", datetime.strptime(dh_baixa,'%Y-%m-%d') if dh_baixa else datetime.now())
        await session.commit()
        return True

async def buscar_atualizar_nunota(ecommerce_id:int):
    async with AsyncSessionLocal() as session:
        result = await session.execute(
            select(Nota)
            .where(Nota.nunota.is_(None),
                   Nota.pedido_.has(Pedido.ecommerce_id == ecommerce_id))
        )
        return result.scalars().all()

async def atualizar_nunota(
        nunota:int,
        id_nota:int=None,
        nunota_pedido:int=None
    ):
    async with AsyncSessionLocal() as session:

        if nunota == nunota_pedido:
            print("Nº único do pedido e da nota não podem ser iguais")
            return False
        
        if id_nota:
            result = await session.execute(
                select(Nota)
                .where(Nota.id_nota == id_nota)
            )            
            nota = result.scalar_one_or_none()
            if not nota:
                print(f"Nota não encontrada. Parâmetro: {id_nota}")
                return False
            setattr(nota, "nunota", nunota)
            await session.commit()
            return True

        if nunota_pedido:
            result = await session.execute(
                select(Nota)
                .where(Nota.pedido_.has(Pedido.nunota == nunota_pedido))
            )
            notas = result.scalars().all()
            if not notas:
                print(f"Notas não encontradas. Parâmetro: {nunota_pedido}")
                return False
            for nota in notas:
                setattr(nota, "nunota", nunota)
            await session.commit()
            return True

async def buscar_confirmar(ecommerce_id:int):
    async with AsyncSessionLocal() as session:
        result = await session.execute(
            select(Nota)
            .where(Nota.nunota.isnot(None),
                   Nota.dh_cancelamento.is_(None),
                   Nota.dh_confirmacao.is_(None),
                   Nota.pedido_.has(Pedido.ecommerce_id == ecommerce_id))
        )
        notas = result.scalars().all()
        return notas
    
async def atualizar_confirmada(
        nunota:int,
        dh_confirmacao:str=None
    ):
    async with AsyncSessionLocal() as session:
        result = await session.execute(
            select(Nota)
            .where(Nota.nunota == nunota)
        )
        notas = result.scalars().all()
        if not notas:
            print(f"Nota não encontrada. Parâmetro: {nunota}")
            return False
        for nota in notas:
            setattr(nota, "dh_confirmacao", datetime.strptime(dh_confirmacao,'%d/%m/%Y') if dh_confirmacao else datetime.now())
        await session.commit()
        return True

async def atualizar_cancelamento(
        id_nota:int,
        dh_cancelamento:str=None
    ):
    async with AsyncSessionLocal() as session:
        result = await session.execute(
            select(Nota)
            .where(Nota.id_nota == id_nota)
        )
        nota = result.scalar_one_or_none()
        if not nota:
            print(f"Nota não encontrada. Parâmetro: {id_nota}")
            return False
        setattr(nota, "dh_cancelamento", datetime.strptime(dh_cancelamento,'%d/%m/%Y') if dh_cancelamento else datetime.now())
        await session.commit()
        return True

async def buscar_cancelar(ecommerce_id:int):
    async with AsyncSessionLocal() as session:
        result = await session.execute(
            select(Nota)
            .where(Nota.cancelado_sankhya.is_(False),
                   Nota.dh_cancelamento.isnot(None),
                   Nota.pedido_.has(Pedido.ecommerce_id == ecommerce_id))
        )
        notas = result.scalars().all()
        return notas

async def atualizar_cancelamento_sankhya(id_nota:int):
    async with AsyncSessionLocal() as session:
        result = await session.execute(
            select(Nota)
            .where(Nota.id_nota == id_nota)
        )
        nota = result.scalar_one_or_none()
        if not nota:
            print(f"Nota não encontrada. Parâmetro: {id_nota}")
            return False
        setattr(nota, "cancelado_sankhya", True)
        await session.commit()
        return True    
