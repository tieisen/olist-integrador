from database.database import AsyncSessionLocal
from database.models import Nota, Pedido
from datetime import datetime
from src.utils.log import Log
from sqlalchemy.future import select
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

async def criar(
        pedido_id:int,
        id_nota:int,
        numero:int,
        serie:str
    ):
    async with AsyncSessionLocal() as session:
        nota = await session.execute(
            select(Nota).where(Nota.id_nota == id_nota)
        )

        if nota:
            print(f"Nota {id_nota} já existe")
            return False
        
        pedido = await session.execute(
            select(Nota).where(Nota.pedido_id == pedido_id,
                               Nota.dh_cancelamento.isnot(None))
        )

        if pedido:
            nota_atendida = pedido.scalar_one_or_none().numero
            print(f"Pedido {pedido_id} já foi atendido na nota {nota_atendida}")
            return False
        
        nova_nota = Nota(pedido_id=pedido_id,
                         id_nota=id_nota,
                         numero=numero,
                         serie=serie)
        session.add(nova_nota)
        await session.commit()
        return True

async def atualizar_autorizada(id_nota:int,chave_acesso:str):
    async with AsyncSessionLocal() as session:
        result = await session.execute(
            select(Nota).where(Nota.id == id_nota)
        )
        nota = result.scalar_one_or_none()
        if not nota:
            print(f"Nota não encontrada. Parâmetro: {id_nota}")
            return False
        setattr(nota, "dh_emissao", datetime.now())
        setattr(nota, "chave_acesso", chave_acesso)
        await session.commit()
        return True

async def buscar_financeiro():
    async with AsyncSessionLocal() as session:
        result = await session.execute(
            select(Nota).where(Nota.dh_cancelamento.is_(None),
                               Nota.id_financeiro.is_(None))
        )
        return result.scalars().all()

async def atualizar_financeiro(id_nota:int, id_financeiro:int):
    async with AsyncSessionLocal() as session:
        result = await session.execute(
            select(Nota).where(Nota.id_nota == id_nota)
        )
        nota = result.scalar_one_or_none()
        if not nota:
            print(f"Nota não encontrada. Parâmetro: {id_nota}")
            return False
        setattr(nota, "id_financeiro", id_financeiro)
        await session.commit()
        return True

async def buscar_financeiro_baixar():
    async with AsyncSessionLocal() as session:
        result = await session.execute(
            select(Nota).where(Nota.dh_cancelamento.is_(None),
                               Nota.id_financeiro.isnot(None),
                               Nota.dh_baixa_financeiro.is_(None))
        )
        return result.scalars().all()    

async def atualizar_financeiro_baixar(id_financeiro:int, dh_baixa:str=None):
    async with AsyncSessionLocal() as session:
        result = await session.execute(
            select(Nota).where(Nota.id_financeiro == id_financeiro)
        )
        nota = result.scalar_one_or_none()
        if not nota:
            print(f"Financiero da nota não encontrado. Parâmetro: {id_financeiro}")
            return False
        setattr(nota, "dh_baixa_financeiro", datetime.strptime(dh_baixa,'%Y-%m-%d') if dh_baixa else datetime.now())
        await session.commit()
        return True

async def buscar_atualizar_nunota():
    async with AsyncSessionLocal() as session:
        result = await session.execute(
            select(Nota).where(Nota.nunota.is_(None))
        )
        return result.scalars().all()

async def atualizar_nunota(nunota:int, id_nota:int=None, nunota_pedido:int=None):
    async with AsyncSessionLocal() as session:
        if id_nota:
            result = await session.execute(
                select(Nota).where(Nota.id_nota == id_nota)
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
                select(Nota).where(Pedido.nunota == nunota_pedido)
            )        
            notas = result.scalars().all()
            if not notas:
                print(f"Notas não encontradas. Parâmetro: {nunota_pedido}")
                return False
            for nota in notas:
                setattr(nota, "nunota", nunota)
            await session.commit()
            return True

async def buscar_confirmar():
    async with AsyncSessionLocal() as session:
        result = await session.execute(
            select(Nota).where(Nota.nunota.isnot(None),
                               Nota.dh_cancelamento.is_(None),
                               Nota.dh_confirmacao.is_(None))
        )
        notas = result.scalars().all()
        return notas
    
async def atualizar_confirmada(nunota:int, dh_confirmacao:str=None):
    async with AsyncSessionLocal() as session:
        result = await session.execute(
            select(Nota).where(Nota.nunota == nunota)
        )
        notas = result.scalars().all()
        if not notas:
            print(f"Nota não encontrada. Parâmetro: {nunota}")
            return False
        for nota in notas:
            setattr(nota, "dh_confirmacao", datetime.strptime(dh_confirmacao,'%d/%m/%Y') if dh_confirmacao else datetime.now())
        await session.commit()
        return True

async def atualizar_cancelamento(id_nota:int, dh_cancelamento:str=None):
    async with AsyncSessionLocal() as session:
        result = await session.execute(
            select(Nota).where(Nota.id_nota == id_nota)
        )
        nota = result.scalar_one_or_none()
        if not nota:
            print(f"Nota não encontrada. Parâmetro: {id_nota}")
            return False
        setattr(nota, "dh_cancelamento", datetime.strptime(dh_cancelamento,'%d/%m/%Y') if dh_cancelamento else datetime.now())
        await session.commit()
        return True
