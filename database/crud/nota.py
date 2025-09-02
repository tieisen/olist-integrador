from database.database import AsyncSessionLocal
from database.models import Nota
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

async def criar(pedido_id:int, id_nota:int, numero:int, serie:str):
    async with AsyncSessionLocal() as session:
        nota = await session.query(Nota).filter(Nota.id == id_nota).first()
        if nota:
            return False
        nova_nota = Nota(pedido_id=pedido_id,
                         id_nota=id_nota,
                         numero=numero,
                         serie=serie)
        session.add(nova_nota)
        await session.commit()
        return True

async def buscar_baixar_financeiro():
    async with AsyncSessionLocal() as session:
        notas = await session.query(Nota).filter(Nota.dh_baixa_financeiro.is_(None)).all()
        return notas

async def buscar_atualizar_nunota():
    async with AsyncSessionLocal() as session:
        notas = await session.query(Nota).filter(Nota.nunota.is_(None)).all()
        return notas

async def buscar_faturar():
    async with AsyncSessionLocal() as session:
        notas = await session.query(Nota).filter(Nota.dh_faturamento.is_(None)).all()
        return notas

async def buscar_confirmar():
    async with AsyncSessionLocal() as session:
        notas = await session.query(Nota).filter(Nota.dh_confirmacao.is_(None)).all()
        return notas

async def atualizar_autorizada(id_nota:int):
    async with AsyncSessionLocal() as session:
        nota = await session.query(Nota).filter(Nota.id == id_nota).first()
        if not nota:
            return False
        setattr(nota, "dh_emissao", datetime.now())
        await session.commit()
        return True

async def atualizar_financeiro(id_nota:int, id_financeiro:int):
    async with AsyncSessionLocal() as session:
        nota = await session.query(Nota).filter(Nota.id_nota == id_nota).first()
        if not nota:
            return False
        setattr(nota, "id_financeiro", id_financeiro)
        await session.commit()
        return True

async def atualizar_financeiro_baixa(id_financeiro:int, dh_baixa:str=None):
    async with AsyncSessionLocal() as session:
        nota = await session.query(Nota).filter(Nota.id_financeiro == id_financeiro).first()
        if not nota:
            return False
        if dh_baixa:
            setattr(nota, "dh_baixa_financeiro", datetime.strptime(dh_baixa,'%Y-%m-%d'))
        else:
            setattr(nota, "dh_baixa_financeiro", datetime.now())
        await session.commit()
        return True

async def atualizar_nunota(id_nota:int, nunota:int):
    async with AsyncSessionLocal() as session:
        nota = await session.query(Nota).filter(Nota.id_nota == id_nota).first()
        if not nota:
            return False
        setattr(nota, "nunota", nunota)
        await session.commit()
        return True

async def atualizar_confirmada(nunota:int, dh_confirmacao:str=None):
    async with AsyncSessionLocal() as session:
        nota = await session.query(Nota).filter(Nota.nunota == nunota).first()
        if not nota:
            return False
        if dh_confirmacao:
            setattr(nota, "dh_confirmacao", datetime.strptime(dh_confirmacao,'%d/%m/%Y'))
        else:
            setattr(nota, "dh_confirmacao", datetime.now())
        await session.commit()
        return True

async def atualizar_confirmada_lote(nunota:int, dh_confirmacao:str=None):
    async with AsyncSessionLocal() as session:
        notas = await session.query(Nota).filter(Nota.nunota == nunota).all()
        if not notas:
            return False
        for nota in notas:
            if dh_confirmacao:
                setattr(nota, "dh_confirmacao", datetime.strptime(dh_confirmacao,'%d/%m/%Y'))
            else:
                setattr(nota, "dh_confirmacao", datetime.now())
        await session.commit()
        return True

async def atualizar_faturada(nunota:int, dh_faturamento:str=None):
    async with AsyncSessionLocal() as session:
        nota = await session.query(Nota).filter(Nota.nunota == nunota).first()
        if not nota:
            return False
        if dh_faturamento:
            setattr(nota, "dh_faturamento", datetime.strptime(dh_faturamento,'%d/%m/%Y'))
        else:
            setattr(nota, "dh_faturamento", datetime.now())
        await session.commit()
        return True

async def atualizar_faturada_lote(nunota:int, dh_faturamento:str=None):
    async with AsyncSessionLocal() as session:
        notas = await session.query(Nota).filter(Nota.nunota == nunota).all()
        if not notas:
            return False
        for nota in notas:
            if dh_faturamento:
                setattr(nota, "dh_faturamento", datetime.strptime(dh_faturamento,'%d/%m/%Y'))
            else:
                setattr(nota, "dh_faturamento", datetime.now())
        await session.commit()
        return True

async def atualizar_cancelamento(id_nota:int, dh_cancelamento:str=None):
    async with AsyncSessionLocal() as session:
        nota = await session.query(Nota).filter(Nota.id_nota == id_nota).first()
        if not nota:
            return False
        if dh_cancelamento:
            setattr(nota, "dh_cancelamento", datetime.strptime(dh_cancelamento,'%d/%m/%Y'))
        else:
            setattr(nota, "dh_cancelamento", datetime.now())
        await session.commit()
        return True
