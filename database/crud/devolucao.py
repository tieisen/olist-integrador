from database.database import AsyncSessionLocal
from database.models import Devolucao, Nota
from datetime import datetime
from src.utils.log import Log
from sqlalchemy.future import select
from src.utils.db import validar_dados
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
        chave_referenciada:int,
        id_nota:int,
        numero:int,
        serie:str,
        dh_emissao:str=None,
        **kwargs
    ):

    if kwargs:
        kwargs = validar_dados(modelo=Devolucao,
                               kwargs=kwargs,
                               colunas_criptografadas=COLUNAS_CRIPTOGRAFADAS)
        if not kwargs:
            return False

    async with AsyncSessionLocal() as session:
        result = await session.execute(
            select(Devolucao).where(Devolucao.id_nota == id_nota)
        )
        devolucao = result.scalar_one_or_none()

        if devolucao:
            print(f"Nota de devolução {id_nota} já existe")
            return False
        
        nota = await session.execute(
            select(Devolucao).where(Nota.chave_acesso == chave_referenciada)
        )

        if nota:
            nota_atendida = nota.scalar_one_or_none().numero
            print(f"Nota {numero} já foi atendida na nota {nota_atendida}")
            return False
        
        nota_original = await session.execute(
            select(Nota).where(Nota.chave_acesso == chave_referenciada)
        )

        if not nota_original:
            print(f"Nota original não encontrada para a devolução {id_nota}")
            return False
        
        nova_devolucao = Devolucao(nota_id=nota_original.scalar_one_or_none().id,
                                   id_nota=id_nota,
                                   numero=numero,
                                   serie=serie,
                                   dh_emissao=datetime.strptime(dh_emissao,'%d/%m/%Y') if dh_emissao else datetime.now(),
                                   **kwargs)
        session.add(nova_devolucao)
        await session.commit()
        return True

async def buscar_atualizar_nunota():
    async with AsyncSessionLocal() as session:
        result = await session.execute(
            select(Devolucao).where(Devolucao.nunota.is_(None))
        )
        return result.scalars().all()

async def atualizar_nunota(
        id_nota:int,
        nunota:int
    ):
    async with AsyncSessionLocal() as session:
        result = await session.execute(
            select(Devolucao).where(Devolucao.id_nota == id_nota)
        )
        devolucao = result.scalar_one_or_none()
        if not devolucao:
            print(f"Devolução não encontrada. Parâmetro: {id_nota}")
            return False
        setattr(devolucao, "nunota", nunota)
        await session.commit()
        return True

async def buscar_confirmar():
    async with AsyncSessionLocal() as session:
        result = await session.execute(
            select(Devolucao).where(Devolucao.nunota.isnot(None),
                                    Devolucao.dh_cancelamento.is_(None),
                                    Devolucao.dh_confirmacao.is_(None))
        )
        return result.scalars().all()
    
async def atualizar_confirmada(
        nunota:int,
        dh_confirmacao:str=None
    ):
    async with AsyncSessionLocal() as session:
        result = await session.execute(
            select(Devolucao).where(Devolucao.nunota == nunota)
        )
        devolucoes = result.scalars().all()
        if not devolucoes:
            print(f"Devolução não encontrada. Parâmetro: {nunota}")
            return False
        for devolucao in devolucoes:
            setattr(devolucao, "dh_confirmacao", datetime.strptime(dh_confirmacao,'%d/%m/%Y') if dh_confirmacao else datetime.now())
        await session.commit()
        return True

async def atualizar_cancelamento(
        id_nota:int,
        dh_cancelamento:str=None
    ):
    async with AsyncSessionLocal() as session:
        result = await session.execute(
            select(Devolucao).where(Devolucao.id_nota == id_nota)
        )
        devolucao = result.scalar_one_or_none()
        if not devolucao:
            print(f"Devolução não encontrada. Parâmetro: {id_nota}")
            return False
        setattr(devolucao, "dh_cancelamento", datetime.strptime(dh_cancelamento,'%d/%m/%Y') if dh_cancelamento else datetime.now())
        await session.commit()
        return True
