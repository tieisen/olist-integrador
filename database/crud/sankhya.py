from database.database import AsyncSessionLocal
from database.models import Sankhya
from datetime import datetime, timedelta
from sqlalchemy.future import select
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

async def criar(
        empresa_id:int,
        token_criptografado:str,
        dh_expiracao_token:str
    ):
    async with AsyncSessionLocal() as session:    
        try:
            novo_token = Sankhya(empresa_id=empresa_id,
                                 token_criptografado=token_criptografado,
                                 dh_expiracao_token=dh_expiracao_token)
            session.add(novo_token)
            await session.commit()
            await session.refresh(novo_token)
            return novo_token.token_criptografado
        except Exception as e:
            logger.error("Erro ao salvar token no banco de dados: %s",e)
            return False

async def buscar(empresa_id:int):
    async with AsyncSessionLocal() as session:
        result = await session.execute(
            select(Sankhya).where(Sankhya.empresa_id == empresa_id).order_by(Sankhya.id.desc()).fetch(1)
        )
        token = result.scalar_one_or_none()
        return token.token_criptografado if token else None

async def excluir(id:int):
    async with AsyncSessionLocal() as session:
        result = await session.execute(
            select(Sankhya).where(Sankhya.id == id)
        )
        token = result.scalar_one_or_none()
        if not token:
            print("Token não encontrado")
            return False
        try:
            await session.delete(token)
            await session.commit()
            return True
        except Exception as e:
            logger.error("Erro ao excluir token do banco de dados: %s", e)
            return False

async def excluir_cache(dias:int=7):
    async with AsyncSessionLocal() as session:
        result = await session.execute(
            select(Sankhya).where(Sankhya.dh_solicitacao < (datetime.now()-timedelta(days=dias)))
        )
        tokens = result.scalars().all()
        if not tokens:
            print("Tokens não encontrados")
            return False
        try:
            for token in tokens:
                await session.delete(token)
            await session.commit()
            return True
        except Exception as e:
            logger.error("Erro ao excluir tokens do banco de dados: %s", e)
            return False
