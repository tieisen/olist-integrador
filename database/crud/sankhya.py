from database.database import AsyncSessionLocal
from database.models import Sankhya
from datetime import datetime, timedelta
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

async def criar(empresa_id:int, token_criptografado:str,dh_expiracao_token:str):
    async with AsyncSessionLocal() as session:    
        try:
            novo_token = Sankhya(empresa_id=empresa_id,
                               token_criptografado=token_criptografado,
                               dh_expiracao_token=dh_expiracao_token)
            session.add(novo_token)
            session.commit()
            session.refresh(novo_token)
            return novo_token.token_criptografado
        except Exception as e:
            logger.error("Erro ao salvar token no banco de dados: %s",e)
            return False

async def buscar(empresa_id:int):
    async with AsyncSessionLocal() as session:
        token = await session.query(Sankhya).filter(Sankhya.empresa_id == empresa_id).order_by(Sankhya.id.desc()).first()
        return token

async def excluir(id:int):
    async with AsyncSessionLocal() as session:
        token = await session.query(Sankhya).filter(Sankhya.id == id).first()
        if not token:
            return False
        try:
            await session.delete(token)
            await session.commit()
            return True
        except Exception as e:
            logger.error("Erro ao excluir token do banco de dados: %s", e)
            return False

async def excluir_cache(empresa_id:int):
    async with AsyncSessionLocal() as session:
        tokens = await session.query(Sankhya).filter(Sankhya.empresa_id == empresa_id,
                                                     Sankhya.dh_solicitacao < (datetime.now()-timedelta(days=7))).all()
        if not tokens:
            return False
        try:
            for token in tokens:
                await session.delete(token)
            await session.commit()
            return True
        except Exception as e:
            logger.error("Erro ao excluir tokens do banco de dados: %s", e)
            return False
