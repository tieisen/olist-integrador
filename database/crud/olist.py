from database.database import AsyncSessionLocal
from database.models import Olist, Empresa
from datetime import datetime, timedelta
from sqlalchemy.future import select
from src.utils.db import validar_dados, formatar_retorno
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

COLUNAS_CRIPTOGRAFADAS = [
        'token', 'refresh_token', 'id_token'
    ]   

async def criar(
        empresa_id:int,
        **kwargs
    ):
    if kwargs:
        kwargs = validar_dados(modelo=Olist,
                               kwargs=kwargs,
                               colunas_criptografadas=COLUNAS_CRIPTOGRAFADAS)
        if not kwargs:
            return False
    async with AsyncSessionLocal() as session:
        try:
            novo_token = Olist(empresa_id=empresa_id,
                               **kwargs)
            session.add(novo_token)
            await session.commit()
            return True
        except Exception as e:
            logger.error("Erro ao salvar token no banco de dados: %s",e)
            return False

async def buscar(
        empresa_id:int=None,
        codemp:int=None
    ):
    if not any([empresa_id,codemp]):
        return False
    async with AsyncSessionLocal() as session:
        if empresa_id:
            result = await session.execute(
                select(Olist).where(Olist.empresa_id == empresa_id).order_by(Olist.id.desc()).fetch(1)
            )
        if codemp:
            result = await session.execute(
                select(Olist).where(Olist.empresa_.has(Empresa.snk_codemp == codemp)).order_by(Olist.id.desc()).fetch(1)
            )
        token = result.scalar_one_or_none()
    if not token:
        print("Token não encontrado")
        return False        
    dados_token = formatar_retorno(colunas_criptografadas=COLUNAS_CRIPTOGRAFADAS,
                                    retorno=token)        
    return dados_token 

async def excluir(id:int):
    async with AsyncSessionLocal() as session:
        result = await session.execute(
            select(Olist).where(Olist.id == id)
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

async def excluir_cache():    
    try:
        dias = os.getenv('DIAS_LIMPA_CACHE',7)
    except Exception as e:
        erro = f"Valor para intervalo de dias do cache não encontrado. {e}"
        logger.error(erro)
        return False    
    async with AsyncSessionLocal() as session:
        result = await session.execute(
            select(Olist).where(Olist.dh_solicitacao < (datetime.now()-timedelta(days=dias)))
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
