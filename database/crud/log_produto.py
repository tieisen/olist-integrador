from database.database import AsyncSessionLocal
from database.models import LogProduto
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

async def criar(log_id:int,codprod:int,idprod:int,campo:str,valor_old:str=None,valor_new:str=None,sucesso:bool=True,obs:str=None):
    async with AsyncSessionLocal() as session:
        novo_log = LogProduto(log_id=log_id,
                              codprod=codprod,
                              idprod=idprod,
                              campo=campo,
                              sucesso=sucesso,
                              valor_old=str(valor_old),
                              valor_new=str(valor_new),
                              obs=obs)
        session.add(novo_log)
        await session.commit()
        await session.refresh(novo_log)
        return True

async def buscar_status_false(log_id: int):
    async with AsyncSessionLocal() as session:
        log = await session.query(LogProduto).filter(LogProduto.log_id == log_id,
                                                     LogProduto.sucesso != 1).first()
        return log

async def buscar_id(log_id: int):
    async with AsyncSessionLocal() as session:
        log = await session.query(LogProduto).filter(LogProduto.log_id == log_id).all()
        return log